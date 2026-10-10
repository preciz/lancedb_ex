//! Demand-driven query workers; no Arrow stream is retained by a BEAM scheduler.
use crate::error::Failure;
use crate::execution::Deadline;
use crate::query::{BatchStream, QueryRequest};
use crate::resources::{atoms, TableResource, RUNTIME};
use arrow_array::RecordBatch;
use futures::{FutureExt, TryStreamExt};
use rustler::{Encoder, Env, LocalPid, Monitor, OwnedEnv, ResourceArc, Term};
use std::future::Future;
use std::panic::{RefUnwindSafe, UnwindSafe};
#[cfg(test)]
use tokio::sync::oneshot;
use tokio::sync::{mpsc, watch};

type BatchResult = Result<Option<RecordBatch>, Failure>;

enum Demand {
    // Retain the resource even if the caller drops its last BEAM reference
    // while waiting. Owner monitoring still cancels on process death.
    Process(Reply, ResourceArc<CursorResource>),
    #[cfg(test)]
    Channel(oneshot::Sender<BatchResult>),
}

impl Demand {
    fn send(self, result: BatchResult) -> Result<(), ()> {
        match self {
            Self::Process(mut reply, _cursor) => reply
                .send(result.map(|batch| {
                    batch.map(|batch| ResourceArc::new(crate::batch::BatchResource { batch }))
                }))
                .map_err(|_| ()),
            #[cfg(test)]
            Self::Channel(reply) => reply.send(result).map_err(|_| ()),
        }
    }
}

struct Reply {
    pid: LocalPid,
    env: OwnedEnv,
    reference: rustler::env::SavedTerm,
}

impl Reply {
    fn new(env: Env<'_>, reference: Term<'_>) -> Self {
        let message_env = OwnedEnv::new();
        let reference = message_env.save(reference);
        Self {
            pid: env.pid(),
            env: message_env,
            reference,
        }
    }

    fn send(&mut self, result: impl Encoder) -> Result<(), rustler::env::SendError> {
        self.env.send_and_clear(&self.pid, |env| {
            (self.reference.load(env), result).encode(env)
        })
    }
}

struct CursorChannels {
    demand: mpsc::Sender<Demand>,
    cancel: watch::Sender<bool>,
    terminal: watch::Receiver<Option<BatchResult>>,
}

pub struct CursorResource {
    channels: CursorChannels,
    owner: LocalPid,
}

impl RefUnwindSafe for CursorResource {}
impl UnwindSafe for CursorResource {}

#[rustler::resource_impl]
impl rustler::Resource for CursorResource {
    fn down(&self, _env: Env<'_>, _pid: LocalPid, _monitor: Monitor) {
        self.channels.signal_cancel();
    }
}

impl Drop for CursorResource {
    fn drop(&mut self) {
        self.channels.signal_cancel();
    }
}

fn spawn_cursor(
    source: impl Future<Output = Result<BatchStream, Failure>> + Send + 'static,
    batch_size: u32,
    deadline: Deadline,
) -> CursorChannels {
    let (demand, mut requests) = mpsc::channel(1);
    let (cancel, mut cancelled) = watch::channel(false);
    let (finished, terminal) = watch::channel(None);
    RUNTIME.spawn(async move {
        let mut pending_reply = None;
        let result = std::panic::AssertUnwindSafe(async {
            let operation = serve(
                source,
                &mut requests,
                batch_size as usize,
                &mut pending_reply,
            );
            tokio::select! {
                biased;
                _ = cancelled.changed() => Err("Stream is closed".into()),
                result = deadline.run(operation) => result,
            }
        })
        .catch_unwind()
        .await
        .unwrap_or_else(|_| Err(Failure::new("execution", "Native stream task panicked")));
        // Publish after dropping the pending query/pull future and read permit.
        finished.send_replace(Some(result.clone()));
        // A cancelled/panicking future leaves its current reply here. Reply only
        // after its query and read permit have been dropped, then drain queued
        // requests so every admitted caller receives the same terminal result.
        if let Some(reply) = pending_reply {
            let _ = reply.send(result.clone());
        }
        requests.close();
        while let Some(reply) = requests.recv().await {
            let _ = reply.send(result.clone());
        }
    });
    CursorChannels {
        demand,
        cancel,
        terminal,
    }
}

async fn serve(
    source: impl Future<Output = Result<BatchStream, Failure>>,
    requests: &mut mpsc::Receiver<Demand>,
    batch_size: usize,
    pending_reply: &mut Option<Demand>,
) -> BatchResult {
    // Planning starts only after the first demand.
    *pending_reply = requests.recv().await;
    if pending_reply.is_none() {
        return Ok(None);
    }
    let _permit = crate::execution::Permit::read()?;
    let mut stream = source.await?;
    let mut pending: Option<RecordBatch> = None;
    while pending_reply.is_some() {
        let result = next_batch(&mut stream, &mut pending, batch_size).await;
        let stop = !matches!(&result, Ok(Some(_)));
        if stop {
            return result;
        }
        if pending_reply.take().unwrap().send(result).is_err() {
            return Ok(None);
        }
        *pending_reply = requests.recv().await;
    }
    Ok(None)
}

async fn next_batch(
    stream: &mut BatchStream,
    pending: &mut Option<RecordBatch>,
    size: usize,
) -> BatchResult {
    loop {
        let batch = match pending.take() {
            Some(batch) => batch,
            None => match stream.try_next().await? {
                Some(batch) => batch,
                None => return Ok(None),
            },
        };
        if batch.num_rows() == 0 {
            continue;
        }
        let length = batch.num_rows().min(size);
        if length < batch.num_rows() {
            *pending = Some(batch.slice(length, batch.num_rows() - length));
        }
        return Ok(Some(batch.slice(0, length)));
    }
}

impl CursorChannels {
    fn signal_cancel(&self) {
        self.cancel.send_replace(true);
    }

    async fn close(&self) {
        self.signal_cancel();
        let _ = self.completed().await;
    }

    #[cfg(test)]
    async fn next(&self) -> BatchResult {
        if let Some(result) = self.terminal.borrow().clone() {
            return result;
        }
        if *self.cancel.borrow() {
            return Err("Stream is closed".into());
        }
        let (reply, response) = oneshot::channel();
        if self.demand.send(Demand::Channel(reply)).await.is_ok() {
            if let Ok(result) = response.await {
                if matches!(result, Ok(Some(_))) {
                    return result;
                }
                // EOF/errors must also acknowledge release of read admission.
                return self.completed().await;
            }
        }
        self.completed().await
    }

    async fn completed(&self) -> BatchResult {
        let mut terminal = self.terminal.clone();
        let result = terminal
            .wait_for(Option::is_some)
            .await
            .map_err(|_| "Stream worker terminated".to_string())?;
        result.clone().unwrap()
    }
}

#[rustler::nif(schedule = "DirtyCpu")]
fn stream_open(
    env: Env<'_>,
    table: ResourceArc<TableResource>,
    request: String,
    batch_size: u32,
) -> Result<ResourceArc<CursorResource>, Failure> {
    if batch_size == 0 {
        return Err("batch_size must be positive".into());
    }
    let request: QueryRequest = serde_json::from_str(&request).map_err(Failure::from_error)?;
    let execution = request.execution;
    open(
        env,
        async move {
            request
                .prepare(&table.table)
                .await?
                .execute(batch_size)
                .await
        },
        batch_size,
        execution.deadline(),
    )
}

pub(crate) fn open(
    env: Env<'_>,
    source: impl Future<Output = Result<BatchStream, Failure>> + Send + 'static,
    batch_size: u32,
    deadline: Deadline,
) -> Result<ResourceArc<CursorResource>, Failure> {
    if batch_size == 0 {
        return Err("batch_size must be positive".into());
    }
    deadline.check()?;
    let channels = spawn_cursor(source, batch_size, deadline);
    let owner = env.pid();
    let cursor = ResourceArc::new(CursorResource { channels, owner });
    if cursor.monitor(Some(env), &owner).is_none() {
        return Err("Could not monitor stream consumer".into());
    }
    Ok(cursor)
}

// Scheduler calls only enqueue work. Pending storage and cleanup futures wait
// on Tokio; the caller receives a reference-tagged result in its own mailbox.
#[rustler::nif]
fn stream_next_batch<'a>(
    env: Env<'a>,
    cursor: ResourceArc<CursorResource>,
    reference: Term<'a>,
) -> Result<Term<'a>, Failure> {
    if let Some(result) = cursor.channels.terminal.borrow().clone() {
        return Ok(result?
            .map(|batch| ResourceArc::new(crate::batch::BatchResource { batch }))
            .encode(env));
    }
    if cursor.owner != env.pid() {
        return Err("Stream cursor belongs to another process".into());
    }
    match cursor
        .channels
        .demand
        .try_send(Demand::Process(Reply::new(env, reference), cursor.clone()))
    {
        Ok(()) => {}
        Err(mpsc::error::TrySendError::Full(_)) => {
            return Err(Failure::new("busy", "A stream pull is already pending"));
        }
        Err(mpsc::error::TrySendError::Closed(_)) => {
            // Completion is published before closing the request queue.
            let result = cursor.channels.terminal.borrow().clone().unwrap();
            return Ok(result?
                .map(|batch| ResourceArc::new(crate::batch::BatchResource { batch }))
                .encode(env));
        }
    }
    Ok(atoms::ok().encode(env))
}

#[rustler::nif]
fn stream_close<'a>(
    env: Env<'a>,
    cursor: ResourceArc<CursorResource>,
    reference: Term<'a>,
) -> Term<'a> {
    if cursor.channels.terminal.borrow().is_some() {
        return atoms::ok().encode(env);
    }
    reply(env, reference, async move {
        cursor.channels.close().await;
        atoms::ok()
    });
    (atoms::ok(), atoms::ok()).encode(env)
}

fn reply<T: Encoder + Send + 'static>(
    env: Env<'_>,
    reference: Term<'_>,
    operation: impl Future<Output = T> + Send + 'static,
) {
    let mut reply = Reply::new(env, reference);
    RUNTIME.spawn(async move {
        let result = operation.await;
        let _ = reply.send(result);
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codec::batches_to_json;
    use arrow_array::Int64Array;
    use futures::Stream;
    use std::pin::Pin;
    use std::sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc,
    };
    use std::task::{Context, Poll};

    fn spawn_cursor(
        source: impl Future<Output = Result<BatchStream, Failure>> + Send + 'static,
        batch_size: u32,
        timeout_ms: Option<u32>,
    ) -> CursorChannels {
        super::spawn_cursor(
            source,
            batch_size,
            crate::execution::ExecutionOptions { timeout_ms }.deadline(),
        )
    }

    struct Probe {
        polls: Arc<AtomicUsize>,
        dropped: Arc<AtomicBool>,
        batch: Option<RecordBatch>,
    }

    impl Stream for Probe {
        type Item = Result<RecordBatch, Failure>;
        fn poll_next(mut self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Option<Self::Item>> {
            self.polls.fetch_add(1, Ordering::SeqCst);
            match self.batch.take() {
                Some(batch) => Poll::Ready(Some(Ok(batch))),
                None => Poll::Pending,
            }
        }
    }

    impl Drop for Probe {
        fn drop(&mut self) {
            self.dropped.store(true, Ordering::SeqCst);
        }
    }

    fn probe(batch: Option<RecordBatch>) -> (BatchStream, Arc<AtomicUsize>, Arc<AtomicBool>) {
        let polls = Arc::new(AtomicUsize::new(0));
        let dropped = Arc::new(AtomicBool::new(false));
        (
            Box::pin(Probe {
                polls: polls.clone(),
                dropped: dropped.clone(),
                batch,
            }),
            polls,
            dropped,
        )
    }

    #[test]
    fn pulls_only_on_demand_slices_large_batches_and_drops_on_cancellation() {
        RUNTIME.block_on(async {
            let batch = RecordBatch::try_from_iter([(
                "id",
                Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5])) as _,
            )])
            .unwrap();
            let (stream, polls, dropped) = probe(Some(batch));
            let cursor = spawn_cursor(async { Ok(stream) }, 2, None);
            tokio::task::yield_now().await;
            assert_eq!(polls.load(Ordering::SeqCst), 0);
            for expected in [vec![1, 2], vec![3, 4], vec![5]] {
                let batch = cursor.next().await.unwrap().unwrap();
                let json = batches_to_json(vec![batch]).unwrap();
                let rows: Vec<serde_json::Value> = serde_json::from_str(&json).unwrap();
                assert_eq!(
                    rows.iter()
                        .map(|r| r["id"].as_i64().unwrap())
                        .collect::<Vec<_>>(),
                    expected
                );
                assert_eq!(polls.load(Ordering::SeqCst), 1);
            }
            cursor.cancel.send_replace(true);
            assert_eq!(cursor.next().await, Err("Stream is closed".into()));
            let mut terminal = cursor.terminal.clone();
            tokio::time::timeout(
                std::time::Duration::from_secs(1),
                terminal.wait_for(Option::is_some),
            )
            .await
            .unwrap()
            .unwrap();
            assert!(dropped.load(Ordering::SeqCst));
        });
    }

    #[test]
    fn close_rejects_ready_buffered_rows_without_waiting_for_worker_scheduling() {
        RUNTIME.block_on(async {
            let batch = RecordBatch::try_from_iter([(
                "id",
                Arc::new(Int64Array::from(vec![1, 2, 3])) as _,
            )])
            .unwrap();
            let (stream, _, _) = probe(Some(batch));
            let cursor = spawn_cursor(async { Ok(stream) }, 1, None);
            assert_eq!(cursor.next().await.unwrap().unwrap().num_rows(), 1);
            cursor.cancel.send_replace(true);
            // Remaining rows are already buffered; no scheduler yield is required
            // for the close signal to reject the next demand.
            assert_eq!(cursor.next().await, Err("Stream is closed".into()));
        });
    }

    #[test]
    fn timeout_cancels_a_pending_pull_and_retains_the_error() {
        RUNTIME.block_on(async {
            let (stream, _, dropped) = probe(None);
            let cursor = spawn_cursor(async { Ok(stream) }, 2, Some(10));
            assert_eq!(
                cursor.next().await,
                Err(Failure::new("timeout", "Query timed out after 10 ms"))
            );
            assert_eq!(
                cursor.next().await,
                Err(Failure::new("timeout", "Query timed out after 10 ms"))
            );
            assert!(dropped.load(Ordering::SeqCst));
        });
    }

    #[test]
    fn consumer_idle_time_counts_towards_the_deadline() {
        RUNTIME.block_on(async {
            let (stream, polls, dropped) = probe(None);
            let cursor = spawn_cursor(async { Ok(stream) }, 2, Some(5));
            let mut terminal = cursor.terminal.clone();
            terminal.wait_for(Option::is_some).await.unwrap();
            assert_eq!(polls.load(Ordering::SeqCst), 0);
            assert!(dropped.load(Ordering::SeqCst));
            assert_eq!(
                cursor.next().await,
                Err(Failure::new("timeout", "Query timed out after 5 ms"))
            );
        });
    }

    #[test]
    fn planning_and_batch_errors_and_eof_are_terminal() {
        RUNTIME.block_on(async {
            let cursor = spawn_cursor(async { Err("planning failed".into()) }, 2, None);
            assert_eq!(cursor.next().await, Err("planning failed".into()));
            assert_eq!(cursor.next().await, Err("planning failed".into()));
            let source: BatchStream = Box::pin(futures::stream::iter([Err("batch failed".into())]));
            let cursor = spawn_cursor(async { Ok(source) }, 2, None);
            assert_eq!(cursor.next().await, Err("batch failed".into()));
            let source: BatchStream = Box::pin(futures::stream::empty());
            let cursor = spawn_cursor(async { Ok(source) }, 2, None);
            assert_eq!(cursor.next().await, Ok(None));
            assert_eq!(cursor.next().await, Ok(None));
        });
    }

    #[test]
    fn explicit_close_waits_for_cleanup_and_is_idempotent() {
        RUNTIME.block_on(async {
            let (stream, polls, dropped) = probe(None);
            let cursor = spawn_cursor(async { Ok(stream) }, 1, None);
            cursor.close().await;
            assert!(dropped.load(Ordering::SeqCst));
            assert_eq!(polls.load(Ordering::SeqCst), 0);
            cursor.close().await;

            let batch =
                RecordBatch::try_from_iter([("id", Arc::new(Int64Array::from(vec![1])) as _)])
                    .unwrap();
            let (stream, _, dropped) = probe(Some(batch));
            let cursor = spawn_cursor(async { Ok(stream) }, 1, None);
            cursor.next().await.unwrap().unwrap();
            cursor.close().await;
            assert!(dropped.load(Ordering::SeqCst));
        });
    }

    #[test]
    fn cancellation_replies_to_in_flight_and_queued_demands_after_cleanup() {
        RUNTIME.block_on(async {
            let (stream, _, dropped) = probe(None);
            let cursor = spawn_cursor(async { Ok(stream) }, 1, None);
            let (first, first_result) = oneshot::channel();
            let (second, second_result) = oneshot::channel();
            cursor.demand.send(Demand::Channel(first)).await.unwrap();
            // Capacity one means this completes only after the worker takes the
            // first demand. The first pull stays pending while this one queues.
            cursor.demand.send(Demand::Channel(second)).await.unwrap();
            cursor.close().await;
            assert!(dropped.load(Ordering::SeqCst));
            let error = Err("Stream is closed".into());
            assert_eq!(first_result.await.unwrap(), error);
            assert_eq!(second_result.await.unwrap(), error);
        });
    }

    #[test]
    fn worker_panics_publish_completion_and_close_cannot_hang() {
        RUNTIME.block_on(async {
            let cursor = spawn_cursor(async { panic!("planning panic") }, 1, None);
            assert_eq!(
                cursor.next().await,
                Err(Failure::new("execution", "Native stream task panicked"))
            );
            cursor.close().await;
        });
    }
}
