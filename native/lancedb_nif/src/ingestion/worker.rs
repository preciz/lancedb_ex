//! Single-commit producer lifecycle, independent of BEAM ownership and encoding.
use super::WriteResult;
use crate::error::Failure;
use crate::resources::RUNTIME;
use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use futures::{FutureExt, StreamExt};
use lancedb::arrow::{SendableRecordBatchStream, SimpleRecordBatchStream};
use std::future::Future;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use tokio::sync::{mpsc, watch};

pub(crate) type Outcome<T = WriteResult> = Result<T, Failure>;

enum Input {
    Batch(RecordBatch),
    Finish,
}

// Input closure and execution completion are separate: finishing or cancelling
// input does not prove that a commit has completed (or that it can be undone).
enum InputState {
    Open,
    Finishing,
    Aborted,
}

pub(crate) struct Writer<T = WriteResult> {
    input: mpsc::Sender<Input>,
    state: Mutex<InputState>,
    cancel: watch::Sender<bool>,
    terminal: watch::Receiver<Option<Outcome<T>>>,
}

#[cfg(test)]
impl Writer {
    pub(crate) fn spawn<F, Fut>(schema: SchemaRef, execute: F) -> Self
    where
        F: FnOnce(SendableRecordBatchStream) -> Fut + Send + 'static,
        Fut: Future<Output = Result<u64, Failure>> + Send + 'static,
    {
        Writer::spawn_operation(schema, move |stream, count| async move {
            execute(stream).await.map(|version| WriteResult {
                inserted: count.load(Ordering::Relaxed),
                version,
            })
        })
    }
}

impl<T: Clone + Send + Sync + 'static> Writer<T> {
    pub(crate) fn spawn_operation<F, Fut>(schema: SchemaRef, execute: F) -> Self
    where
        F: FnOnce(SendableRecordBatchStream, Arc<AtomicUsize>) -> Fut + Send + 'static,
        Fut: Future<Output = Outcome<T>> + Send + 'static,
    {
        let (input, receiver) = mpsc::channel(1);
        let (cancel, cancelled) = watch::channel(false);
        let (complete, terminal) = watch::channel(None);
        let count = Arc::new(AtomicUsize::new(0));
        let stream = input_stream(schema, receiver, cancelled, count.clone());
        let processed = count.clone();
        RUNTIME.spawn(async move {
            // Always await execution: aborting an SDK future can detach insert tasks.
            let outcome = std::panic::AssertUnwindSafe(async move { execute(stream, count).await })
                .catch_unwind()
                .await
                .unwrap_or_else(|_| Err(Failure::new("execution", "Native writer task panicked")));
            complete.send_replace(Some(outcome.map_err(|error| {
                error
                    .executing()
                    .progress(processed.load(Ordering::Relaxed))
            })));
        });
        Self {
            input,
            state: Mutex::new(InputState::Open),
            cancel,
            terminal,
        }
    }

    pub(crate) fn check_open(&self) -> Result<(), Failure> {
        if matches!(*self.state.lock().unwrap(), InputState::Open) {
            Ok(())
        } else {
            Err("Writer is closed".into())
        }
    }

    pub(crate) async fn push(&self, batch: RecordBatch) -> Result<(), Failure> {
        self.check_open()?;
        crate::batch::validation::validate(&batch)?;
        if self.input.send(Input::Batch(batch)).await.is_err() {
            return Err(self
                .outcome()
                .await
                .err()
                .unwrap_or("Writer is closed".into()));
        }
        Ok(())
    }

    pub(crate) async fn finish(&self) -> Outcome<T> {
        {
            let mut state = self.state.lock().unwrap();
            if !matches!(*state, InputState::Open) {
                return Err("Writer is closed".into());
            }
            *state = InputState::Finishing;
        }
        let _ = self.input.send(Input::Finish).await;
        self.outcome().await
    }

    pub(crate) fn cancel(&self) {
        *self.state.lock().unwrap() = InputState::Aborted;
        self.cancel.send_replace(true);
    }

    pub(crate) async fn abort(&self) {
        self.cancel();
        let _ = self.outcome().await;
    }

    async fn outcome(&self) -> Outcome<T> {
        let mut terminal = self.terminal.clone();
        let result = terminal
            .wait_for(Option::is_some)
            .await
            .map_err(|_| "Writer terminated".to_string())?;
        result.clone().unwrap()
    }
}

impl<T> Drop for Writer<T> {
    fn drop(&mut self) {
        *self.state.lock().unwrap() = InputState::Aborted;
        self.cancel.send_replace(true);
    }
}

fn input_stream(
    schema: SchemaRef,
    receiver: mpsc::Receiver<Input>,
    cancelled: watch::Receiver<bool>,
    count: Arc<AtomicUsize>,
) -> SendableRecordBatchStream {
    let stream = futures::stream::unfold(
        (receiver, cancelled, false),
        move |(mut receiver, mut cancelled, ended)| {
            let count = count.clone();
            async move {
                if ended {
                    return None;
                }
                let input = tokio::select! {
                    biased;
                    _ = cancelled.changed() => None,
                    input = receiver.recv() => input,
                };
                match input {
                    Some(Input::Batch(batch)) => {
                        count.fetch_add(batch.num_rows(), Ordering::Relaxed);
                        Some((Ok(batch), (receiver, cancelled, false)))
                    }
                    Some(Input::Finish) => None,
                    None => Some((
                        Err(lancedb::Error::Runtime {
                            message: "Writer aborted before finish".into(),
                        }),
                        (receiver, cancelled, true),
                    )),
                }
            }
        },
    );
    Box::pin(SimpleRecordBatchStream {
        schema,
        stream: stream.fuse(),
    })
}

#[cfg(test)]
mod tests;
