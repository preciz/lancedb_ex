use super::*;
use crate::error::Failure;
use arrow_array::Int64Array;
use futures::TryStreamExt;
use std::sync::atomic::AtomicBool;
use std::time::Duration;
use tokio::sync::oneshot;

fn batch(values: Vec<i64>) -> RecordBatch {
    RecordBatch::try_from_iter([("id", Arc::new(Int64Array::from(values)) as _)]).unwrap()
}

fn run(test: impl Future<Output = ()>) {
    RUNTIME.block_on(async {
        tokio::time::timeout(Duration::from_secs(5), test)
            .await
            .expect("writer test timed out");
    });
}

#[test]
fn finish_drains_input_once_and_retains_the_committed_result_after_cleanup() {
    run(async {
        let first = batch(vec![1, 2]);
        let writer = Writer::spawn(first.schema(), |mut stream| async move {
            let mut ids = Vec::new();
            while let Some(batch) = stream.try_next().await.map_err(Failure::from_error)? {
                let column = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap();
                ids.extend(column.values().iter().copied());
            }
            assert_eq!(ids, vec![1, 2, 3]);
            Ok(42)
        });
        writer.push(first).await.unwrap();
        writer.push(batch(vec![3])).await.unwrap();
        let result = writer.finish().await.unwrap();
        assert_eq!((result.inserted, result.version), (3, 42));
        assert_eq!(
            writer.finish().await.unwrap_err(),
            Failure::from("Writer is closed")
        );
        assert_eq!(
            writer.push(batch(vec![4])).await.unwrap_err(),
            Failure::from("Writer is closed")
        );
        writer.abort().await;
        writer.abort().await;
        let result = writer.outcome().await.unwrap();
        assert_eq!((result.inserted, result.version), (3, 42));
    });
}

#[test]
fn cancellation_of_full_input_unblocks_a_pending_push_and_prevents_successful_eof() {
    run(async {
        let first = batch(vec![1]);
        let (start, ready) = oneshot::channel();
        let writer = Writer::spawn(first.schema(), |mut stream| async move {
            ready.await.unwrap();
            // Cancellation wins even though a batch is already queued.
            let error = stream.try_next().await.unwrap_err();
            assert!(stream.try_next().await.unwrap().is_none());
            Err(Failure::from_error(error))
        });
        writer.push(first).await.unwrap();
        let pending = writer.push(batch(vec![2]));
        tokio::pin!(pending);
        assert!(futures::poll!(&mut pending).is_pending());
        writer.cancel();
        start.send(()).unwrap();
        assert!(pending
            .await
            .unwrap_err()
            .contains("Writer aborted before finish"));
        assert!(writer
            .outcome()
            .await
            .unwrap_err()
            .contains("Writer aborted before finish"));
        writer.abort().await;
    });
}

#[test]
fn cancel_during_finish_does_not_turn_a_full_queue_into_successful_eof() {
    run(async {
        let first = batch(vec![1]);
        let (start, ready) = oneshot::channel();
        let writer = Writer::spawn(first.schema(), |mut stream| async move {
            ready.await.unwrap();
            while stream
                .try_next()
                .await
                .map_err(Failure::from_error)?
                .is_some()
            {}
            Ok(99)
        });
        writer.push(first).await.unwrap();
        let finish = writer.finish();
        tokio::pin!(finish);
        assert!(futures::poll!(&mut finish).is_pending());
        assert_eq!(
            writer.check_open().unwrap_err(),
            Failure::from("Writer is closed")
        );
        writer.cancel();
        start.send(()).unwrap();
        assert!(finish
            .await
            .unwrap_err()
            .contains("Writer aborted before finish"));
        assert_eq!(
            writer.finish().await.unwrap_err(),
            Failure::from("Writer is closed")
        );
    });
}

#[test]
fn unexpected_channel_closure_is_one_error_followed_by_eof() {
    run(async {
        let (input, receiver) = mpsc::channel(1);
        let (_cancel, cancelled) = watch::channel(false);
        let count = Arc::new(AtomicUsize::new(0));
        let mut stream = input_stream(batch(vec![]).schema(), receiver, cancelled, count.clone());
        drop(input);
        assert!(stream
            .try_next()
            .await
            .unwrap_err()
            .to_string()
            .contains("Writer aborted before finish"));
        assert!(stream.try_next().await.unwrap().is_none());
        assert_eq!(count.load(Ordering::Relaxed), 0);
    });
}

#[test]
fn execution_and_commit_errors_remain_available_to_producers() {
    run(async {
        let writer = Writer::spawn(batch(vec![]).schema(), |_stream| async {
            Err("cannot open table".into())
        });
        assert_eq!(
            writer.outcome().await.unwrap_err(),
            Failure::from("cannot open table").executing().progress(0)
        );
        assert_eq!(
            writer.push(batch(vec![1])).await.unwrap_err(),
            Failure::from("cannot open table").executing().progress(0)
        );
        assert_eq!(
            writer.finish().await.unwrap_err(),
            Failure::from("cannot open table").executing().progress(0)
        );

        let writer = Writer::spawn(batch(vec![]).schema(), |mut stream| async move {
            while stream
                .try_next()
                .await
                .map_err(Failure::from_error)?
                .is_some()
            {}
            Err("commit failed".into())
        });
        writer.push(batch(vec![1])).await.unwrap();
        assert_eq!(
            writer.finish().await.unwrap_err(),
            Failure::from("commit failed").executing().progress(1)
        );
        writer.abort().await;
        assert_eq!(
            writer.outcome().await.unwrap_err(),
            Failure::from("commit failed").executing().progress(1)
        );
    });
}

struct Dropped(Arc<AtomicBool>);
impl Drop for Dropped {
    fn drop(&mut self) {
        self.0.store(true, Ordering::SeqCst);
    }
}

#[test]
fn dropping_the_handle_cancels_input_and_waits_for_executor_cleanup_before_publication() {
    run(async {
        let dropped = Arc::new(AtomicBool::new(false));
        let guard = Dropped(dropped.clone());
        let (started, running) = oneshot::channel();
        let writer = Writer::spawn(batch(vec![]).schema(), |mut stream| async move {
            let _guard = guard;
            started.send(()).unwrap();
            stream.try_next().await.map_err(Failure::from_error)?;
            Ok(1)
        });
        running.await.unwrap();
        let mut terminal = writer.terminal.clone();
        assert!(terminal.borrow().is_none());
        drop(writer);
        let result = terminal.wait_for(Option::is_some).await.unwrap();
        assert!(result
            .as_ref()
            .unwrap()
            .as_ref()
            .unwrap_err()
            .contains("Writer aborted before finish"));
        assert!(dropped.load(Ordering::SeqCst));
    });
}

#[test]
fn worker_failure_closes_completion_instead_of_hanging_the_producer() {
    run(async {
        let writer = Writer::spawn(batch(vec![]).schema(), |_stream| async {
            panic!("simulated executor panic");
            #[allow(unreachable_code)]
            Ok(1)
        });
        assert_eq!(
            writer.outcome().await.unwrap_err(),
            Failure::new("execution", "Native writer task panicked")
                .executing()
                .progress(0)
        );
        assert_eq!(
            writer.finish().await.unwrap_err(),
            Failure::new("execution", "Native writer task panicked")
                .executing()
                .progress(0)
        );
    });
}

#[test]
fn successful_eof_is_repeatable_for_empty_sdk_inputs() {
    run(async {
        let writer = Writer::spawn(batch(vec![]).schema(), |mut stream| async move {
            for _ in 0..3 {
                assert!(stream
                    .try_next()
                    .await
                    .map_err(Failure::from_error)?
                    .is_none());
            }
            Ok(7)
        });
        let result = writer.finish().await.unwrap();
        assert_eq!(result.version, 7);
        assert_eq!(result.inserted, 0);
    });
}
