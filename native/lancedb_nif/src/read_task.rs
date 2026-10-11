//! Bounded one-shot reads that yield BEAM schedulers and cancel on caller death.
use crate::error::Failure;
use crate::execution::{Deadline, Permit};
use crate::reply::Reply;
use crate::resources::{atoms, RUNTIME};
use futures::FutureExt;
use rustler::{Atom, Encoder, Env, LocalPid, Monitor, ResourceArc, Term};
use std::future::Future;
use std::panic::{AssertUnwindSafe, RefUnwindSafe, UnwindSafe};
use tokio::sync::watch;

struct ReadTask {
    cancel: watch::Sender<()>,
}

impl RefUnwindSafe for ReadTask {}
impl UnwindSafe for ReadTask {}

#[rustler::resource_impl]
impl rustler::Resource for ReadTask {
    fn down(&self, _env: Env<'_>, _pid: LocalPid, _monitor: Monitor) {
        self.cancel.send_replace(());
    }
}

pub(crate) fn spawn<T: Encoder + Send + 'static>(
    env: Env<'_>,
    reference: Term<'_>,
    deadline: Deadline,
    operation: impl Future<Output = Result<T, Failure>> + Send + 'static,
) -> Result<Atom, Failure> {
    // Admit before spawning, so the runtime never accumulates queued readers.
    let permit = Permit::read()?;
    let (cancel, cancelled) = watch::channel(());
    let task = ResourceArc::new(ReadTask { cancel });
    let monitor = task
        .monitor(Some(env), &env.pid())
        .ok_or("Could not monitor read caller")?;
    let mut reply = Reply::new(env, reference);
    RUNTIME.spawn(async move {
        let result = run(cancelled, deadline, operation).await;
        // The operation future and permit are gone before the reply is sent.
        drop(permit);
        task.demonitor(None, &monitor);
        drop(task);
        let _ = reply.send(result);
    });
    Ok(atoms::ok())
}

async fn run<T>(
    mut cancelled: watch::Receiver<()>,
    deadline: Deadline,
    operation: impl Future<Output = Result<T, Failure>>,
) -> Result<T, Failure> {
    AssertUnwindSafe(async {
        tokio::select! {
            biased;
            _ = cancelled.changed() => Err(Failure::new("execution", "Read caller terminated")),
            result = deadline.run(operation) => result,
        }
    })
    .catch_unwind()
    .await
    .unwrap_or_else(|_| Err(Failure::new("execution", "Native read task panicked")))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::execution::ExecutionOptions;
    use std::sync::atomic::{AtomicBool, Ordering};

    struct DropGuard<'a>(&'a AtomicBool);
    impl Drop for DropGuard<'_> {
        fn drop(&mut self) {
            self.0.store(true, Ordering::SeqCst);
        }
    }

    #[test]
    fn cancellation_and_timeout_drop_pending_work() {
        RUNTIME.block_on(async {
            for timeout in [false, true] {
                let dropped = AtomicBool::new(false);
                let guard = DropGuard(&dropped);
                let (cancel, cancelled) = watch::channel(());
                if !timeout {
                    cancel.send_replace(());
                }
                let deadline = ExecutionOptions {
                    timeout_ms: timeout.then_some(5),
                }
                .deadline_for("Table open");
                let result: Result<(), Failure> = run(cancelled, deadline, async move {
                    let _guard = guard;
                    std::future::pending().await
                })
                .await;
                assert_eq!(
                    result.unwrap_err().kind,
                    if timeout { "timeout" } else { "execution" }
                );
                assert!(dropped.load(Ordering::SeqCst));
            }
        });
    }

    #[test]
    fn panics_become_errors_and_successes_pass_through() {
        RUNTIME.block_on(async {
            let (_cancel, cancelled) = watch::channel(());
            let deadline = ExecutionOptions { timeout_ms: None }.deadline_for("Connection");
            assert_eq!(
                run(cancelled.clone(), deadline, async { Ok(42) }).await,
                Ok(42)
            );
            let result: Result<(), Failure> =
                run(cancelled, deadline, async { panic!("read panic") }).await;
            assert_eq!(result.unwrap_err().kind, "execution");
        });
    }
}
