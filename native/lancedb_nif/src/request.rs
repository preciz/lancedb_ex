//! One-shot requests yield schedulers; writes retain admission until SDK settlement.
use crate::error::Failure;
use crate::execution::{ExecutionOptions, Permit};
use crate::resources::atoms;
use futures::FutureExt;
use rustler::{Atom, Encoder, Env, Term};
use std::{future::Future, panic::AssertUnwindSafe};

pub(crate) fn read<T: Encoder + Send + 'static>(
    env: Env<'_>,
    reference: Term<'_>,
    operation: impl Future<Output = Result<T, Failure>> + Send + 'static,
) -> Result<Atom, Failure> {
    crate::read_task::spawn(
        env,
        reference,
        ExecutionOptions { timeout_ms: None }.deadline(),
        operation,
    )
}

pub(crate) fn write<T, F>(
    env: Env<'_>,
    reference: Term<'_>,
    operation: impl FnOnce(Permit) -> F,
) -> Result<Atom, Failure>
where
    T: Encoder + Send + 'static,
    F: Future<Output = Result<T, Failure>> + Send + 'static,
{
    // Admit before spawning. A streaming writer or index job can transfer this
    // permit to its completion watcher rather than releasing it at preparation.
    let permit = Permit::write().map_err(Failure::before_write)?;
    let operation = operation(permit);
    settle(env, reference, async move {
        // Mutation calls mark execution errors explicitly. Unmarked failures
        // occur during preparation; never replace an existing write boundary.
        operation.await.map_err(Failure::before_write)
    })
}

pub(crate) fn settle<T: Encoder + Send + 'static>(
    env: Env<'_>,
    reference: Term<'_>,
    operation: impl Future<Output = Result<T, Failure>> + Send + 'static,
) -> Result<Atom, Failure> {
    // Never cancel a mutation future when its caller exits: the SDK may have
    // started a commit. Writer commands already share their worker's permit.
    crate::reply::spawn(env, reference, settled(operation));
    Ok(atoms::ok())
}

async fn settled<T>(operation: impl Future<Output = Result<T, Failure>>) -> Result<T, Failure> {
    AssertUnwindSafe(operation)
        .catch_unwind()
        .await
        .unwrap_or_else(
            |_| Err(Failure::new("execution", "Native write task panicked").executing()),
        )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::resources::RUNTIME;

    #[test]
    fn writes_settle_and_panics_report_an_unknown_commit_outcome() {
        RUNTIME.block_on(async {
            let (complete, pending) = tokio::sync::oneshot::channel();
            let waiting = RUNTIME.spawn(settled(async move {
                pending.await.unwrap();
                Ok(42)
            }));
            tokio::task::yield_now().await;
            assert!(!waiting.is_finished());
            complete.send(()).unwrap();
            assert_eq!(waiting.await.unwrap(), Ok(42));
            let result: Result<(), Failure> = settled(async { panic!("write panic") }).await;
            let error = result.unwrap_err();
            assert_eq!(error.phase, Some("execution"));
            assert_eq!(error.commit_outcome, Some("unknown"));
        });
    }
}
