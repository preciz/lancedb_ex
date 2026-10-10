use crate::error::Failure;
#[derive(rustler::NifUnitEnum)]
pub(crate) enum QueryOperation {
    Explain,
    ExplainVerbose,
    Analyze,
    Schema,
}

use std::future::Future;
use std::time::{Duration, Instant};

#[derive(Clone, Copy, serde::Deserialize)]
pub(crate) struct ExecutionOptions {
    pub(crate) timeout_ms: Option<u32>,
}

/// A deadline survives the native/BEAM/native phases without restarting its budget.
#[derive(Clone, Copy)]
pub(crate) struct Deadline {
    at: Option<Instant>,
    timeout_ms: Option<u32>,
}

impl ExecutionOptions {
    pub(crate) fn deadline(self) -> Deadline {
        Deadline {
            at: self
                .timeout_ms
                .map(|ms| Instant::now() + Duration::from_millis(ms.into())),
            timeout_ms: self.timeout_ms,
        }
    }

    pub(crate) async fn run<T>(
        self,
        operation: impl Future<Output = Result<T, Failure>>,
    ) -> Result<T, Failure> {
        self.deadline().run(operation).await
    }
}

impl Deadline {
    pub(crate) fn check(self) -> Result<(), Failure> {
        if self.at.is_some_and(|at| Instant::now() >= at) {
            Err(self.error())
        } else {
            Ok(())
        }
    }

    fn error(self) -> Failure {
        Failure::new(
            "timeout",
            format!("Query timed out after {} ms", self.timeout_ms.unwrap()),
        )
    }

    pub(crate) async fn run<T>(
        self,
        operation: impl Future<Output = Result<T, Failure>>,
    ) -> Result<T, Failure> {
        self.check()?;
        let result = match self.at {
            Some(at) => tokio::time::timeout_at(tokio::time::Instant::from_std(at), operation)
                .await
                .map_err(|_| self.error())?,
            None => operation.await,
        };
        // Include synchronous final processing that may not yield to Tokio.
        self.check()?;
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::resources::RUNTIME;
    use std::sync::atomic::{AtomicBool, Ordering};

    #[test]
    fn deadline_drops_pending_work_after_execution_has_started() {
        struct DropGuard<'a>(&'a AtomicBool);
        impl Drop for DropGuard<'_> {
            fn drop(&mut self) {
                self.0.store(true, Ordering::SeqCst);
            }
        }

        let dropped = AtomicBool::new(false);
        let options = ExecutionOptions {
            timeout_ms: Some(5),
        };
        let result: Result<(), Failure> = RUNTIME.block_on(options.run(async {
            let _guard = DropGuard(&dropped);
            tokio::task::yield_now().await;
            // Simulate waiting for a later result batch, not just initial execution.
            std::future::pending().await
        }));
        assert_eq!(
            result,
            Err(Failure::new("timeout", "Query timed out after 5 ms"))
        );
        assert!(dropped.load(Ordering::SeqCst));
    }

    #[test]
    fn successful_results_and_errors_pass_through_with_or_without_deadlines() {
        for timeout_ms in [None, Some(10_000)] {
            let options = ExecutionOptions { timeout_ms };
            assert_eq!(RUNTIME.block_on(options.run(async { Ok(42) })), Ok(42));
            let result: Result<(), Failure> =
                RUNTIME.block_on(options.run(async { Err("query failed".into()) }));
            assert_eq!(result, Err("query failed".into()));
        }
    }
}

// Separate read and write budgets allow a query cursor to feed a writer even
// with a limit of one. Admission never queues an unbounded number of jobs.
use std::sync::atomic::{AtomicUsize, Ordering};
static READ_LIMIT: AtomicUsize = AtomicUsize::new(64);
static WRITE_LIMIT: AtomicUsize = AtomicUsize::new(16);
static READ_ACTIVE: AtomicUsize = AtomicUsize::new(0);
static WRITE_ACTIVE: AtomicUsize = AtomicUsize::new(0);

pub(crate) struct Permit(&'static AtomicUsize);
impl Permit {
    fn acquire(active: &'static AtomicUsize, limit: &AtomicUsize) -> Result<Self, Failure> {
        active
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |count| {
                (count < limit.load(Ordering::Acquire)).then_some(count + 1)
            })
            .map_err(|_| Failure::new("busy", "Native operation concurrency limit reached"))?;
        Ok(Self(active))
    }
    pub(crate) fn read() -> Result<Self, Failure> {
        Self::acquire(&READ_ACTIVE, &READ_LIMIT)
    }
    pub(crate) fn write() -> Result<Self, Failure> {
        Self::acquire(&WRITE_ACTIVE, &WRITE_LIMIT)
    }
}
impl Drop for Permit {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::AcqRel);
    }
}

#[rustler::nif]
fn execution_limits(reads: usize, writes: usize) -> Result<rustler::Atom, Failure> {
    if reads == 0 || writes == 0 {
        return Err("Execution limits must be positive".into());
    }
    READ_LIMIT.store(reads, Ordering::Release);
    WRITE_LIMIT.store(writes, Ordering::Release);
    Ok(crate::resources::atoms::ok())
}

#[cfg(test)]
mod admission_tests {
    use super::*;
    #[test]
    fn admission_is_nonblocking_and_dropping_work_releases_capacity() {
        static ACTIVE: AtomicUsize = AtomicUsize::new(0);
        let limit = AtomicUsize::new(1);
        let permit = Permit::acquire(&ACTIVE, &limit).unwrap();
        assert!(matches!(
            Permit::acquire(&ACTIVE, &limit),
            Err(Failure { kind: "busy", .. })
        ));
        drop(permit);
        let _next = Permit::acquire(&ACTIVE, &limit).unwrap();
        assert_eq!(ACTIVE.load(Ordering::Acquire), 1);
    }
}
