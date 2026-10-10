//! Shareable jobs outlive callers; admission remains held until native completion.
use crate::{
    error::Failure,
    execution::Permit,
    resources::{atoms, RUNTIME},
};
use rustler::{Atom, ResourceArc};
use std::{
    panic::{RefUnwindSafe, UnwindSafe},
    sync::Arc,
    time::Duration,
};

pub struct JobResource {
    job: Arc<lancedb::job::Job>,
    terminal: tokio::sync::watch::Receiver<Option<Result<(), Failure>>>,
}
impl RefUnwindSafe for JobResource {}
impl UnwindSafe for JobResource {}
#[rustler::resource_impl]
impl rustler::Resource for JobResource {}
impl JobResource {
    pub(crate) fn new(job: lancedb::job::Job, permit: Permit) -> ResourceArc<Self> {
        let job = Arc::new(job);
        let waiting = job.clone();
        let (complete, terminal) = tokio::sync::watch::channel(None);
        RUNTIME.spawn(async move {
            let outcome = waiting.wait().await.map_err(Failure::write_error);
            drop(permit);
            complete.send_replace(Some(outcome));
        });
        ResourceArc::new(Self { job, terminal })
    }
    async fn wait(&self) -> Result<(), Failure> {
        let mut terminal = self.terminal.clone();
        let result = terminal
            .wait_for(Option::is_some)
            .await
            .map_err(|_| Failure::new("execution", "Index job watcher terminated"))?;
        result.clone().unwrap()
    }
}
#[rustler::nif(schedule = "DirtyIo")]
fn job_status(job: ResourceArc<JobResource>) -> Result<String, Failure> {
    RUNTIME
        .block_on(job.job.status())
        .map_err(Failure::from_error)
}
#[rustler::nif(schedule = "DirtyIo")]
fn job_wait(job: ResourceArc<JobResource>, timeout: Option<u64>) -> Result<Atom, Failure> {
    RUNTIME.block_on(async {
        match timeout {
            None => job.wait().await,
            Some(ms) => tokio::time::timeout(Duration::from_millis(ms), job.wait())
                .await
                .map_err(|_| {
                    Failure::new("timeout", "Index job wait timed out; job continues running")
                        .waiting()
                })?,
        }
        .map_err(Failure::from_error)?;
        Ok(atoms::created())
    })
}
#[rustler::nif(schedule = "DirtyIo")]
fn job_cancel(job: ResourceArc<JobResource>) -> Result<Atom, Failure> {
    RUNTIME
        .block_on(job.job.cancel())
        .map_err(Failure::from_error)?;
    Ok(atoms::ok())
}
