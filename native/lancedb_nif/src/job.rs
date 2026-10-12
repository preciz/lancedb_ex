//! Shareable jobs outlive callers; admission remains held until native completion.
use crate::{
    error::Failure,
    execution::Permit,
    resources::{atoms, RUNTIME},
};
use rustler::{Atom, Env, ResourceArc, Term};
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
#[rustler::nif]
fn job_status(
    env: Env<'_>,
    job: ResourceArc<JobResource>,
    reference: Term<'_>,
) -> Result<Atom, Failure> {
    crate::request::settle(env, reference, async move {
        job.job.status().await.map_err(Failure::from_error)
    })
}
#[rustler::nif]
fn job_wait(
    env: Env<'_>,
    job: ResourceArc<JobResource>,
    timeout: Option<u64>,
    reference: Term<'_>,
) -> Result<Atom, Failure> {
    crate::reply::spawn(env, reference, async move {
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
        Ok::<_, Failure>(atoms::created())
    });
    Ok(atoms::ok())
}
#[rustler::nif]
fn job_cancel(
    env: Env<'_>,
    job: ResourceArc<JobResource>,
    reference: Term<'_>,
) -> Result<Atom, Failure> {
    crate::request::settle(env, reference, async move {
        job.job.cancel().await.map_err(Failure::from_error)?;
        Ok(atoms::ok())
    })
}
