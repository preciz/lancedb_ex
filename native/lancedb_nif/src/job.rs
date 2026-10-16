//! Shareable jobs outlive callers; admission remains held until native completion.
use crate::{
    error::Failure,
    execution::Permit,
    resources::{atoms, RUNTIME},
};
use rustler::{Atom, Encoder, Env, ResourceArc, Term};
use std::{
    panic::{RefUnwindSafe, UnwindSafe},
    sync::Arc,
    time::Duration,
};

#[derive(Clone)]
enum JobHandle {
    Index(Arc<lancedb::job::Job>),
    Refresh(Arc<lancedb::job::Job<lancedb::function::RefreshColumnResult>>),
}

#[derive(Clone)]
enum JobResult {
    Created,
    Refresh(crate::computed::RefreshResult),
}

impl Encoder for JobResult {
    fn encode<'a>(&self, env: Env<'a>) -> Term<'a> {
        match self {
            Self::Created => atoms::created().encode(env),
            Self::Refresh(result) => crate::wire::Refresh(result.clone()).encode(env),
        }
    }
}

impl JobHandle {
    async fn wait(&self) -> Result<JobResult, Failure> {
        match self {
            Self::Index(job) => job.wait().await.map(|()| JobResult::Created),
            Self::Refresh(job) => job
                .wait()
                .await
                .map(|result| JobResult::Refresh(result.into())),
        }
        .map_err(Failure::write_error)
    }

    async fn status(&self) -> lancedb::Result<String> {
        match self {
            Self::Index(job) => job.status().await,
            Self::Refresh(job) => job.status().await,
        }
    }

    async fn cancel(&self) -> lancedb::Result<()> {
        match self {
            Self::Index(job) => job.cancel().await,
            Self::Refresh(job) => job.cancel().await,
        }
    }
}

pub struct JobResource {
    job: JobHandle,
    terminal: tokio::sync::watch::Receiver<Option<Result<JobResult, Failure>>>,
}
impl RefUnwindSafe for JobResource {}
impl UnwindSafe for JobResource {}
#[rustler::resource_impl]
impl rustler::Resource for JobResource {}
impl JobResource {
    pub(crate) fn new(job: lancedb::job::Job, permit: Permit) -> ResourceArc<Self> {
        Self::start(JobHandle::Index(Arc::new(job)), permit)
    }

    pub(crate) fn refresh(
        job: lancedb::job::Job<lancedb::function::RefreshColumnResult>,
        permit: Permit,
    ) -> ResourceArc<Self> {
        Self::start(JobHandle::Refresh(Arc::new(job)), permit)
    }

    fn start(job: JobHandle, permit: Permit) -> ResourceArc<Self> {
        let waiting = job.clone();
        let (complete, terminal) = tokio::sync::watch::channel(None);
        RUNTIME.spawn(async move {
            let outcome = waiting.wait().await;
            drop(permit);
            complete.send_replace(Some(outcome));
        });
        ResourceArc::new(Self { job, terminal })
    }
    async fn wait(&self) -> Result<JobResult, Failure> {
        let mut terminal = self.terminal.clone();
        let result = terminal
            .wait_for(Option::is_some)
            .await
            .map_err(|_| Failure::new("execution", "Native job watcher terminated"))?;
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
                    Failure::new("timeout", "Job wait timed out; job continues running").waiting()
                })?,
        }
        .map_err(Failure::from_error)
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
