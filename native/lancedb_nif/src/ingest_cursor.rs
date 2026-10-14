//! BEAM producer ownership and term conversion for single-commit ingestion.
use crate::codec::rows_to_record_batch;
use crate::error::Failure;
use crate::ingestion::{self, worker::Writer, Options, ProgressTarget};
use crate::outcome::WriteOutput;
use crate::resources::{atoms, TableResource};
use arrow_schema::SchemaRef;
use rustler::{Atom, Env, LocalPid, Monitor, ResourceArc, Term};
use std::panic::{RefUnwindSafe, UnwindSafe};

pub struct WriterResource {
    owner: LocalPid,
    schema: SchemaRef,
    worker: Writer<WriteOutput>,
}

impl RefUnwindSafe for WriterResource {}
impl UnwindSafe for WriterResource {}

#[rustler::resource_impl]
impl rustler::Resource for WriterResource {
    fn down(&self, _env: Env<'_>, _pid: LocalPid, _monitor: Monitor) {
        self.worker.cancel();
    }
}

impl WriterResource {
    fn check_owner(&self, env: Env<'_>) -> Result<(), Failure> {
        if self.owner != env.pid() {
            return Err("Writer belongs to another process".into());
        }
        Ok(())
    }
}

#[rustler::nif(schedule = "DirtyCpu")]
fn ingest_open(
    env: Env<'_>,
    table: ResourceArc<TableResource>,
    options: String,
    progress: Option<(LocalPid, Term<'_>)>,
    reference: Term<'_>,
) -> Result<Atom, Failure> {
    let owner = env.pid();
    let progress = progress.map(ProgressTarget::new);
    crate::request::write(env, reference, move |permit| async move {
        ingest_open_impl(owner, table, options, progress, permit)
            .await
            .map_err(Failure::before_write)
    })
}

async fn ingest_open_impl(
    owner: LocalPid,
    table: ResourceArc<TableResource>,
    options: String,
    progress: Option<ProgressTarget>,
    permit: crate::execution::Permit,
) -> Result<ResourceArc<WriterResource>, Failure> {
    let options = Options::parse(&options)?;
    table.ensure_mutable()?;
    let schema = table.table.schema().await.map_err(Failure::from_error)?;
    let worker = Writer::spawn_operation(schema.clone(), move |stream, count| async move {
        let _permit = permit;
        let version = ingestion::execute(&table.table, stream, options, progress).await?;
        Ok(WriteOutput::Add(ingestion::WriteResult {
            inserted: count.load(std::sync::atomic::Ordering::Relaxed),
            version,
        }))
    })?;
    WriterResource::start(owner, schema, worker)
}

impl WriterResource {
    pub(crate) fn start(
        owner: LocalPid,
        schema: SchemaRef,
        worker: Writer<WriteOutput>,
    ) -> Result<ResourceArc<Self>, Failure> {
        let writer = ResourceArc::new(Self {
            owner,
            schema,
            worker,
        });
        if writer.monitor(None, &owner).is_none() {
            return Err("Could not monitor producer".into());
        }
        Ok(writer)
    }
}

#[rustler::nif(schedule = "DirtyCpu")]
fn ingest_push(
    env: Env<'_>,
    writer: ResourceArc<WriterResource>,
    records: Term<'_>,
    reference: Term<'_>,
) -> Result<Atom, Failure> {
    writer.check_owner(env)?;
    writer.worker.check_open()?;
    let batch = rows_to_record_batch(
        crate::batch::rows_from_terms(records)?,
        writer.schema.clone(),
    )?;
    crate::request::settle(env, reference, async move {
        writer.worker.push(batch).await?;
        Ok(atoms::ok())
    })
}

#[rustler::nif]
fn ingest_finish(
    env: Env<'_>,
    writer: ResourceArc<WriterResource>,
    reference: Term<'_>,
) -> Result<Atom, Failure> {
    writer.check_owner(env)?;
    crate::request::settle(env, reference, async move { writer.worker.finish().await })
}

#[rustler::nif]
fn ingest_abort(
    env: Env<'_>,
    writer: ResourceArc<WriterResource>,
    reference: Term<'_>,
) -> Result<Atom, Failure> {
    writer.check_owner(env)?;
    crate::request::settle(env, reference, async move {
        writer.worker.abort().await;
        Ok(atoms::ok())
    })
}

#[rustler::nif(schedule = "DirtyCpu")]
fn ingest_push_batch(
    env: Env<'_>,
    writer: ResourceArc<WriterResource>,
    batch: ResourceArc<crate::batch::BatchResource>,
    reference: Term<'_>,
) -> Result<Atom, Failure> {
    writer.check_owner(env)?;
    writer.worker.check_open()?;
    let batch = crate::compat::json::mask_unreachable(&batch.batch)?;
    let batch = lance_arrow::json::convert_json_columns(&batch).map_err(Failure::from_error)?;
    crate::request::settle(env, reference, async move {
        writer.worker.push(batch).await?;
        Ok(atoms::ok())
    })
}
