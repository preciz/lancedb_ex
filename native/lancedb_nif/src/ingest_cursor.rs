//! BEAM producer ownership and term conversion for single-commit ingestion.
use crate::codec::rows_to_record_batch;
use crate::error::Failure;
use crate::ingestion::{
    self,
    worker::{Outcome, Writer},
    Options, ProgressTarget,
};
use crate::outcome::WriteOutput;
use crate::resources::{atoms, TableResource, RUNTIME};
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

#[rustler::nif(schedule = "DirtyIo")]
fn ingest_open(
    env: Env<'_>,
    table: ResourceArc<TableResource>,
    options: String,
    progress: Option<(LocalPid, Term<'_>)>,
) -> Result<ResourceArc<WriterResource>, Failure> {
    ingest_open_impl(env, table, options, progress).map_err(Failure::before_write)
}

fn ingest_open_impl(
    env: Env<'_>,
    table: ResourceArc<TableResource>,
    options: String,
    progress: Option<(LocalPid, Term<'_>)>,
) -> Result<ResourceArc<WriterResource>, Failure> {
    let options = Options::parse(&options)?;
    table.ensure_mutable()?;
    let schema = RUNTIME
        .block_on(table.table.schema())
        .map_err(Failure::from_error)?;
    let progress = progress.map(ProgressTarget::new);
    let worker = Writer::spawn_operation(schema.clone(), move |stream, count| async move {
        let version = ingestion::execute(&table.table, stream, options, progress).await?;
        Ok(WriteOutput::Add(ingestion::WriteResult {
            inserted: count.load(std::sync::atomic::Ordering::Relaxed),
            version,
        }))
    });
    WriterResource::start(env, schema, worker)
}

impl WriterResource {
    pub(crate) fn start(
        env: Env<'_>,
        schema: SchemaRef,
        worker: Writer<WriteOutput>,
    ) -> Result<ResourceArc<Self>, Failure> {
        let writer = ResourceArc::new(Self {
            owner: env.pid(),
            schema,
            worker,
        });
        if writer.monitor(Some(env), &env.pid()).is_none() {
            return Err("Could not monitor producer".into());
        }
        Ok(writer)
    }
}

#[rustler::nif(schedule = "DirtyIo")]
fn ingest_push(
    env: Env<'_>,
    writer: ResourceArc<WriterResource>,
    records: Term<'_>,
) -> Result<Atom, Failure> {
    writer.check_owner(env)?;
    writer.worker.check_open()?;
    let batch = rows_to_record_batch(
        crate::batch::rows_from_terms(records)?,
        writer.schema.clone(),
    )?;
    RUNTIME.block_on(writer.worker.push(batch))?;
    Ok(atoms::ok())
}

#[rustler::nif(schedule = "DirtyIo")]
fn ingest_finish(env: Env<'_>, writer: ResourceArc<WriterResource>) -> Outcome<WriteOutput> {
    writer.check_owner(env)?;
    RUNTIME.block_on(writer.worker.finish())
}

#[rustler::nif(schedule = "DirtyIo")]
fn ingest_abort(env: Env<'_>, writer: ResourceArc<WriterResource>) -> Result<Atom, Failure> {
    writer.check_owner(env)?;
    RUNTIME.block_on(writer.worker.abort());
    Ok(atoms::ok())
}

#[rustler::nif(schedule = "DirtyIo")]
fn ingest_push_batch(
    env: Env<'_>,
    writer: ResourceArc<WriterResource>,
    batch: ResourceArc<crate::batch::BatchResource>,
) -> Result<Atom, Failure> {
    writer.check_owner(env)?;
    writer.worker.check_open()?;
    let batch =
        lance_arrow::json::convert_json_columns(&batch.batch).map_err(Failure::from_error)?;
    if writer.schema.fields() != batch.schema().fields() {
        return Err("Batch fields do not match destination schema".into());
    }
    RUNTIME.block_on(writer.worker.push(batch))?;
    Ok(atoms::ok())
}
