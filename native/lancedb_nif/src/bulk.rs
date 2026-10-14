//! Single-commit creation, merges and column backfills share producer ownership.
use crate::{
    error::Failure,
    execution::Permit,
    ingest_cursor::WriterResource,
    ingestion::worker::Writer,
    outcome::WriteOutput,
    resources::{ConnectionResource, TableResource, RUNTIME},
};
use arrow_array::{RecordBatch, RecordBatchReader};
use arrow_schema::{ArrowError, SchemaRef};
use futures::{StreamExt, TryStreamExt};
use lancedb::arrow::SendableRecordBatchStream;
use rustler::{Atom, Env, LocalPid, ResourceArc, Term};
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

#[rustler::nif(schedule = "DirtyCpu")]
fn create_stream_open(
    env: Env<'_>,
    conn: ResourceArc<ConnectionResource>,
    name: String,
    schema: String,
    metadata: HashMap<String, String>,
    mode: String,
    reference: Term<'_>,
) -> Result<Atom, Failure> {
    let owner = env.pid();
    crate::request::write(env, reference, move |permit| async move {
        create_stream_open_impl(owner, conn, name, schema, metadata, mode, permit)
            .await
            .map_err(Failure::before_write)
    })
}

async fn create_stream_open_impl(
    owner: LocalPid,
    conn: ResourceArc<ConnectionResource>,
    name: String,
    schema: String,
    metadata: HashMap<String, String>,
    mode: String,
    permit: Permit,
) -> Result<ResourceArc<WriterResource>, Failure> {
    let schema = crate::schema::storage::StorageSchema::prepare(
        crate::schema::definition::explicit_schema(&schema)?,
    )?
    .into_arrow();
    let schema = Arc::new(schema.as_ref().clone().with_metadata(metadata));
    let options = crate::creation::CreateOptions::parse(&mode)?.prepare(&conn.config)?;
    let worker = Writer::spawn_operation(schema.clone(), move |stream, _| async move {
        let _permit = permit;
        let table = options
            .create(&conn.conn, name, stream)
            .await
            .map_err(Failure::from_error)?;
        Ok(WriteOutput::Created(Arc::new(table)))
    })?;
    WriterResource::start(owner, schema, worker)
}

#[rustler::nif(schedule = "DirtyCpu")]
fn merge_stream_open(
    env: Env<'_>,
    table: ResourceArc<TableResource>,
    keys: Vec<String>,
    options: String,
    reference: Term<'_>,
) -> Result<Atom, Failure> {
    let owner = env.pid();
    crate::request::write(env, reference, move |permit| async move {
        merge_stream_open_impl(owner, table, keys, options, permit)
            .await
            .map_err(Failure::before_write)
    })
}

async fn merge_stream_open_impl(
    owner: LocalPid,
    table: ResourceArc<TableResource>,
    keys: Vec<String>,
    options: String,
    permit: Permit,
) -> Result<ResourceArc<WriterResource>, Failure> {
    table.ensure_mutable()?;
    let schema = table.table.schema().await.map_err(Failure::from_error)?;
    for key in &keys {
        let field = schema.field_with_name(key).map_err(Failure::from_error)?;
        if field.data_type() != &arrow_schema::DataType::Utf8 && !field.data_type().is_integer() {
            return Err("Merge keys must be string or integer columns".into());
        }
    }
    if keys.is_empty() {
        return Err("Merge keys cannot be empty".into());
    }
    let options = serde_json::from_str(&options).map_err(Failure::from_error)?;
    let worker = Writer::spawn_operation(schema.clone(), move |stream, _| async move {
        let _permit = permit;
        let merge = crate::write::merge_builder(&table.table, &keys, options);
        let reader = StreamReader::new(stream, keys);
        let result = merge
            .execute(Box::new(reader))
            .await
            .map_err(Failure::from_error)?;
        Ok(WriteOutput::Merge(result.into()))
    })?;
    WriterResource::start(owner, schema, worker)
}

#[rustler::nif(schedule = "DirtyCpu")]
fn columns_stream_open(
    env: Env<'_>,
    table: ResourceArc<TableResource>,
    schema: String,
    expected_version: Option<u64>,
    reference: Term<'_>,
) -> Result<Atom, Failure> {
    let owner = env.pid();
    crate::request::write(env, reference, move |permit| async move {
        columns_stream_open_impl(owner, table, schema, expected_version, permit)
            .await
            .map_err(Failure::before_write)
    })
}

async fn columns_stream_open_impl(
    owner: LocalPid,
    table: ResourceArc<TableResource>,
    schema: String,
    expected_version: Option<u64>,
    permit: Permit,
) -> Result<ResourceArc<WriterResource>, Failure> {
    table.ensure_mutable()?;
    let schema = crate::schema::storage::StorageSchema::prepare(
        crate::schema::definition::explicit_schema(&schema)?,
    )?
    .into_arrow();
    let wrapper = table
        .table
        .dataset()
        .ok_or("Column backfills require an embedded table")?;
    let mut dataset = wrapper
        .get()
        .await
        .map_err(Failure::from_error)?
        .as_ref()
        .clone();
    if expected_version.is_some_and(|v| v != dataset.version().version) {
        return Err(Failure::new(
            "conflict",
            "Table changed before column backfill started",
        ));
    }
    let expected_rows = dataset
        .count_rows(None)
        .await
        .map_err(Failure::from_error)?;
    let worker = Writer::spawn_operation(schema.clone(), move |stream, _| async move {
        let _permit = permit;
        let mut stream = checked_columns(stream, expected_rows);
        // Empty datasets may never poll a transform. Validate EOF before commit.
        if expected_rows == 0 {
            while let Some(batch) = stream.next().await {
                batch.map_err(Failure::from_error)?;
            }
        }
        let stream = Box::pin(
            lancedb::datafusion::physical_plan::stream::RecordBatchStreamAdapter::new(
                stream.schema(),
                stream.map_err(|error| {
                    lancedb::datafusion::error::DataFusionError::External(Box::new(error))
                }),
            ),
        );
        dataset
            .add_columns(
                lancedb::table::NewColumnTransform::Stream(stream),
                None,
                None,
            )
            .await
            .map_err(Failure::from_error)?;
        let version = dataset.version().version;
        table.table.dataset().unwrap().update(dataset);
        Ok(WriteOutput::Columns(version))
    })?;
    WriterResource::start(owner, schema, worker)
}

// SDK merge/backfill readers are polled on background blocking threads. Yield the
// Tokio worker when called from a runtime thread; never block a BEAM scheduler.
struct StreamReader {
    stream: SendableRecordBatchStream,
    schema: SchemaRef,
    keys: Vec<String>,
    seen: HashSet<Vec<String>>,
}
impl StreamReader {
    fn new(stream: SendableRecordBatchStream, keys: Vec<String>) -> Self {
        Self {
            schema: stream.schema(),
            stream,
            keys,
            seen: HashSet::new(),
        }
    }
    fn validate_keys(&mut self, batch: &RecordBatch) -> Result<(), ArrowError> {
        if self.keys.is_empty() {
            return Ok(());
        }
        for row in 0..batch.num_rows() {
            let mut key = Vec::new();
            for column in &self.keys {
                let array = batch
                    .column_by_name(column)
                    .ok_or_else(|| ArrowError::InvalidArgumentError("Missing merge key".into()))?;
                if array.is_null(row) {
                    return Err(ArrowError::InvalidArgumentError(
                        "Merge keys cannot be null".into(),
                    ));
                }
                key.push(arrow_cast::display::array_value_to_string(
                    array.as_ref(),
                    row,
                )?);
            }
            if !self.seen.insert(key) {
                return Err(ArrowError::InvalidArgumentError(
                    "Merge records must have unique keys across the entire stream".into(),
                ));
            }
        }
        Ok(())
    }
}
impl Iterator for StreamReader {
    type Item = Result<RecordBatch, ArrowError>;
    fn next(&mut self) -> Option<Self::Item> {
        let result = tokio::task::block_in_place(|| RUNTIME.block_on(self.stream.next()))?;
        Some(
            result
                .map_err(|e| ArrowError::ExternalError(Box::new(e)))
                .and_then(|batch| {
                    self.validate_keys(&batch)?;
                    Ok(batch)
                }),
        )
    }
}
impl RecordBatchReader for StreamReader {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

// Withhold the final rows until explicit EOF: Lance's column transform can stop
// reading once it has enough rows, so otherwise a late producer error is lost.
fn checked_columns(
    stream: SendableRecordBatchStream,
    expected: usize,
) -> SendableRecordBatchStream {
    let schema = stream.schema();
    let checked = futures::stream::unfold(
        (stream, 0usize, false),
        move |(mut stream, mut count, done)| async move {
            if done {
                return None;
            }
            let result = async {
                let Some(batch) = stream.next().await else {
                    return if count == expected {
                        Ok(None)
                    } else {
                        Err(lancedb::Error::InvalidInput {
                            message: "Column backfill has too few rows".into(),
                        })
                    };
                };
                let batch = batch?;
                count += batch.num_rows();
                if count > expected {
                    return Err(lancedb::Error::InvalidInput {
                        message: "Column backfill has too many rows".into(),
                    });
                }
                if count == expected {
                    while let Some(extra) = stream.next().await {
                        if extra?.num_rows() != 0 {
                            return Err(lancedb::Error::InvalidInput {
                                message: "Column backfill has too many rows".into(),
                            });
                        }
                    }
                }
                Ok(Some(batch))
            }
            .await;
            match result {
                Ok(None) => None,
                Ok(Some(batch)) => Some((Ok(batch), (stream, count, count == expected))),
                Err(error) => Some((Err(error), (stream, count, true))),
            }
        },
    );
    Box::pin(lancedb::arrow::SimpleRecordBatchStream {
        schema,
        stream: checked.fuse(),
    })
}
