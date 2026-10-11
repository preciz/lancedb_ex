//! Arrow-owned batch values. JSON is only a row adapter, never the cursor storage format.
use crate::{codec, error::Failure};
use arrow_array::RecordBatch;
use rustler::ResourceArc;
use std::panic::{RefUnwindSafe, UnwindSafe};
pub(crate) mod validation;

pub(crate) struct BatchResource {
    pub batch: RecordBatch,
}
impl RefUnwindSafe for BatchResource {}
impl UnwindSafe for BatchResource {}
#[rustler::resource_impl]
impl rustler::Resource for BatchResource {}

#[rustler::nif(schedule = "DirtyCpu")]
fn batch_rows(batch: ResourceArc<BatchResource>) -> Result<String, Failure> {
    codec::batches_to_json(vec![batch.batch.clone()])
}
#[rustler::nif]
fn batch_size(batch: ResourceArc<BatchResource>) -> usize {
    batch.batch.num_rows()
}
#[rustler::nif(schedule = "DirtyCpu")]
fn batch_schema(batch: ResourceArc<BatchResource>) -> String {
    crate::schema::schema_json(&batch.batch.schema())
}
#[rustler::nif(schedule = "DirtyCpu")]
fn batch_to_ipc<'a>(
    env: rustler::Env<'a>,
    batch: ResourceArc<BatchResource>,
) -> Result<rustler::Binary<'a>, Failure> {
    let mut bytes = Vec::new();
    {
        let mut writer =
            arrow::ipc::writer::StreamWriter::try_new(&mut bytes, &batch.batch.schema())
                .map_err(Failure::from_error)?;
        writer.write(&batch.batch).map_err(Failure::from_error)?;
        writer.finish().map_err(Failure::from_error)?;
    }
    let mut binary = rustler::OwnedBinary::new(bytes.len()).ok_or("Cannot allocate IPC binary")?;
    binary.as_mut_slice().copy_from_slice(&bytes);
    Ok(binary.release(env))
}

#[rustler::nif(schedule = "DirtyCpu")]
fn batch_from_ipc(bytes: rustler::Binary<'_>) -> Result<Vec<ResourceArc<BatchResource>>, Failure> {
    let batches = crate::compat::ipc::read_stream(bytes.as_slice())?;
    Ok(batches
        .into_iter()
        .map(|batch| ResourceArc::new(BatchResource { batch }))
        .collect())
}

// Decode owned values before the shared schema validation and Arrow conversion.
#[rustler::nif(schedule = "DirtyCpu")]
fn batch_from_terms(
    rows: rustler::Term<'_>,
    schema: String,
    metadata: std::collections::HashMap<String, String>,
) -> Result<ResourceArc<BatchResource>, Failure> {
    let schema = crate::schema::definition::explicit_schema(&schema)?;
    let schema = std::sync::Arc::new(schema.as_ref().clone().with_metadata(metadata));
    let rows = rows_from_terms(rows)?;
    let batch = codec::rows_to_record_batch(rows, schema)?;
    Ok(ResourceArc::new(BatchResource { batch }))
}

// Bound both decoding and serde_json::Value's recursive drop on dirty scheduler
// stacks. Check before constructing children, including schema-invalid values.
const MAX_TERM_DEPTH: usize = 64;

fn term_value(term: rustler::Term<'_>, depth: usize) -> Result<serde_json::Value, Failure> {
    use serde_json::Value;
    if depth > MAX_TERM_DEPTH {
        return Err("Record nesting exceeds the maximum depth of 64".into());
    }
    if term.is_map() {
        let mut values = serde_json::Map::new();
        for (key, value) in rustler::types::map::MapIterator::new(term).unwrap() {
            let key: String = if key.is_atom() {
                key.atom_to_string()
            } else {
                key.decode()
            }
            .map_err(|_| "Row keys must be strings or atoms")?;
            if values.insert(key, term_value(value, depth + 1)?).is_some() {
                return Err("Duplicate row keys".into());
            }
        }
        Ok(Value::Object(values))
    } else if term.is_list() || term.is_empty_list() {
        let values: Vec<rustler::Term<'_>> = term.decode().map_err(|_| "Expected a proper list")?;
        Ok(Value::Array(
            values
                .into_iter()
                .map(|value| term_value(value, depth + 1))
                .collect::<Result<_, _>>()?,
        ))
    } else if term.is_binary() {
        Ok(Value::String(
            term.decode().map_err(|_| "Expected UTF-8 wire value")?,
        ))
    } else if term.is_atom() {
        match term.atom_to_string().map_err(|_| "Invalid atom")?.as_str() {
            "nil" => Ok(Value::Null),
            "true" => Ok(Value::Bool(true)),
            "false" => Ok(Value::Bool(false)),
            _ => Err("Unsupported value atom".into()),
        }
    } else if let Ok(value) = term.decode::<i64>() {
        Ok(value.into())
    } else if let Ok(value) = term.decode::<u64>() {
        Ok(value.into())
    } else if let Ok(value) = term.decode::<f64>() {
        serde_json::Number::from_f64(value)
            .map(Value::Number)
            .ok_or_else(|| "Expected finite float".into())
    } else {
        Err("Unsupported wire value".into())
    }
}

pub(crate) fn rows_from_terms(rows: rustler::Term<'_>) -> Result<Vec<serde_json::Value>, Failure> {
    match term_value(rows, 0)? {
        serde_json::Value::Array(rows) => Ok(rows),
        _ => Err("Rows must be a list".into()),
    }
}

#[rustler::nif(schedule = "DirtyCpu")]
fn batch_infer(rows: rustler::Term<'_>) -> Result<ResourceArc<BatchResource>, Failure> {
    let batch = codec::inferred_record_batch(rows_from_terms(rows)?)?;
    Ok(ResourceArc::new(BatchResource { batch }))
}

#[rustler::nif(schedule = "DirtyCpu")]
fn schema_validate(schema: String) -> Result<rustler::Atom, Failure> {
    crate::schema::storage::StorageSchema::prepare(crate::schema::definition::explicit_schema(
        &schema,
    )?)?;
    Ok(crate::resources::atoms::ok())
}
