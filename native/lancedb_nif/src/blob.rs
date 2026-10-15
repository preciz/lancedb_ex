//! Blob resources share the SDK's synchronized cursor and release storage references on drop.
pub(crate) mod schema;
pub(crate) mod write_options;
use crate::{
    error::Failure,
    resources::{atoms, TableResource},
};
use arrow_array::Array;
use lancedb::blob::{BlobFile, BlobRangeRequest};
use rustler::{Atom, Binary, Encoder, Env, OwnedBinary, OwnedEnv, ResourceArc, Term};
use std::panic::{RefUnwindSafe, UnwindSafe};

pub(crate) struct BlobResource {
    file: BlobFile,
}
impl RefUnwindSafe for BlobResource {}
impl UnwindSafe for BlobResource {}
#[rustler::resource_impl]
impl rustler::Resource for BlobResource {}

fn binary<'a>(env: Env<'a>, bytes: &[u8]) -> Result<Binary<'a>, Failure> {
    let mut output = OwnedBinary::new(bytes.len()).ok_or("Cannot allocate blob binary")?;
    output.as_mut_slice().copy_from_slice(bytes);
    Ok(output.release(env))
}

fn binaries<'a>(
    env: Env<'a>,
    values: &arrow_array::LargeBinaryArray,
) -> Result<Vec<Option<Binary<'a>>>, Failure> {
    (0..values.len())
        .map(|i| {
            if values.is_null(i) {
                Ok(None)
            } else {
                binary(env, values.value(i)).map(Some)
            }
        })
        .collect()
}

// Encode in an owned environment after I/O completes. Copy bytes once into a
// BEAM binary; transferring the saved term to the reply only copies references.
struct BlobValue {
    env: OwnedEnv,
    value: rustler::env::SavedTerm,
}
impl BlobValue {
    fn new(operation: impl FnOnce(Env<'_>) -> Result<Term<'_>, Failure>) -> Result<Self, Failure> {
        let env = OwnedEnv::new();
        let value = env.run(|source| operation(source).map(|value| env.save(value)))?;
        Ok(Self { env, value })
    }
}
impl Encoder for BlobValue {
    fn encode<'a>(&self, env: Env<'a>) -> Term<'a> {
        self.env.run(|source| self.value.load(source).in_env(env))
    }
}

#[rustler::nif]
fn blob_columns(
    env: Env<'_>,
    table: ResourceArc<TableResource>,
    reference: Term<'_>,
) -> Result<Atom, Failure> {
    crate::request::read(env, reference, async move {
        table
            .table
            .blob_columns()
            .await
            .map_err(Failure::from_error)
    })
}

#[rustler::nif(schedule = "DirtyCpu")]
fn fetch_blobs(
    env: Env<'_>,
    table: ResourceArc<TableResource>,
    column: String,
    row_ids: Vec<u64>,
    reference: Term<'_>,
) -> Result<Atom, Failure> {
    crate::request::read(env, reference, async move {
        let values = table
            .table
            .fetch_blobs(&column, &row_ids)
            .await
            .map_err(Failure::from_error)?;
        BlobValue::new(|env| Ok(binaries(env, &values)?.encode(env)))
    })
}

#[rustler::nif(schedule = "DirtyCpu")]
fn fetch_blob_ranges(
    env: Env<'_>,
    table: ResourceArc<TableResource>,
    column: String,
    requests: Vec<(u64, u64, u64)>,
    reference: Term<'_>,
) -> Result<Atom, Failure> {
    crate::request::read(env, reference, async move {
        let requests: Vec<_> = requests
            .into_iter()
            .map(|(id, offset, length)| BlobRangeRequest::new(id, offset, length))
            .collect();
        let values = table
            .table
            .fetch_blob_ranges(&column, requests)
            .await
            .map_err(Failure::from_error)?;
        BlobValue::new(|env| Ok(binaries(env, &values)?.encode(env)))
    })
}

#[rustler::nif(schedule = "DirtyCpu")]
fn fetch_blob_files(
    env: Env<'_>,
    table: ResourceArc<TableResource>,
    column: String,
    row_ids: Vec<u64>,
    reference: Term<'_>,
) -> Result<Atom, Failure> {
    crate::request::read(env, reference, async move {
        let values = table
            .table
            .fetch_blob_files(&column, &row_ids)
            .await
            .map_err(Failure::from_error)?;
        Ok(values
            .into_iter()
            .map(|file| file.map(|file| ResourceArc::new(BlobResource { file })))
            .collect::<Vec<_>>())
    })
}

#[rustler::nif]
fn blob_read(
    env: Env<'_>,
    blob: ResourceArc<BlobResource>,
    length: Option<usize>,
    reference: Term<'_>,
) -> Result<Atom, Failure> {
    crate::request::read(env, reference, async move {
        let bytes = (async {
            match length {
                Some(length) => blob.file.read_up_to(length).await,
                None => blob.file.read().await,
            }
        })
        .await
        .map_err(Failure::from_error)?;
        BlobValue::new(|env| Ok(binary(env, &bytes)?.encode(env)))
    })
}

#[rustler::nif(schedule = "DirtyCpu")]
fn blob_read_ranges(
    env: Env<'_>,
    blob: ResourceArc<BlobResource>,
    ranges: Vec<(u64, u64)>,
    reference: Term<'_>,
) -> Result<Atom, Failure> {
    crate::request::read(env, reference, async move {
        let ranges = ranges
            .into_iter()
            .map(|(offset, length)| {
                offset
                    .checked_add(length)
                    .map(|end| offset..end)
                    .ok_or_else(|| Failure::from("Blob range overflows uint64"))
            })
            .collect::<Result<Vec<_>, _>>()?;
        let bytes = blob
            .file
            .read_ranges(&ranges)
            .await
            .map_err(Failure::from_error)?;
        BlobValue::new(|env| {
            let values = bytes
                .iter()
                .map(|bytes| binary(env, bytes))
                .collect::<Result<Vec<_>, _>>()?;
            Ok(values.encode(env))
        })
    })
}

#[rustler::nif]
fn blob_seek(
    env: Env<'_>,
    blob: ResourceArc<BlobResource>,
    offset: u64,
    reference: Term<'_>,
) -> Result<Atom, Failure> {
    crate::request::settle(env, reference, async move {
        blob.file.seek(offset).await.map_err(Failure::from_error)?;
        Ok(atoms::ok())
    })
}
#[rustler::nif]
fn blob_tell(
    env: Env<'_>,
    blob: ResourceArc<BlobResource>,
    reference: Term<'_>,
) -> Result<Atom, Failure> {
    crate::request::settle(env, reference, async move {
        blob.file.tell().await.map_err(Failure::from_error)
    })
}
#[rustler::nif]
fn blob_close(
    env: Env<'_>,
    blob: ResourceArc<BlobResource>,
    reference: Term<'_>,
) -> Result<Atom, Failure> {
    crate::request::settle(env, reference, async move {
        blob.file.close().await.map_err(Failure::from_error)?;
        Ok(atoms::ok())
    })
}
#[rustler::nif]
fn blob_is_closed(
    env: Env<'_>,
    blob: ResourceArc<BlobResource>,
    reference: Term<'_>,
) -> Result<Atom, Failure> {
    crate::reply::spawn(env, reference, async move { blob.file.is_closed().await });
    Ok(atoms::ok())
}
#[rustler::nif]
fn blob_size(blob: ResourceArc<BlobResource>) -> u64 {
    blob.file.size()
}

#[rustler::nif(schedule = "DirtyCpu")]
fn blob_info(blob: ResourceArc<BlobResource>) -> String {
    serde_json::json!({
        "size": blob.file.size(), "position": blob.file.position(),
        "data_path": blob.file.data_path().map(ToString::to_string),
        "kind": blob.file.kind().map(|kind| format!("{kind:?}").to_lowercase()),
        "uri": blob.file.uri(),
    })
    .to_string()
}
