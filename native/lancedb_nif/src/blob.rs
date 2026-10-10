//! Blob resources share the SDK's synchronized cursor and release storage references on drop.
pub(crate) mod schema;
pub(crate) mod write_options;
use crate::{
    error::Failure,
    execution::Permit,
    resources::{atoms, TableResource, RUNTIME},
};
use arrow_array::Array;
use lancedb::blob::{BlobFile, BlobRangeRequest};
use rustler::{Atom, Binary, Env, OwnedBinary, ResourceArc};
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

#[rustler::nif(schedule = "DirtyIo")]
fn blob_columns(table: ResourceArc<TableResource>) -> Result<Vec<String>, Failure> {
    let _permit = Permit::read()?;
    RUNTIME
        .block_on(table.table.blob_columns())
        .map_err(Failure::from_error)
}

#[rustler::nif(schedule = "DirtyIo")]
fn fetch_blobs<'a>(
    env: Env<'a>,
    table: ResourceArc<TableResource>,
    column: String,
    row_ids: Vec<u64>,
) -> Result<Vec<Option<Binary<'a>>>, Failure> {
    let _permit = Permit::read()?;
    let values = RUNTIME
        .block_on(table.table.fetch_blobs(&column, &row_ids))
        .map_err(Failure::from_error)?;
    binaries(env, &values)
}

#[rustler::nif(schedule = "DirtyIo")]
fn fetch_blob_ranges<'a>(
    env: Env<'a>,
    table: ResourceArc<TableResource>,
    column: String,
    requests: Vec<(u64, u64, u64)>,
) -> Result<Vec<Option<Binary<'a>>>, Failure> {
    let _permit = Permit::read()?;
    let requests: Vec<_> = requests
        .into_iter()
        .map(|(id, offset, length)| BlobRangeRequest::new(id, offset, length))
        .collect();
    let values = RUNTIME
        .block_on(table.table.fetch_blob_ranges(&column, requests))
        .map_err(Failure::from_error)?;
    binaries(env, &values)
}

#[rustler::nif(schedule = "DirtyIo")]
fn fetch_blob_files(
    table: ResourceArc<TableResource>,
    column: String,
    row_ids: Vec<u64>,
) -> Result<Vec<Option<ResourceArc<BlobResource>>>, Failure> {
    let _permit = Permit::read()?;
    let values = RUNTIME
        .block_on(table.table.fetch_blob_files(&column, &row_ids))
        .map_err(Failure::from_error)?;
    Ok(values
        .into_iter()
        .map(|file| file.map(|file| ResourceArc::new(BlobResource { file })))
        .collect())
}

#[rustler::nif(schedule = "DirtyIo")]
fn blob_read<'a>(
    env: Env<'a>,
    blob: ResourceArc<BlobResource>,
    length: Option<usize>,
) -> Result<Binary<'a>, Failure> {
    let _permit = Permit::read()?;
    let bytes = RUNTIME
        .block_on(async {
            match length {
                Some(length) => blob.file.read_up_to(length).await,
                None => blob.file.read().await,
            }
        })
        .map_err(Failure::from_error)?;
    binary(env, &bytes)
}

#[rustler::nif(schedule = "DirtyIo")]
fn blob_read_ranges<'a>(
    env: Env<'a>,
    blob: ResourceArc<BlobResource>,
    ranges: Vec<(u64, u64)>,
) -> Result<Vec<Binary<'a>>, Failure> {
    let _permit = Permit::read()?;
    let ranges = ranges
        .into_iter()
        .map(|(offset, length)| {
            offset
                .checked_add(length)
                .map(|end| offset..end)
                .ok_or_else(|| Failure::from("Blob range overflows uint64"))
        })
        .collect::<Result<Vec<_>, _>>()?;
    let bytes = RUNTIME
        .block_on(blob.file.read_ranges(&ranges))
        .map_err(Failure::from_error)?;
    bytes.iter().map(|bytes| binary(env, bytes)).collect()
}

#[rustler::nif(schedule = "DirtyIo")]
fn blob_seek(blob: ResourceArc<BlobResource>, offset: u64) -> Result<Atom, Failure> {
    RUNTIME
        .block_on(blob.file.seek(offset))
        .map_err(Failure::from_error)?;
    Ok(atoms::ok())
}
#[rustler::nif(schedule = "DirtyIo")]
fn blob_tell(blob: ResourceArc<BlobResource>) -> Result<u64, Failure> {
    RUNTIME
        .block_on(blob.file.tell())
        .map_err(Failure::from_error)
}
#[rustler::nif(schedule = "DirtyIo")]
fn blob_close(blob: ResourceArc<BlobResource>) -> Result<Atom, Failure> {
    RUNTIME
        .block_on(blob.file.close())
        .map_err(Failure::from_error)?;
    Ok(atoms::ok())
}
#[rustler::nif(schedule = "DirtyIo")]
fn blob_is_closed(blob: ResourceArc<BlobResource>) -> bool {
    RUNTIME.block_on(blob.file.is_closed())
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
