//! Arrow 58 schema conversion can unwind on malformed IPC metadata.
//! Contain parsing, including reader iteration, before publishing batch resources.
use crate::error::Failure;
use arrow::ipc::reader::StreamReader;
use arrow_array::RecordBatch;
use std::panic::catch_unwind;

pub(crate) fn read_stream(bytes: &[u8]) -> Result<Vec<RecordBatch>, Failure> {
    catch_unwind(|| {
        let reader = StreamReader::try_new(bytes, None).map_err(Failure::from_error)?;
        reader
            .map(|batch| batch.map_err(Failure::from_error))
            .collect()
    })
    .map_err(|_| {
        Failure::new(
            "invalid_data",
            "Invalid Arrow IPC stream: parser rejected malformed data",
        )
    })?
}
