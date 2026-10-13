//! Lance 13 JSON document tokenization can unwind on malformed documents.
//! Contain that unwind at SDK calls; ordinary errors remain unchanged.
use crate::error::Failure;
use futures::FutureExt;
use std::{
    future::Future,
    panic::{catch_unwind, AssertUnwindSafe},
};

pub(crate) fn tokenize<T>(operation: impl FnOnce() -> T) -> Result<T, Failure> {
    catch_unwind(AssertUnwindSafe(operation)).map_err(|_| {
        Failure::new(
            "invalid_data",
            "Tokenizer rejected input; JSON tokenization requires valid JSON input",
        )
    })
}
pub(crate) async fn index<T>(
    operation: impl Future<Output = Result<T, lancedb::Error>>,
) -> Result<T, Failure> {
    AssertUnwindSafe(operation)
        .catch_unwind()
        .await
        .map_err(|_| {
            Failure::new(
                "invalid_data",
                "Index builder rejected input; JSON indexes require valid JSON documents",
            )
        })?
        .map_err(Failure::from_error)
}
