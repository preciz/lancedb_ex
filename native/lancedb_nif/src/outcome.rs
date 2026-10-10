//! Engine outcomes, independent of the NIF representation.
use std::sync::Arc;

#[derive(Clone)]
pub(crate) enum WriteOutput {
    Add(crate::ingestion::WriteResult),
    Created(Arc<lancedb::Table>),
    Merge(crate::write::ConditionalMergeResult),
    Columns(u64),
}
