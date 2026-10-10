//! LanceDB 0.37.1 merge and tag metadata writes bypass the usual snapshot guard.
use crate::error::Failure;
pub(crate) fn ensure_mutable(table: &lancedb::Table) -> Result<(), Failure> {
    if let Some(dataset) = table.dataset() {
        dataset.ensure_mutable().map_err(Failure::from_error)?;
    }
    Ok(())
}
