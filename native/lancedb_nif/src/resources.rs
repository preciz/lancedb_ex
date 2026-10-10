use crate::error::Failure;
use lazy_static::lazy_static;
use std::panic::{RefUnwindSafe, UnwindSafe};
use std::sync::Arc;
use tokio::runtime::Runtime;

lazy_static! {
    pub(crate) static ref RUNTIME: Runtime = Runtime::new().expect("Failed to start Tokio runtime");
}

pub(crate) mod atoms {
    rustler::atoms! {
        ok,
        deleted,
        dropped,
        created,
        optimized,
        migrated,
        updated,
        prewarmed,
    }
}

pub struct ConnectionResource {
    pub conn: Arc<lancedb::Connection>,
    pub(crate) config: crate::connection::ConnectionConfig,
}

impl RefUnwindSafe for ConnectionResource {}
impl UnwindSafe for ConnectionResource {}

#[rustler::resource_impl]
impl rustler::Resource for ConnectionResource {}

pub struct TableResource {
    pub table: Arc<lancedb::Table>,
}

impl RefUnwindSafe for TableResource {}
impl UnwindSafe for TableResource {}

#[rustler::resource_impl]
impl rustler::Resource for TableResource {}

impl TableResource {
    /// LanceDB 0.37.1's merge and tag metadata paths bypass the usual write guard.
    pub(crate) fn ensure_mutable(&self) -> Result<(), Failure> {
        crate::compat::table::ensure_mutable(&self.table)
    }
}
