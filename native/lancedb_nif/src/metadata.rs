use crate::error::Failure;
use crate::resources::{TableResource, RUNTIME};
use rustler::ResourceArc;
use std::collections::HashMap;

#[rustler::nif(schedule = "DirtyIo")]
fn schema_metadata(table: ResourceArc<TableResource>) -> Result<String, Failure> {
    RUNTIME.block_on(async {
        let _permit = crate::execution::Permit::read()?;
        let schema = table.table.schema().await.map_err(Failure::from_error)?;
        serde_json::to_string(schema.metadata()).map_err(Failure::from_error)
    })
}

#[rustler::nif(schedule = "DirtyCpu")]
fn update_schema_metadata(
    table: ResourceArc<TableResource>,
    changes: HashMap<String, Option<String>>,
    replace: bool,
) -> Result<crate::wire::Version, Failure> {
    update_schema_metadata_impl(table, changes, replace)
        .map(crate::wire::Version)
        .map_err(Failure::before_write)
}

fn update_schema_metadata_impl(
    table: ResourceArc<TableResource>,
    changes: HashMap<String, Option<String>>,
    replace: bool,
) -> Result<u64, Failure> {
    RUNTIME.block_on(async {
        let _permit = crate::execution::Permit::write()?;
        table.ensure_mutable()?;
        let wrapper = table
            .table
            .dataset()
            .ok_or("Schema metadata updates require an embedded table")?;
        let mut dataset = (*wrapper.get().await.map_err(Failure::from_error)?).clone();
        let update = dataset.update_schema_metadata(changes);
        let update = if replace { update.replace() } else { update };
        update.await.map_err(Failure::write_error)?;
        let version = dataset.version().version;
        wrapper.update(dataset);
        Ok(version)
    })
}

#[rustler::nif(schedule = "DirtyCpu")]
fn update_field_metadata(
    table: ResourceArc<TableResource>,
    path: String,
    metadata: HashMap<String, Option<String>>,
    replace: bool,
) -> Result<crate::wire::Version, Failure> {
    update_field_metadata_impl(table, path, metadata, replace)
        .map(crate::wire::Version)
        .map_err(Failure::before_write)
}

fn update_field_metadata_impl(
    table: ResourceArc<TableResource>,
    path: String,
    metadata: HashMap<String, Option<String>>,
    replace: bool,
) -> Result<u64, Failure> {
    RUNTIME.block_on(async {
        let _permit = crate::execution::Permit::write()?;
        table.ensure_mutable()?;
        let schema = table.table.schema().await.map_err(Failure::from_error)?;
        let update = crate::schema::storage::prepare_metadata(&schema, path, metadata, replace)?;
        table
            .table
            .update_field_metadata(&[update])
            .await
            .map(|r| r.version)
            .map_err(Failure::write_error)
    })
}
