use crate::error::Failure;
use crate::resources::{atoms, TableResource, RUNTIME};
use rustler::{Atom, ResourceArc};
use std::collections::HashMap;

#[rustler::nif(schedule = "DirtyCpu")]
fn list_tags(table_res: ResourceArc<TableResource>) -> Result<String, Failure> {
    RUNTIME.block_on(async {
        let _permit = crate::execution::Permit::read()?;
        let tags = table_res.table.tags().await.map_err(Failure::from_error)?;
        let entries = tags.list().await.map_err(Failure::from_error)?;
        let entries: HashMap<_, _> = entries.into_iter().map(|(name, tag)| (
            name, serde_json::json!({
                "version": tag.version, "manifest_size": tag.manifest_size, "branch": tag.branch
            })
        )).collect();
        Ok(serde_json::json!(entries).to_string())
    })
}

#[rustler::nif(schedule = "DirtyCpu")]
fn tag_version(table_res: ResourceArc<TableResource>, name: String) -> Result<u64, Failure> {
    RUNTIME.block_on(async {
        let _permit = crate::execution::Permit::read()?;
        let tags = table_res.table.tags().await.map_err(Failure::from_error)?;
        tags.get_version(&name).await.map_err(Failure::from_error)
    })
}

#[rustler::nif(schedule = "DirtyCpu")]
fn tag_action(
    table_res: ResourceArc<TableResource>,
    action: String,
    name: String,
    version: Option<u64>,
) -> Result<Atom, Failure> {
    RUNTIME.block_on(async {
        let _permit = crate::execution::Permit::write()?;
        // Keep the package's checked-out handles read-only, including tag metadata.
        table_res.ensure_mutable()?;
        let mut tags = table_res.table.tags().await.map_err(Failure::from_error)?;
        match action.as_str() {
            "create" => {
                tags.create(&name, version.ok_or("Missing version")?)
                    .await
                    .map_err(Failure::from_error)?;
                Ok(atoms::created())
            }
            "update" => {
                tags.update(&name, version.ok_or("Missing version")?)
                    .await
                    .map_err(Failure::from_error)?;
                Ok(atoms::updated())
            }
            "delete" => {
                tags.delete(&name).await.map_err(Failure::from_error)?;
                Ok(atoms::deleted())
            }
            _ => Err("Invalid tag action".into()),
        }
    })
}

#[rustler::nif(schedule = "DirtyCpu")]
fn table_version(table_res: ResourceArc<TableResource>) -> Result<u64, Failure> {
    RUNTIME.block_on(async {
        let _permit = crate::execution::Permit::read()?;
        table_res.table.version().await.map_err(Failure::from_error)
    })
}

#[rustler::nif(schedule = "DirtyCpu")]
fn list_versions(table_res: ResourceArc<TableResource>) -> Result<String, Failure> {
    RUNTIME.block_on(async {
        let _permit = crate::execution::Permit::read()?;
        let versions = table_res
            .table
            .list_versions()
            .await
            .map_err(Failure::from_error)?;
        let versions: Vec<_> = versions.into_iter().map(|v| serde_json::json!({
            "version": v.version, "timestamp": v.timestamp.to_rfc3339(), "metadata": v.metadata,
        })).collect();
        Ok(serde_json::json!(versions).to_string())
    })
}

// SDK branch checkout creates an independent DatasetConsistencyWrapper, including
// for main. Cloning Table/Arc alone would still share the mutable view.
#[rustler::nif(schedule = "DirtyCpu")]
fn snapshot_table(
    table: ResourceArc<TableResource>,
    version: Option<u64>,
    tag: Option<String>,
) -> Result<ResourceArc<TableResource>, Failure> {
    RUNTIME.block_on(async {
        let _permit = crate::execution::Permit::read()?;
        let branch = table
            .table
            .current_branch()
            .unwrap_or_else(|| "main".into());
        let snapshot = table
            .table
            .checkout_branch(&branch, version)
            .await
            .map_err(Failure::from_error)?;
        if let Some(tag) = tag {
            snapshot
                .checkout_tag(&tag)
                .await
                .map_err(Failure::from_error)?;
        }
        Ok(ResourceArc::new(TableResource {
            table: std::sync::Arc::new(snapshot),
        }))
    })
}

#[rustler::nif(schedule = "DirtyCpu")]
fn restore_version(
    table: ResourceArc<TableResource>,
    version: u64,
) -> Result<crate::wire::Version, Failure> {
    restore_version_impl(table, version)
        .map(crate::wire::Version)
        .map_err(Failure::before_write)
}

fn restore_version_impl(table: ResourceArc<TableResource>, version: u64) -> Result<u64, Failure> {
    RUNTIME.block_on(async {
        let _permit = crate::execution::Permit::write()?;
        table.ensure_mutable()?;
        let branch = table
            .table
            .current_branch()
            .unwrap_or_else(|| "main".into());
        let snapshot = table
            .table
            .checkout_branch(&branch, Some(version))
            .await
            .map_err(Failure::from_error)?;
        snapshot.restore().await.map_err(Failure::write_error)?;
        let version = snapshot.version().await.map_err(Failure::write_error)?;
        table
            .table
            .checkout_latest()
            .await
            .map_err(Failure::write_error)?;
        Ok(version)
    })
}
