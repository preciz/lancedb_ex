use crate::error::Failure;
use crate::resources::{atoms, TableResource, RUNTIME};
use lance::dataset::refs::Ref;
use rustler::{Atom, ResourceArc};
use std::sync::Arc;

#[derive(serde::Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum SourceRef {
    Current,
    Version { version: u64 },
    Tag { name: String },
    Branch { name: String, version: Option<u64> },
}

#[rustler::nif(schedule = "DirtyCpu")]
fn create_branch(
    table: ResourceArc<TableResource>,
    name: String,
    source: String,
) -> Result<ResourceArc<TableResource>, Failure> {
    let source: SourceRef = serde_json::from_str(&source).map_err(Failure::from_error)?;
    RUNTIME.block_on(async {
        let _permit = crate::execution::Permit::write()?;
        table.ensure_mutable()?;
        let source = match source {
            SourceRef::Current => Ref::Version(
                table.table.current_branch(),
                Some(table.table.version().await.map_err(Failure::from_error)?),
            ),
            SourceRef::Version { version } => Ref::VersionNumber(version),
            SourceRef::Tag { name } => Ref::Tag(name),
            SourceRef::Branch { name, version } => Ref::from((name.as_str(), version)),
        };
        let branch = table
            .table
            .create_branch(&name, source)
            .await
            .map_err(Failure::from_error)?;
        Ok(ResourceArc::new(TableResource {
            table: Arc::new(branch),
        }))
    })
}

#[rustler::nif(schedule = "DirtyCpu")]
fn checkout_branch(
    table: ResourceArc<TableResource>,
    name: String,
    version: Option<u64>,
) -> Result<ResourceArc<TableResource>, Failure> {
    RUNTIME.block_on(async {
        let _permit = crate::execution::Permit::read()?;
        let branch = table
            .table
            .checkout_branch(&name, version)
            .await
            .map_err(Failure::from_error)?;
        Ok(ResourceArc::new(TableResource {
            table: Arc::new(branch),
        }))
    })
}

#[rustler::nif(schedule = "DirtyCpu")]
fn list_branches(table: ResourceArc<TableResource>) -> Result<String, Failure> {
    RUNTIME.block_on(async {
        let _permit = crate::execution::Permit::read()?;
        let branches = table.table.list_branches().await.map_err(Failure::from_error)?;
        let branches: std::collections::HashMap<_, _> = branches.into_iter().map(|(name, b)| (name,
            serde_json::json!({"parent_branch": b.parent_branch, "parent_version": b.parent_version,
                "created_at": b.create_at, "manifest_size": b.manifest_size, "metadata": b.metadata})
        )).collect();
        Ok(serde_json::json!(branches).to_string())
    })
}

#[rustler::nif]
fn current_branch(table: ResourceArc<TableResource>) -> Result<String, Failure> {
    Ok(table
        .table
        .current_branch()
        .unwrap_or_else(|| "main".into()))
}

#[rustler::nif(schedule = "DirtyCpu")]
fn delete_branch(table: ResourceArc<TableResource>, name: String) -> Result<Atom, Failure> {
    RUNTIME.block_on(async {
        let _permit = crate::execution::Permit::write()?;
        table.ensure_mutable()?;
        table
            .table
            .delete_branch(&name)
            .await
            .map_err(Failure::from_error)?;
        Ok(atoms::deleted())
    })
}

#[rustler::nif(schedule = "DirtyCpu")]
fn table_uri(table: ResourceArc<TableResource>) -> Result<String, Failure> {
    RUNTIME.block_on(async {
        let _permit = crate::execution::Permit::read()?;
        table.table.uri().await.map_err(Failure::from_error)
    })
}
