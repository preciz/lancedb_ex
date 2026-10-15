use crate::error::Failure;
use crate::resources::{atoms, TableResource};
use rustler::{Atom, Env, ResourceArc, Term};
use std::collections::HashMap;

#[rustler::nif]
fn list_tags(
    env: Env<'_>,
    table_res: ResourceArc<TableResource>,
    reference: Term<'_>,
) -> Result<Atom, Failure> {
    crate::request::read(env, reference, async move {
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

#[rustler::nif]
fn tag_version(
    env: Env<'_>,
    table_res: ResourceArc<TableResource>,
    name: String,
    reference: Term<'_>,
) -> Result<Atom, Failure> {
    crate::request::read(env, reference, async move {
        let tags = table_res.table.tags().await.map_err(Failure::from_error)?;
        tags.get_version(&name).await.map_err(Failure::from_error)
    })
}

#[rustler::nif]
fn tag_action(
    env: Env<'_>,
    table_res: ResourceArc<TableResource>,
    action: String,
    name: String,
    version: Option<u64>,
    reference: Term<'_>,
) -> Result<Atom, Failure> {
    crate::request::write(env, reference, move |permit| async move {
        let _permit = permit;

        // Keep the package's checked-out handles read-only, including tag metadata.
        table_res.ensure_mutable()?;
        let mut tags = table_res.table.tags().await.map_err(Failure::from_error)?;
        match action.as_str() {
            "create" => {
                tags.create(&name, version.ok_or("Missing version")?)
                    .await
                    .map_err(Failure::write_error)?;
                Ok(atoms::created())
            }
            "update" => {
                tags.update(&name, version.ok_or("Missing version")?)
                    .await
                    .map_err(Failure::write_error)?;
                Ok(atoms::updated())
            }
            "delete" => {
                tags.delete(&name).await.map_err(Failure::write_error)?;
                Ok(atoms::deleted())
            }
            _ => Err("Invalid tag action".into()),
        }
    })
}

#[rustler::nif]
fn table_version(
    env: Env<'_>,
    table_res: ResourceArc<TableResource>,
    reference: Term<'_>,
) -> Result<Atom, Failure> {
    crate::request::read(env, reference, async move {
        table_res.table.version().await.map_err(Failure::from_error)
    })
}

#[rustler::nif]
fn list_versions(
    env: Env<'_>,
    table_res: ResourceArc<TableResource>,
    reference: Term<'_>,
) -> Result<Atom, Failure> {
    crate::request::read(env, reference, async move {
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
#[rustler::nif]
fn snapshot_table(
    env: Env<'_>,
    table: ResourceArc<TableResource>,
    version: Option<u64>,
    tag: Option<String>,
    reference: Term<'_>,
) -> Result<Atom, Failure> {
    crate::request::read(env, reference, async move {
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

#[rustler::nif]
fn restore_version(
    env: Env<'_>,
    table: ResourceArc<TableResource>,
    version: u64,
    reference: Term<'_>,
) -> Result<Atom, Failure> {
    crate::request::write(env, reference, move |permit| async move {
        let _permit = permit;
        restore_version_impl(table, version)
            .await
            .map(crate::wire::Version)
            .map_err(Failure::before_write)
    })
}

async fn restore_version_impl(
    table: ResourceArc<TableResource>,
    version: u64,
) -> Result<u64, Failure> {
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
}
