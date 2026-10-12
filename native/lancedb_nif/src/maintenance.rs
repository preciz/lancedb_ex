use crate::error::Failure;
use crate::resources::{atoms, TableResource, RUNTIME};
use rustler::{Atom, ResourceArc};

#[rustler::nif(schedule = "DirtyIo")]
fn table_stats(table: ResourceArc<TableResource>) -> Result<String, Failure> {
    RUNTIME.block_on(async {
        let _permit = crate::execution::Permit::read()?;
        let stats = table.table.stats().await.map_err(Failure::from_error)?;
        let fragments = stats.fragment_stats;
        let lengths = fragments.lengths;
        Ok(serde_json::json!({
            "total_bytes": stats.total_bytes,
            "num_rows": stats.num_rows,
            "num_indices": stats.num_indices,
            "fragment_stats": {
                "num_fragments": fragments.num_fragments,
                "num_small_fragments": fragments.num_small_fragments,
                "lengths": {
                    "min": lengths.min, "max": lengths.max, "mean": lengths.mean,
                    "p25": lengths.p25, "p50": lengths.p50,
                    "p75": lengths.p75, "p99": lengths.p99,
                },
            },
        })
        .to_string())
    })
}

#[rustler::nif(schedule = "DirtyCpu")]
fn optimize_indices(
    table_res: ResourceArc<TableResource>,
    options: String,
) -> Result<Atom, Failure> {
    let options: crate::maintenance_options::IndexOptions =
        serde_json::from_str(&options).map_err(Failure::from_error)?;
    RUNTIME.block_on(async {
        let _permit = crate::execution::Permit::write()?;
        table_res
            .table
            .optimize(lancedb::table::OptimizeAction::Index(options.into_sdk()))
            .await
            .map_err(|e| {
                let message = format!("Failed to optimize indexes: {e}");
                Failure::from_error(e).context(message)
            })?;
        Ok(atoms::optimized())
    })
}

#[rustler::nif(schedule = "DirtyCpu")]
fn compact_files(
    table_res: ResourceArc<TableResource>,
    options: String,
) -> Result<String, Failure> {
    let options = crate::maintenance_options::compaction(&options)?;
    RUNTIME.block_on(async {
        let _permit = crate::execution::Permit::write()?;
        let stats = table_res
            .table
            .optimize(lancedb::table::OptimizeAction::Compact {
                options,
                remap_options: None,
            })
            .await
            .map_err(|e| {
                let message = format!("Failed to compact files: {e}");
                Failure::from_error(e).context(message)
            })?;
        let stats = stats.compaction.ok_or("Missing compaction statistics")?;
        Ok(serde_json::json!({
            "fragments_removed": stats.fragments_removed,
            "fragments_added": stats.fragments_added,
            "files_removed": stats.files_removed,
            "files_added": stats.files_added,
        })
        .to_string())
    })
}

#[rustler::nif(schedule = "DirtyCpu")]
fn cleanup_old_versions(
    table_res: ResourceArc<TableResource>,
    options: String,
) -> Result<String, Failure> {
    let options: crate::maintenance_options::PruneOptions =
        serde_json::from_str(&options).map_err(Failure::from_error)?;
    let older_than_seconds = options.older_than_seconds;
    if !(0..=31_536_000_000).contains(&older_than_seconds) {
        return Err("Invalid retention seconds".into());
    }
    let duration = lancedb::table::optimize::Duration::try_seconds(older_than_seconds)
        .ok_or("Invalid retention duration")?;
    RUNTIME.block_on(async {
        let _permit = crate::execution::Permit::write()?;
        let stats = table_res
            .table
            .optimize(lancedb::table::OptimizeAction::Prune {
                older_than: Some(duration),
                delete_unverified: Some(options.delete_unverified.unwrap_or(false)),
                error_if_tagged_old_versions: Some(
                    options.error_if_tagged_old_versions.unwrap_or(true),
                ),
            })
            .await
            .map_err(|e| {
                let message = format!("Failed to clean up old versions: {e}");
                Failure::from_error(e).context(message)
            })?;
        let stats = stats.prune.ok_or("Missing cleanup statistics")?;
        Ok(serde_json::json!({
            "bytes_removed": stats.bytes_removed,
            "old_versions": stats.old_versions,
            "data_files_removed": stats.data_files_removed,
            "transaction_files_removed": stats.transaction_files_removed,
            "index_files_removed": stats.index_files_removed,
            "deletion_files_removed": stats.deletion_files_removed,
        })
        .to_string())
    })
}

#[rustler::nif(schedule = "DirtyIo")]
fn uses_v2_manifest_paths(table: ResourceArc<TableResource>) -> Result<bool, Failure> {
    RUNTIME.block_on(async {
        let _permit = crate::execution::Permit::read()?;
        table
            .table
            .as_native()
            .ok_or("Manifest paths require an embedded table")?
            .uses_v2_manifest_paths()
            .await
            .map_err(Failure::from_error)
    })
}

#[rustler::nif(schedule = "DirtyIo")]
fn migrate_manifest_paths_v2(table: ResourceArc<TableResource>) -> Result<Atom, Failure> {
    RUNTIME
        .block_on(async {
            let _permit = crate::execution::Permit::write()?;
            table.ensure_mutable()?;
            table
                .table
                .as_native()
                .ok_or("Manifest paths require an embedded table")?
                .migrate_manifest_paths_v2()
                .await
                .map_err(Failure::write_error)?;
            Ok(atoms::migrated())
        })
        .map_err(Failure::before_write)
}
