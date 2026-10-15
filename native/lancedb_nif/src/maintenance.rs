use crate::error::Failure;
use crate::resources::{atoms, TableResource};
use rustler::{Atom, Env, ResourceArc, Term};

#[rustler::nif(schedule = "DirtyCpu")]
fn optimize(
    env: Env<'_>,
    table: ResourceArc<TableResource>,
    reference: Term<'_>,
) -> Result<Atom, Failure> {
    crate::request::write(env, reference, move |permit| async move {
        let _permit = permit;
        table.ensure_mutable()?;
        let stats = table
            .table
            .optimize(lancedb::table::OptimizeAction::All)
            .await
            .map_err(|error| {
                let message = format!("Failed to optimize table: {error}");
                Failure::write_error(error).context(message)
            })?;
        let compaction = stats.compaction.ok_or_else(|| {
            Failure::new("execution", "Missing compaction statistics").executing()
        })?;
        let cleanup = stats
            .prune
            .ok_or_else(|| Failure::new("execution", "Missing cleanup statistics").executing())?;
        Ok(serde_json::json!({
            "compaction": compaction_stats(compaction),
            "cleanup": cleanup_stats(cleanup),
        })
        .to_string())
    })
}

#[rustler::nif(schedule = "DirtyCpu")]
fn table_stats(
    env: Env<'_>,
    table: ResourceArc<TableResource>,
    reference: Term<'_>,
) -> Result<Atom, Failure> {
    crate::request::read(env, reference, async move {
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
    env: Env<'_>,
    table_res: ResourceArc<TableResource>,
    options: String,
    reference: Term<'_>,
) -> Result<Atom, Failure> {
    crate::request::write(env, reference, move |permit| async move {
        let _permit = permit;
        table_res.ensure_mutable()?;
        let options: crate::maintenance_options::IndexOptions =
            serde_json::from_str(&options).map_err(Failure::from_error)?;

        table_res
            .table
            .optimize(lancedb::table::OptimizeAction::Index(options.into_sdk()))
            .await
            .map_err(|e| {
                let message = format!("Failed to optimize indexes: {e}");
                Failure::write_error(e).context(message)
            })?;
        Ok(atoms::optimized())
    })
}

#[rustler::nif(schedule = "DirtyCpu")]
fn compact_files(
    env: Env<'_>,
    table_res: ResourceArc<TableResource>,
    options: String,
    reference: Term<'_>,
) -> Result<Atom, Failure> {
    crate::request::write(env, reference, move |permit| async move {
        let _permit = permit;
        table_res.ensure_mutable()?;
        let options = crate::maintenance_options::compaction(&options)?;

        let stats = table_res
            .table
            .optimize(lancedb::table::OptimizeAction::Compact {
                options,
                remap_options: None,
            })
            .await
            .map_err(|e| {
                let message = format!("Failed to compact files: {e}");
                Failure::write_error(e).context(message)
            })?;
        let stats = stats.compaction.ok_or_else(|| {
            Failure::new("execution", "Missing compaction statistics").executing()
        })?;
        Ok(compaction_stats(stats).to_string())
    })
}

#[rustler::nif(schedule = "DirtyCpu")]
fn cleanup_old_versions(
    env: Env<'_>,
    table_res: ResourceArc<TableResource>,
    options: String,
    reference: Term<'_>,
) -> Result<Atom, Failure> {
    crate::request::write(env, reference, move |permit| async move {
        let _permit = permit;
        table_res.ensure_mutable()?;
        let options: crate::maintenance_options::PruneOptions =
            serde_json::from_str(&options).map_err(Failure::from_error)?;
        let older_than_seconds = options.older_than_seconds;
        if !(0..=31_536_000_000).contains(&older_than_seconds) {
            return Err("Invalid retention seconds".into());
        }
        let duration = lancedb::table::optimize::Duration::try_seconds(older_than_seconds)
            .ok_or("Invalid retention duration")?;

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
                Failure::write_error(e).context(message)
            })?;
        let stats = stats
            .prune
            .ok_or_else(|| Failure::new("execution", "Missing cleanup statistics").executing())?;
        Ok(cleanup_stats(stats).to_string())
    })
}

fn compaction_stats(stats: lance::dataset::optimize::CompactionMetrics) -> serde_json::Value {
    serde_json::json!({
        "fragments_removed": stats.fragments_removed,
        "fragments_added": stats.fragments_added,
        "files_removed": stats.files_removed,
        "files_added": stats.files_added,
    })
}

fn cleanup_stats(stats: lance::dataset::cleanup::RemovalStats) -> serde_json::Value {
    serde_json::json!({
        "bytes_removed": stats.bytes_removed,
        "old_versions": stats.old_versions,
        "data_files_removed": stats.data_files_removed,
        "transaction_files_removed": stats.transaction_files_removed,
        "index_files_removed": stats.index_files_removed,
        "deletion_files_removed": stats.deletion_files_removed,
    })
}

#[rustler::nif(schedule = "DirtyCpu")]
fn uses_v2_manifest_paths(
    env: Env<'_>,
    table: ResourceArc<TableResource>,
    reference: Term<'_>,
) -> Result<Atom, Failure> {
    crate::request::read(env, reference, async move {
        table
            .table
            .as_native()
            .ok_or("Manifest paths require an embedded table")?
            .uses_v2_manifest_paths()
            .await
            .map_err(Failure::from_error)
    })
}

#[rustler::nif(schedule = "DirtyCpu")]
fn migrate_manifest_paths_v2(
    env: Env<'_>,
    table: ResourceArc<TableResource>,
    reference: Term<'_>,
) -> Result<Atom, Failure> {
    crate::request::write(env, reference, move |permit| async move {
        let _permit = permit;
        (async {
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
        .await
        .map_err(Failure::before_write)
    })
}
