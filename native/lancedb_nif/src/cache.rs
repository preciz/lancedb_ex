use crate::error::Failure;
use crate::resources::{TableResource, RUNTIME};
use rustler::ResourceArc;

#[rustler::nif(schedule = "DirtyIo")]
fn cache_stats(table: ResourceArc<TableResource>) -> Result<String, Failure> {
    RUNTIME.block_on(async {
        let _permit = crate::execution::Permit::read()?;
        let dataset = table.table.dataset().ok_or_else(|| {
            Failure::new("unsupported", "Cache statistics require an embedded table")
        })?;
        let dataset = dataset.get().await.map_err(Failure::from_error)?;
        let session = dataset.session();
        let mut caches = serde_json::Map::new();
        for (name, stats) in [
            ("index", session.index_cache_stats().await),
            ("metadata", session.metadata_cache_stats().await),
        ] {
            caches.insert(
                name.into(),
                serde_json::json!({
                    "size_bytes": stats.size_bytes,
                    "num_entries": stats.num_entries,
                    "hits": stats.hits,
                    "misses": stats.misses,
                    // The pinned SDK exposes usage but not configured capacity.
                    "capacity_bytes": null,
                }),
            );
        }
        caches.insert("scope".into(), "session".into());
        Ok(serde_json::Value::Object(caches).to_string())
    })
}
