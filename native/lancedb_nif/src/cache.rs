use crate::error::Failure;
use crate::resources::TableResource;
use lance::{dataset::ReadParams, session::Session};
use rustler::{Atom, Env, ResourceArc, Term};
use std::sync::Arc;

pub(crate) fn session(
    index_size_bytes: Option<usize>,
    metadata_size_bytes: Option<usize>,
) -> Option<Arc<Session>> {
    if index_size_bytes.is_none() && metadata_size_bytes.is_none() {
        return None;
    }
    let defaults = ReadParams::default();
    Some(Arc::new(Session::new(
        index_size_bytes.unwrap_or(defaults.index_cache_size_bytes),
        metadata_size_bytes.unwrap_or(defaults.metadata_cache_size_bytes),
        Default::default(),
    )))
}

#[rustler::nif]
fn cache_stats(
    env: Env<'_>,
    table: ResourceArc<TableResource>,
    reference: Term<'_>,
) -> Result<Atom, Failure> {
    crate::request::read(env, reference, async move {
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
