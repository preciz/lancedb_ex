//! Isolate per-open sessions when cache sizes are explicitly configured.
use crate::error::Failure;
use crate::resources::{ConnectionResource, TableResource, RUNTIME};
use lance::{dataset::ReadParams, session::Session};
use rustler::ResourceArc;
use std::collections::HashMap;
use std::sync::Arc;

#[derive(Default, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct OpenOptions {
    branch: Option<String>,
    version: Option<u64>,
    location: Option<String>,
    storage_options: Option<HashMap<String, String>>,
    index_cache_size_bytes: Option<usize>,
    metadata_cache_size_bytes: Option<usize>,
}

impl OpenOptions {
    pub(crate) fn at(location: String) -> Self {
        Self {
            location: Some(location),
            ..Default::default()
        }
    }

    async fn connection(
        &self,
        resource: &ConnectionResource,
    ) -> Result<Arc<lancedb::Connection>, Failure> {
        if self.index_cache_size_bytes.is_none() && self.metadata_cache_size_bytes.is_none() {
            return Ok(resource.conn.clone());
        }
        // ListingDatabase overwrites ReadParams.session with the connection's
        // session, so builder cache-size setters alone have no effect. Supply
        // a dedicated connection session and preserve its storage/consistency settings.
        let defaults = ReadParams::default();
        let session = Arc::new(Session::new(
            self.index_cache_size_bytes
                .unwrap_or(defaults.index_cache_size_bytes),
            self.metadata_cache_size_bytes
                .unwrap_or(defaults.metadata_cache_size_bytes),
            Default::default(),
        ));
        resource.config.connect(Some(session)).await
    }

    pub(crate) async fn open(
        self,
        resource: &ConnectionResource,
        name: &str,
    ) -> Result<lancedb::Table, Failure> {
        lancedb::utils::validate_table_name(name).map_err(Failure::from_error)?;
        let conn = self.connection(resource).await?;
        let mut builder = conn.open_table(name);
        if let Some(location) = self.location {
            builder = builder.location(location);
        }
        // Local SDK connections discard storage options. Merge the saved
        // configuration explicitly so location overrides can open remote tables.
        if let Some(options) = resource.config.storage_options(self.storage_options) {
            builder = builder.storage_options(options);
        }
        if let Some(branch) = self.branch {
            builder = builder.branch(branch);
        }
        if let Some(version) = self.version {
            builder = builder.version(version);
        }
        builder.execute().await.map_err(|e| {
            let message = format!("Failed to open table: {e}");
            Failure::from_error(e).context(message)
        })
    }
}

#[rustler::nif(schedule = "DirtyCpu")]
fn open_table(
    conn: ResourceArc<ConnectionResource>,
    name: String,
    options: String,
) -> Result<ResourceArc<TableResource>, Failure> {
    let options: OpenOptions = serde_json::from_str(&options).map_err(Failure::from_error)?;
    RUNTIME.block_on(async {
        let _permit = crate::execution::Permit::read()?;
        let table = options.open(&conn, &name).await?;
        Ok(ResourceArc::new(TableResource {
            table: Arc::new(table),
        }))
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Int64Array, RecordBatch};

    #[test]
    fn explicit_cache_limits_use_an_isolated_session_and_zero_disables_caching() {
        let path = std::env::temp_dir().join(format!(
            "lancedb_open_cache_{}_{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        struct Cleanup(std::path::PathBuf);
        impl Drop for Cleanup {
            fn drop(&mut self) {
                let _ = std::fs::remove_dir_all(&self.0);
            }
        }
        let _cleanup = Cleanup(path.clone());
        RUNTIME.block_on(async {
            let config = crate::connection::ConnectionConfig::new(
                path.to_str().unwrap().into(),
                HashMap::new(),
                Some(0),
            );
            let conn = config.connect(None).await.unwrap();
            let batch = RecordBatch::try_from_iter([(
                "id",
                Arc::new(Int64Array::from(vec![1, 2, 3])) as _,
            )])
            .unwrap();
            let table = conn.create_table("docs", batch).execute().await.unwrap();
            table
                .create_index(&["id"], lancedb::index::Index::BTree(Default::default()))
                .name("ids".into())
                .execute()
                .await
                .unwrap();
            table.prewarm_index("ids").await.unwrap();
            let original_session = table.dataset().unwrap().get().await.unwrap().session();
            assert!(original_session.index_cache_stats().await.num_entries > 0);
            let resource = ConnectionResource { conn, config };
            assert!(Arc::ptr_eq(
                &OpenOptions::default().connection(&resource).await.unwrap(),
                &resource.conn
            ));
            let options = OpenOptions {
                index_cache_size_bytes: Some(0),
                metadata_cache_size_bytes: Some(0),
                ..Default::default()
            };
            let opened = options.open(&resource, "docs").await.unwrap();
            opened.prewarm_index("ids").await.unwrap();
            let session = opened.dataset().unwrap().get().await.unwrap().session();
            assert!(!Arc::ptr_eq(&session, &original_session));
            assert_eq!(session.index_cache_stats().await.num_entries, 0);
            assert_eq!(session.metadata_cache_stats().await.num_entries, 0);
            assert_eq!(opened.count_rows(None).await.unwrap(), 3);
        });
    }
}
