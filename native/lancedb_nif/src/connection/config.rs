//! Saved connection settings used by initial connections and dedicated sessions.
use crate::error::Failure;
use lance::session::Session;
use std::collections::HashMap;
use std::sync::Arc;

pub(crate) struct ConnectionConfig {
    uri: String,
    storage_options: HashMap<String, String>,
    read_consistency_interval_ms: Option<u32>,
}

impl ConnectionConfig {
    pub(crate) fn new(
        uri: String,
        storage_options: HashMap<String, String>,
        read_consistency_interval_ms: Option<u32>,
    ) -> Self {
        Self {
            uri,
            storage_options,
            read_consistency_interval_ms,
        }
    }

    // Retain the original URI, including query parameters consumed by the SDK.
    pub(crate) fn uri(&self) -> &str {
        &self.uri
    }

    pub(crate) async fn connect(
        &self,
        session: Option<Arc<Session>>,
    ) -> Result<Arc<lancedb::Connection>, Failure> {
        let mut builder = lancedb::connect(&self.uri).storage_options(self.storage_options.clone());
        if let Some(ms) = self.read_consistency_interval_ms {
            builder =
                builder.read_consistency_interval(std::time::Duration::from_millis(ms.into()));
        }
        if let Some(session) = session {
            builder = builder.session(session);
        }
        builder
            .execute()
            .await
            .map(Arc::new)
            .map_err(Failure::from_error)
    }

    pub(crate) fn storage_options(
        &self,
        overrides: Option<HashMap<String, String>>,
    ) -> Option<HashMap<String, String>> {
        // None preserves SDK store-registry reuse; an explicitly empty map
        // remains explicit. Overrides never mutate the saved configuration.
        if overrides.is_none() && self.storage_options.is_empty() {
            return None;
        }
        let mut options = self.storage_options.clone();
        options.extend(overrides.unwrap_or_default());
        Some(options)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn storage_overrides_preserve_inheritance_and_absent_versus_empty_options() {
        let empty = ConnectionConfig::new("/tmp/db".into(), HashMap::new(), None);
        assert!(empty.storage_options(None).is_none());
        assert_eq!(
            empty.storage_options(Some(HashMap::new())),
            Some(HashMap::new())
        );
        let defaults = HashMap::from([
            ("aws_region".into(), "us-east-1".into()),
            ("aws_endpoint".into(), "http://localhost:9000".into()),
        ]);
        let config =
            ConnectionConfig::new("s3://bucket/db?key=value".into(), defaults.clone(), Some(0));
        let overrides = HashMap::from([("aws_region".into(), "eu-central-1".into())]);
        let merged = config.storage_options(Some(overrides)).unwrap();
        assert_eq!(merged["aws_region"], "eu-central-1");
        assert_eq!(merged["aws_endpoint"], "http://localhost:9000");
        assert_eq!(config.storage_options(None), Some(defaults.clone()));
        assert_eq!(config.storage_options(Some(HashMap::new())), Some(defaults));
        assert_eq!(config.uri(), "s3://bucket/db?key=value");
    }
}
