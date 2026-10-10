//! Extend SDK defaults only with explicitly supplied maintenance options.
use crate::error::Failure;
use lance_index::optimize::OptimizeOptions;
use lancedb::table::CompactionOptions;

#[derive(Default, serde::Deserialize)]
#[serde(default, deny_unknown_fields)]
pub(crate) struct IndexOptions {
    index_names: Option<Vec<String>>,
    num_indices_to_merge: Option<usize>,
    retrain: bool,
}
impl IndexOptions {
    pub(crate) fn into_sdk(self) -> OptimizeOptions {
        let mut opts = OptimizeOptions::default();
        opts.index_names = self.index_names;
        opts.num_indices_to_merge = self.num_indices_to_merge;
        opts.retrain = self.retrain;
        opts
    }
}

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct PruneOptions {
    pub(crate) older_than_seconds: i64,
    pub(crate) delete_unverified: Option<bool>,
    pub(crate) error_if_tagged_old_versions: Option<bool>,
}

/// Package-owned request; SDK field serialization is not part of our protocol.
#[derive(Default, serde::Deserialize)]
#[serde(default, deny_unknown_fields)]
pub(crate) struct CompactionRequest {
    target_rows_per_fragment: Option<usize>,
    max_rows_per_group: Option<usize>,
    materialize_deletions: Option<bool>,
    materialize_deletions_threshold: Option<f32>,
    defer_index_remap: Option<bool>,
    max_bytes_per_file: Option<usize>,
    num_threads: Option<usize>,
    batch_size: Option<usize>,
    io_buffer_size: Option<u64>,
    binary_copy_read_batch_bytes: Option<usize>,
    max_source_fragments: Option<usize>,
    compaction_mode: Option<String>,
    index_remap_mode: Option<String>,
    #[serde(default, deserialize_with = "nullable_count")]
    max_overlays_per_fragment: Option<Option<usize>>,
}
fn nullable_count<'de, D: serde::Deserializer<'de>>(
    de: D,
) -> Result<Option<Option<usize>>, D::Error> {
    <Option<usize> as serde::Deserialize>::deserialize(de).map(Some)
}
impl CompactionRequest {
    fn into_sdk(self) -> Result<CompactionOptions, Failure> {
        let mut options = CompactionOptions::default();
        if let Some(value) = self.target_rows_per_fragment {
            options.target_rows_per_fragment = value;
        }
        if let Some(value) = self.max_rows_per_group {
            options.max_rows_per_group = value;
        }
        if let Some(value) = self.materialize_deletions {
            options.materialize_deletions = value;
        }
        if let Some(value) = self.materialize_deletions_threshold {
            options.materialize_deletions_threshold = value;
        }
        if let Some(value) = self.defer_index_remap {
            options.defer_index_remap = value;
        }
        if let Some(value) = self.max_bytes_per_file {
            options.max_bytes_per_file = Some(value);
        }
        if let Some(value) = self.num_threads {
            options.num_threads = Some(value);
        }
        if let Some(value) = self.batch_size {
            options.batch_size = Some(value);
        }
        if let Some(value) = self.io_buffer_size {
            options.io_buffer_size = Some(value);
        }
        if let Some(value) = self.binary_copy_read_batch_bytes {
            options.binary_copy_read_batch_bytes = Some(value);
        }
        if let Some(value) = self.max_source_fragments {
            options.max_source_fragments = Some(value);
        }
        if let Some(value) = self.max_overlays_per_fragment {
            options.max_overlays_per_fragment = value;
        }
        if let Some(value) = self.compaction_mode {
            options.compaction_mode = Some(
                lance::dataset::optimize::CompactionMode::try_from(value.as_str())
                    .map_err(Failure::from_error)?,
            );
        }
        if let Some(value) = self.index_remap_mode {
            options.index_remap_mode =
                lance::dataset::optimize::IndexRemapMode::try_from(value.as_str())
                    .map_err(Failure::from_error)?;
        }
        Ok(options)
    }
}
pub(crate) fn compaction(json: &str) -> Result<CompactionOptions, Failure> {
    serde_json::from_str::<CompactionRequest>(json)
        .map_err(Failure::from_error)?
        .into_sdk()
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn compaction_retains_sdk_defaults_and_applies_explicit_modes() {
        let defaults = serde_json::to_value(CompactionOptions::default()).unwrap();
        assert_eq!(
            serde_json::to_value(compaction("{}").unwrap()).unwrap(),
            defaults
        );
        let options = compaction(r#"{"num_threads":2,"compaction_mode":"try_binary_copy","index_remap_mode":"compact","max_overlays_per_fragment":0}"#).unwrap();
        assert_eq!(options.num_threads, Some(2));
        assert_eq!(options.max_overlays_per_fragment, Some(0));
        assert!(matches!(
            options.compaction_mode(),
            lance::dataset::optimize::CompactionMode::TryBinaryCopy
        ));
        assert_eq!(
            compaction(r#"{"max_overlays_per_fragment":null}"#)
                .unwrap()
                .max_overlays_per_fragment,
            None
        );
        assert!(compaction(r#"{"unknown":1}"#).is_err());
        assert!(compaction(r#"{"compaction_mode":"unknown"}"#).is_err());
    }
}
