//! FTS build controls are separate from reusable text analysis.
use crate::error::Failure;
use lance_index::scalar::inverted::InvertedListFormatVersion;
use lancedb::index::scalar::FtsIndexBuilder;

#[derive(Default, serde::Deserialize)]
pub(crate) struct FtsBuildOptions {
    block_size: Option<usize>,
    memory_limit_mb: Option<u64>,
    num_workers: Option<usize>,
    format_version: Option<String>,
}

impl FtsBuildOptions {
    pub(crate) fn apply(self, mut builder: FtsIndexBuilder) -> Result<FtsIndexBuilder, Failure> {
        if let Some(v) = self.block_size {
            builder = builder.block_size(v).map_err(Failure::from_error)?;
        }
        if let Some(v) = self.memory_limit_mb {
            if v == 0 {
                return Err("memory_limit_mb must be positive".into());
            }
            builder = builder.memory_limit_mb(v);
        }
        if let Some(v) = self.num_workers {
            if v == 0 {
                return Err("num_workers must be positive".into());
            }
            builder = builder.num_workers(v);
        }
        if let Some(v) = self.format_version {
            builder = builder.format_version(match v.as_str() {
                "v1" => InvertedListFormatVersion::V1,
                "v2" => InvertedListFormatVersion::V2,
                "v3" => InvertedListFormatVersion::V3,
                _ => return Err("Invalid FTS format_version".into()),
            });
        }
        builder
            .validate_format_version()
            .map_err(Failure::from_error)?;
        Ok(builder)
    }
}
