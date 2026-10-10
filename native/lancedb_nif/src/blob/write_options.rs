use crate::error::Failure;
use lance::dataset::{ExternalBlobMode, WriteParams};

#[derive(Default, serde::Deserialize)]
#[serde(default, deny_unknown_fields)]
pub(crate) struct WriteOptions {
    external_blob_mode: Option<String>,
    allow_external_blob_outside_bases: bool,
    blob_pack_file_size_threshold: Option<usize>,
}
impl WriteOptions {
    pub(crate) fn apply(self, params: &mut WriteParams) -> Result<(), Failure> {
        params.external_blob_mode =
            ExternalBlobMode::try_from(self.external_blob_mode.as_deref().unwrap_or("reference"))
                .map_err(Failure::from_error)?;
        if self.allow_external_blob_outside_bases
            && params.external_blob_mode == ExternalBlobMode::Ingest
        {
            return Err("allow_external_blob_outside_bases only applies to reference mode".into());
        }
        if self
            .blob_pack_file_size_threshold
            .is_some_and(|n| n == 0 || n > u32::MAX as usize)
        {
            return Err("blob_pack_file_size_threshold must be a positive uint32 integer".into());
        }
        params.allow_external_blob_outside_bases = self.allow_external_blob_outside_bases;
        params.blob_pack_file_size_threshold = self.blob_pack_file_size_threshold;
        Ok(())
    }
}
