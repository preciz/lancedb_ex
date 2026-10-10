//! Creation controls shared by inferred rows and streamed inputs.
use crate::error::Failure;
use lance::dataset::WriteParams;
use lancedb::{
    data::scannable::Scannable, database::CreateTableMode, table::WriteOptions, Connection, Table,
};
use std::collections::HashMap;

#[derive(Default, serde::Deserialize)]
#[serde(default, deny_unknown_fields)]
pub(crate) struct CreateOptions {
    mode: Option<String>,
    blob: crate::blob::write_options::WriteOptions,
    location: Option<String>,
    storage_options: Option<HashMap<String, String>>,
    max_rows_per_file: Option<usize>,
    max_rows_per_group: Option<usize>,
    max_bytes_per_file: Option<usize>,
    data_storage_version: Option<String>,
    enable_stable_row_ids: Option<bool>,
    enable_v2_manifest_paths: Option<bool>,
}

pub(crate) struct PreparedCreation {
    mode: CreateTableMode,
    params: WriteParams,
    location: Option<String>,
    storage_options: Option<HashMap<String, String>>,
}

impl CreateOptions {
    pub(crate) fn parse(json: &str) -> Result<Self, Failure> {
        serde_json::from_str(json).map_err(Failure::from_error)
    }

    pub(crate) fn prepare(
        self,
        config: &crate::connection::ConnectionConfig,
    ) -> Result<PreparedCreation, Failure> {
        let mut params = WriteParams::default();
        self.blob.apply(&mut params)?;
        for value in [
            self.max_rows_per_file,
            self.max_rows_per_group,
            self.max_bytes_per_file,
        ]
        .into_iter()
        .flatten()
        {
            if value == 0 || value > u32::MAX as usize {
                return Err("Creation sizes must be positive uint32 integers".into());
            }
        }
        if let Some(v) = self.max_rows_per_file {
            params.max_rows_per_file = v;
        }
        if let Some(v) = self.max_rows_per_group {
            params.max_rows_per_group = v;
        }
        if let Some(v) = self.max_bytes_per_file {
            params.max_bytes_per_file = v;
        }
        // ListingDatabase gives storage options precedence over WriteParams and
        // connection defaults. Put explicit format/row-id settings there too.
        let mut storage = config.storage_options(self.storage_options);
        if let Some(v) = self.data_storage_version {
            storage
                .get_or_insert_with(HashMap::new)
                .insert("new_table_data_storage_version".into(), v);
        }
        if let Some(v) = self.enable_stable_row_ids {
            storage
                .get_or_insert_with(HashMap::new)
                .insert("new_table_enable_stable_row_ids".into(), v.to_string());
        }
        if let Some(v) = self.enable_v2_manifest_paths {
            storage
                .get_or_insert_with(HashMap::new)
                .insert("new_table_enable_v2_manifest_paths".into(), v.to_string());
        }
        let mode = match self.mode.as_deref().unwrap_or("create") {
            "create" => CreateTableMode::Create,
            "overwrite" => CreateTableMode::Overwrite,
            _ => return Err("Invalid creation mode".into()),
        };
        Ok(PreparedCreation {
            mode,
            params,
            location: self.location,
            storage_options: storage,
        })
    }
}

impl PreparedCreation {
    pub(crate) async fn create<T: Scannable + 'static>(
        self,
        conn: &Connection,
        name: String,
        data: T,
    ) -> Result<Table, Failure> {
        let mut builder =
            conn.create_table(name, data)
                .mode(self.mode)
                .write_options(WriteOptions {
                    lance_write_params: Some(self.params),
                });
        if let Some(storage) = self.storage_options {
            builder = builder.storage_options(storage);
        }
        if let Some(location) = self.location {
            builder = builder.location(location);
        }
        builder.execute().await.map_err(Failure::from_error)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connection::ConnectionConfig;

    #[test]
    fn creation_resolves_inheritance_overrides_and_typed_settings_in_order() {
        let config = ConnectionConfig::new(
            "/tmp/db".into(),
            HashMap::from([
                ("aws_endpoint".into(), "endpoint".into()),
                ("new_table_enable_stable_row_ids".into(), "false".into()),
            ]),
            None,
        );
        let prepared = CreateOptions::parse(r#"{"storage_options":{"aws_region":"eu-central-1","new_table_enable_stable_row_ids":"false"},"enable_stable_row_ids":true}"#).unwrap().prepare(&config).unwrap();
        let options = prepared.storage_options.unwrap();
        assert_eq!(options["aws_endpoint"], "endpoint");
        assert_eq!(options["aws_region"], "eu-central-1");
        assert_eq!(options["new_table_enable_stable_row_ids"], "true");
        assert_eq!(
            config.storage_options(None).unwrap()["new_table_enable_stable_row_ids"],
            "false"
        );

        let config = ConnectionConfig::new("/tmp/db".into(), HashMap::new(), None);
        assert!(CreateOptions::parse("{}")
            .unwrap()
            .prepare(&config)
            .unwrap()
            .storage_options
            .is_none());
        assert_eq!(
            CreateOptions::parse(r#"{"storage_options":{}}"#)
                .unwrap()
                .prepare(&config)
                .unwrap()
                .storage_options,
            Some(HashMap::new())
        );
    }
}
