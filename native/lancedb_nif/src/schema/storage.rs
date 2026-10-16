//! Checks schema fields before the package stores them.
use crate::error::Failure;
use arrow_schema::{DataType, Field, SchemaRef};
use std::collections::{HashMap, HashSet};

pub(crate) struct StorageSchema(SchemaRef);

impl StorageSchema {
    pub(crate) fn prepare(schema: SchemaRef) -> Result<Self, Failure> {
        validate_fields(schema.fields().iter().map(|field| field.as_ref()))?;
        Ok(Self(schema))
    }

    pub(crate) fn into_arrow(self) -> SchemaRef {
        self.0
    }
}

fn validate_fields<'a>(fields: impl Iterator<Item = &'a Field>) -> Result<(), Failure> {
    let mut names = HashSet::new();
    for field in fields {
        if field.name().trim().is_empty() || !names.insert(field.name()) {
            return Err("Schema field names must be non-empty and unique".into());
        }
        validate_field(field)?;
    }
    if names.is_empty() {
        return Err("Schema must contain fields".into());
    }
    Ok(())
}

fn validate_field(field: &Field) -> Result<(), Failure> {
    if !crate::value::supported_field_type(field.data_type()) {
        return Err(Failure::new(
            "unsupported",
            format!(
                "Column '{}' has unsupported type {:?}",
                field.name(),
                field.data_type()
            ),
        ));
    }
    if (lancedb::blob::is_blob(field) && !crate::blob::schema::is_input(field))
        || field.metadata().contains_key("lance-encoding:blob")
    {
        return Err(Failure::new(
            "unsupported",
            "Custom Arrow blob schemas are read-only; declare a blob field",
        ));
    }
    if field
        .metadata()
        .get("ARROW:extension:name")
        .is_some_and(|name| matches!(name.as_str(), "lance.json" | "arrow.json"))
        && !crate::json_value::is_json(field)
    {
        return Err(Failure::new(
            "unsupported",
            "JSON extension metadata requires its JSON storage type",
        ));
    }
    crate::compat::storage::validate_storage_type(field.data_type())?;
    match field.data_type() {
        DataType::Struct(fields) => validate_fields(fields.iter().map(|field| field.as_ref())),
        DataType::List(item) | DataType::LargeList(item) | DataType::FixedSizeList(item, _) => {
            validate_field(item)
        }
        DataType::Map(entries, _) => {
            crate::extended_value::map_fields(entries)?;
            validate_field(entries)
        }
        _ => Ok(()),
    }
}

pub(crate) fn prepare_cast(
    field: &lance::datatypes::Field,
    data_type: DataType,
) -> Result<DataType, Failure> {
    let field = Field::from(field).with_data_type(data_type.clone());
    validate_field(&field)?;
    Ok(data_type)
}

fn owned_metadata(key: &str) -> bool {
    matches!(
        key,
        "ARROW:extension:name" | "ARROW:extension:metadata" | "lance-encoding:blob"
    ) || primary_key_metadata(key)
}

fn primary_key_metadata(key: &str) -> bool {
    matches!(
        key,
        lance_core::datatypes::LANCE_UNENFORCED_PRIMARY_KEY
            | lance_core::datatypes::LANCE_UNENFORCED_PRIMARY_KEY_POSITION
    )
}

/// Replacement changes user metadata and keeps engine-owned markers.
/// Metadata operations cannot change these markers, including on ordinary fields.
pub(crate) fn prepare_metadata(
    schema: &arrow_schema::Schema,
    path: String,
    mut metadata: HashMap<String, Option<String>>,
    replace: bool,
) -> Result<lancedb::table::FieldMetadataUpdate, Failure> {
    let schema = lance::datatypes::Schema::try_from(schema).map_err(Failure::from_error)?;
    let field = schema
        .field(&path)
        .ok_or_else(|| format!("Column '{path}' does not exist"))?;
    for (key, value) in &metadata {
        if owned_metadata(key) && value.as_ref() != field.metadata.get(key) {
            return Err(Failure::new(
                "unsupported",
                format!(
                    "Engine-owned metadata '{key}' cannot be changed through a metadata update"
                ),
            ));
        }
    }
    // Lance rejects even identical writes to an installed primary-key marker.
    metadata.retain(|key, _| !primary_key_metadata(key));
    let preserve_primary_key = replace && field.is_unenforced_primary_key();
    if preserve_primary_key {
        // Express replacement as a delta, leaving the immutable marker untouched.
        for key in field.metadata.keys().filter(|key| !owned_metadata(key)) {
            metadata.entry(key.clone()).or_insert(None);
        }
    } else if replace {
        for (key, value) in &field.metadata {
            if owned_metadata(key) {
                metadata.insert(key.clone(), Some(value.clone()));
            }
        }
    }
    Ok(lancedb::table::FieldMetadataUpdate {
        path,
        metadata,
        replace: replace && !preserve_primary_key,
    })
}
