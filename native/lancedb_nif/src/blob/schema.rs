//! Blob inputs use Lance's logical struct; query descriptors remain ordinary structs.
use crate::error::Failure;
use arrow_schema::{DataType, Field};
use serde_json::{json, Value};
use std::sync::LazyLock;

pub(crate) const EXTENSION: &str = "ARROW:extension:name";
pub(crate) const NAME: &str = "lance.blob.v2";

static INPUT_TYPE: LazyLock<DataType> =
    LazyLock::new(|| lancedb::blob("", true).data_type().clone());

pub(crate) fn is_input(field: &Field) -> bool {
    lancedb::blob::is_blob(field) && field.data_type() == &*INPUT_TYPE
}

pub(crate) fn normalize(value: &mut Value) -> Result<(), Failure> {
    if value
        .as_object()
        .is_some_and(|map| map.len() == 1 && map.contains_key("__lancedb_binary__"))
    {
        *value = json!({"data": value.take(), "uri": null});
    }
    let object = value
        .as_object_mut()
        .ok_or("Blob values must be binaries, nil, or Blob.reference(uri)")?;
    if object.len() != 2 || !object.contains_key("data") || !object.contains_key("uri") {
        return Err("Blob structs must contain data and uri".into());
    }
    if object["data"].is_null() == object["uri"].is_null() {
        return Err(
            "Blob values must supply exactly one of data or uri; use nil for a null blob".into(),
        );
    }
    if !object["uri"].is_null() {
        crate::binary::normalize(&mut object["uri"], &DataType::Utf8)?;
        if !object["uri"]
            .as_str()
            .is_some_and(|uri| !uri.trim().is_empty())
        {
            return Err("Blob URI must be a non-empty UTF-8 string".into());
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn blob_normalization_is_idempotent_and_preserves_binary_values() {
        let field = lancedb::blob("payload", true);
        let mut value = json!({"__lancedb_binary__": "00ff"});
        crate::value::normalize(&mut value, &field).unwrap();
        assert_eq!(value, json!({"data":"00ff", "uri":null}));
        crate::value::normalize(&mut value, &field).unwrap();
        assert_eq!(value, json!({"data":"00ff", "uri":null}));
        assert!(crate::value::normalize(&mut Value::Null, &field).is_ok());
        assert!(!is_input(&Field::new(
            "payload",
            lance_core::datatypes::BLOB_V2_DESC_TYPE.clone(),
            true
        )));
        // Never describe extended Arrow write layouts as a reusable two-field blob.
        let extended = Field::new(
            "payload",
            lance_core::datatypes::BLOB_V2_USER_TYPE.clone(),
            true,
        )
        .with_metadata(field.metadata().clone());
        assert!(!is_input(&extended));
        let wire = crate::schema::schema_json(&arrow_schema::Schema::new(vec![field]));
        assert!(wire.contains("\"type\":\"blob\""));
        assert!(!wire.contains(EXTENSION));
    }
}
