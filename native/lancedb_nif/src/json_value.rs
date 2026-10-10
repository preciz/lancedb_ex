//! Logical JSON values use Lance's native JSONB encoding, never opaque JSON text columns.
use crate::error::Failure;
use arrow_schema::{DataType, Field};
use lance_arrow::json::{is_arrow_json_field, is_json_field};
use serde_json::Value;
use std::collections::HashMap;

pub(crate) fn is_json(field: &Field) -> bool {
    is_json_field(field) || is_arrow_json_field(field)
}

pub(crate) fn field(
    name: String,
    nullable: bool,
    mut metadata: HashMap<String, String>,
) -> Result<Field, Failure> {
    if metadata
        .get("ARROW:extension:name")
        .is_some_and(|name| name != "lance.json")
    {
        return Err("Conflicting JSON extension metadata".into());
    }
    let field = lance_arrow::json::json_field(&name, nullable);
    metadata.extend(field.metadata().clone());
    Ok(field.with_metadata(metadata))
}

pub(crate) fn normalize(value: &mut Value, field: &Field) -> Result<(), Failure> {
    let json = if let Some(text) = value
        .as_object()
        .filter(|map| map.len() == 1)
        .and_then(|map| map.get("__lancedb_json__"))
        .and_then(Value::as_str)
    {
        serde_json::from_str(text).map_err(Failure::from_error)?
    } else {
        decode_wire(value.clone())?
    };
    let text = json.to_string();
    *value = if is_json_field(field) {
        let bytes =
            lance_arrow::json::encode_json(&text).map_err(|e| Failure::from(e.to_string()))?;
        Value::String(bytes.iter().map(|b| format!("{b:02x}")).collect())
    } else {
        Value::String(text)
    };
    Ok(())
}

fn decode_wire(mut value: Value) -> Result<Value, Failure> {
    // Only wire-tag values are strings here. An actual object with this key has
    // a tagged string as its value and therefore cannot be mistaken for the tag.
    if value.as_object().is_some_and(|map| {
        map.len() == 1 && map.get("__lancedb_binary__").is_some_and(Value::is_string)
    }) {
        crate::binary::normalize(&mut value, &DataType::Utf8)?;
        return Ok(value);
    }
    match value {
        Value::Array(values) => Ok(Value::Array(
            values
                .into_iter()
                .map(decode_wire)
                .collect::<Result<_, _>>()?,
        )),
        Value::Object(values) => Ok(Value::Object(
            values
                .into_iter()
                .map(|(key, value)| Ok((key, decode_wire(value)?)))
                .collect::<Result<_, Failure>>()?,
        )),
        value => Ok(value),
    }
}
