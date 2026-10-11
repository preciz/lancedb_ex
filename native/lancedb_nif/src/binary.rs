//! Schema-directed binary transport: input bytes use a tag; output carries a
//! separate type tree so user structs can never be mistaken for transport tags.
use crate::error::Failure;
use arrow_schema::{DataType, Fields};
use serde_json::{Map, Value};

pub(crate) fn decode(hex: &str) -> Result<Vec<u8>, Failure> {
    if !hex.len().is_multiple_of(2) {
        return Err("Invalid binary hex encoding".into());
    }
    hex.as_bytes()
        .as_chunks::<2>()
        .0
        .iter()
        .map(|pair| {
            let high = (pair[0] as char).to_digit(16);
            let low = (pair[1] as char).to_digit(16);
            high.zip(low)
                .map(|(h, l)| (h * 16 + l) as u8)
                .ok_or_else(|| "Invalid binary hex encoding".into())
        })
        .collect()
}

pub(crate) fn normalize(value: &mut Value, data_type: &DataType) -> Result<(), Failure> {
    let Some(object) = value.as_object().filter(|object| object.len() == 1) else {
        return Ok(());
    };
    let Some(hex) = object.get("__lancedb_binary__").and_then(Value::as_str) else {
        return Ok(());
    };
    let bytes = decode(hex)?;
    *value = if matches!(
        data_type,
        DataType::Binary | DataType::LargeBinary | DataType::FixedSizeBinary(_)
    ) {
        Value::String(hex.to_owned())
    } else {
        Value::String(String::from_utf8(bytes).map_err(|_| "String values must be valid UTF-8")?)
    };
    Ok(())
}

pub(crate) fn fields(fields: &Fields) -> Map<String, Value> {
    fields
        .iter()
        .filter_map(|field| {
            field_descriptor(field).map(|description| (field.name().clone(), description))
        })
        .collect()
}

fn field_descriptor(field: &arrow_schema::Field) -> Option<Value> {
    if crate::json_value::is_json(field) {
        Some("json".into())
    } else {
        descriptor(field.data_type())
    }
}

fn descriptor(data_type: &DataType) -> Option<Value> {
    match data_type {
        DataType::Binary | DataType::LargeBinary | DataType::FixedSizeBinary(_) => {
            Some("binary".into())
        }
        DataType::List(item) | DataType::LargeList(item) | DataType::FixedSizeList(item, _) => {
            field_descriptor(item).map(|item| serde_json::json!({"list": item}))
        }
        DataType::Dictionary(_, value) => descriptor(value),
        DataType::Map(entries, _) => {
            let DataType::Struct(children) = entries.data_type() else {
                return None;
            };
            Some(serde_json::json!({"map": {
                "key_name": children[0].name(), "value_name": children[1].name(),
                "key": field_descriptor(&children[0]), "value": field_descriptor(&children[1])
            }}))
        }
        DataType::Struct(children) => {
            let children = fields(children);
            (!children.is_empty()).then(|| serde_json::json!({"struct": children}))
        }
        _ => None,
    }
}
