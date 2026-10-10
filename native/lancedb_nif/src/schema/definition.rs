//! Wire schema definitions shared by table creation and schema evolution.
use crate::error::Failure;
use arrow_schema::{DataType, Field, Schema};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

#[derive(serde::Serialize, serde::Deserialize)]
pub(crate) struct FieldDefinition {
    name: String,
    nullable: bool,
    #[serde(default)]
    metadata: HashMap<String, String>,
    #[serde(flatten)]
    data_type: TypeDefinition,
}

#[derive(serde::Serialize, serde::Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub(crate) enum TypeDefinition {
    String,
    LargeString,
    LargeBinary,
    Json,
    FixedSizeBinary {
        width: i32,
    },
    Decimal128 {
        precision: u8,
        scale: i8,
    },
    Decimal256 {
        precision: u8,
        scale: i8,
    },
    Time32 {
        unit: String,
    },
    Time64 {
        unit: String,
    },
    Duration {
        unit: String,
    },
    Dictionary {
        key: Box<TypeDefinition>,
        value: Box<TypeDefinition>,
    },
    LargeList {
        item: Option<Box<FieldDefinition>>,
    },
    Map {
        item: Option<Box<FieldDefinition>>,
    },
    Int8,
    Int16,
    Int32,
    Int64,
    Uint8,
    Uint16,
    Uint32,
    Uint64,
    Binary,
    Blob,
    Date32,
    Date64,
    Float16,
    Float32,
    Float64,
    Boolean,
    Timestamp {
        unit: Option<String>,
        timezone: Option<String>,
    },
    Vector {
        dimension: Option<i32>,
        item: Option<Box<FieldDefinition>>,
    },
    List {
        item: Option<Box<FieldDefinition>>,
    },
    Struct {
        fields: Option<Vec<FieldDefinition>>,
    },
    Unsupported {
        arrow_type: String,
    },
    #[serde(other)]
    Unknown,
}

impl TypeDefinition {
    pub(crate) fn into_arrow(self) -> Result<DataType, Failure> {
        Ok(match self {
            Self::String => DataType::Utf8,
            Self::LargeString => DataType::LargeUtf8,
            Self::LargeBinary => DataType::LargeBinary,
            Self::Json => {
                return Err("JSON types require a field definition, not a type cast".into())
            }
            Self::FixedSizeBinary { width } if width > 0 => DataType::FixedSizeBinary(width),
            Self::FixedSizeBinary { .. } => {
                return Err("Fixed binary width must be positive".into())
            }
            Self::Decimal128 { precision, scale } => {
                crate::extended_value::decimal_type(precision, scale, 38)?;
                DataType::Decimal128(precision, scale)
            }
            Self::Decimal256 { precision, scale } => {
                crate::extended_value::decimal_type(precision, scale, 76)?;
                DataType::Decimal256(precision, scale)
            }
            Self::Time32 { unit } => {
                let unit = crate::extended_value::unit(&unit)?;
                if !matches!(
                    unit,
                    arrow_schema::TimeUnit::Second | arrow_schema::TimeUnit::Millisecond
                ) {
                    return Err("Time32 requires seconds or milliseconds".into());
                }
                DataType::Time32(unit)
            }
            Self::Time64 { unit } => {
                let unit = crate::extended_value::unit(&unit)?;
                if !matches!(
                    unit,
                    arrow_schema::TimeUnit::Microsecond | arrow_schema::TimeUnit::Nanosecond
                ) {
                    return Err("Time64 requires microseconds or nanoseconds".into());
                }
                DataType::Time64(unit)
            }
            Self::Duration { unit } => DataType::Duration(crate::extended_value::unit(&unit)?),
            Self::Dictionary { key, value } => {
                let key = key.into_arrow()?;
                let value = value.into_arrow()?;
                if !key.is_integer() || !crate::extended_value::dictionary_value_type(&value) {
                    return Err(
                        "Dictionary requires integer keys and string, binary or numeric values"
                            .into(),
                    );
                }
                DataType::Dictionary(Box::new(key), Box::new(value))
            }
            Self::LargeList { item } => {
                DataType::LargeList(Arc::new(item.ok_or("List item required")?.into_arrow()?))
            }
            Self::Map { item } => {
                let item = item.ok_or("Map entries required")?.into_arrow()?;
                crate::extended_value::map_fields(&item)?;
                DataType::Map(Arc::new(item), false)
            }
            Self::Int8 => DataType::Int8,
            Self::Int16 => DataType::Int16,
            Self::Int32 => DataType::Int32,
            Self::Int64 => DataType::Int64,
            Self::Uint8 => DataType::UInt8,
            Self::Uint16 => DataType::UInt16,
            Self::Uint32 => DataType::UInt32,
            Self::Uint64 => DataType::UInt64,
            Self::Binary => DataType::Binary,
            Self::Blob => {
                return Err("Blob types require a field definition, not a type cast".into())
            }
            Self::Date32 => DataType::Date32,
            Self::Date64 => DataType::Date64,
            Self::Float16 => DataType::Float16,
            Self::Float32 => DataType::Float32,
            Self::Float64 => DataType::Float64,
            Self::Boolean => DataType::Boolean,
            Self::Timestamp { unit, timezone } => crate::timestamp::data_type(
                unit.as_deref().ok_or("Timestamp unit is required")?,
                timezone,
            )?,
            Self::Vector { dimension, item } => {
                let item = match item {
                    Some(item) => item.into_arrow()?,
                    None => Field::new("item", DataType::Float32, true),
                };
                if !crate::value::vector_element_type(item.data_type()) {
                    return Err("Vector items must be float16, float32, float64 or uint8".into());
                }
                DataType::FixedSizeList(
                    Arc::new(item),
                    dimension
                        .filter(|d| *d > 0)
                        .ok_or("Invalid vector dimension")?,
                )
            }
            Self::List { item } => DataType::List(Arc::new(
                item.ok_or("List item type is required")?.into_arrow()?,
            )),
            Self::Struct { fields } => {
                DataType::Struct(arrow_fields(fields.ok_or("Struct fields are required")?)?.into())
            }
            Self::Unsupported { .. } | Self::Unknown => {
                return Err("Unsupported schema type".into())
            }
        })
    }
}

impl FieldDefinition {
    fn into_arrow(self) -> Result<Field, Failure> {
        if matches!(self.data_type, TypeDefinition::Json) {
            return crate::json_value::field(self.name, self.nullable, self.metadata);
        }
        if matches!(self.data_type, TypeDefinition::Blob) {
            let mut metadata = self.metadata;
            if metadata
                .get(crate::blob::schema::EXTENSION)
                .is_some_and(|v| v != crate::blob::schema::NAME)
            {
                return Err("Conflicting blob extension metadata".into());
            }
            let field = lancedb::blob(&self.name, self.nullable);
            metadata.extend(field.metadata().clone());
            return Ok(field.with_metadata(metadata));
        }
        if self
            .metadata
            .get(crate::blob::schema::EXTENSION)
            .is_some_and(|v| v == crate::blob::schema::NAME)
            || self.metadata.contains_key("lance-encoding:blob")
        {
            return Err("Blob descriptor and custom Arrow blob schemas are read-only; declare a blob field and supply payload bytes or a URI".into());
        }
        Ok(
            Field::new(self.name, self.data_type.into_arrow()?, self.nullable)
                .with_metadata(self.metadata),
        )
    }
}

pub(crate) fn explicit_schema(json: &str) -> Result<Arc<Schema>, Failure> {
    let fields: Vec<FieldDefinition> = serde_json::from_str(json).map_err(Failure::from_error)?;
    Ok(Arc::new(Schema::new(arrow_fields(fields)?)))
}

fn arrow_fields(fields: Vec<FieldDefinition>) -> Result<Vec<Field>, Failure> {
    if fields.is_empty() {
        return Err("Schema must contain fields".into());
    }
    let mut names = HashSet::new();
    fields
        .into_iter()
        .map(|field| {
            if field.name.trim().is_empty() || !names.insert(field.name.clone()) {
                return Err("Schema field names must be non-empty and unique".into());
            }
            field.into_arrow()
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_schema::TimeUnit;
    use serde_json::json;

    fn assert_type(definition: serde_json::Value, expected: DataType) {
        let data_type: TypeDefinition = serde_json::from_value(definition.clone()).unwrap();
        assert_eq!(data_type.into_arrow().unwrap(), expected);
        let mut field = definition;
        field["name"] = "column".into();
        field["nullable"] = false.into();
        field["metadata"] = json!({"label": "雪"});
        let schema = explicit_schema(&json!([field]).to_string()).unwrap();
        assert_eq!(schema.field(0).data_type(), &expected);
        assert!(!schema.field(0).is_nullable());
        assert_eq!(schema.field(0).metadata()["label"], "雪");
        let encoded = serde_json::to_string(&FieldDefinition::from(schema.field(0))).unwrap();
        let decoded: FieldDefinition = serde_json::from_str(&encoded).unwrap();
        assert_eq!(decoded.into_arrow().unwrap(), *schema.field(0));
    }

    #[test]
    fn creation_and_standalone_types_preserve_all_primitives_and_timestamp_units() {
        for (name, expected) in [
            ("string", DataType::Utf8),
            ("binary", DataType::Binary),
            ("boolean", DataType::Boolean),
            ("int8", DataType::Int8),
            ("int16", DataType::Int16),
            ("int32", DataType::Int32),
            ("int64", DataType::Int64),
            ("uint8", DataType::UInt8),
            ("uint16", DataType::UInt16),
            ("uint32", DataType::UInt32),
            ("uint64", DataType::UInt64),
            ("float16", DataType::Float16),
            ("float32", DataType::Float32),
            ("float64", DataType::Float64),
            ("date32", DataType::Date32),
            ("date64", DataType::Date64),
        ] {
            assert_type(json!({"type": name}), expected);
        }
        for (name, unit) in [
            ("second", TimeUnit::Second),
            ("millisecond", TimeUnit::Millisecond),
            ("microsecond", TimeUnit::Microsecond),
            ("nanosecond", TimeUnit::Nanosecond),
        ] {
            for timezone in [None, Some("UTC"), Some("Europe/Vienna"), Some("+02:00")] {
                assert_type(
                    json!({"type": "timestamp", "unit": name, "timezone": timezone}),
                    DataType::Timestamp(unit.clone(), timezone.map(Into::into)),
                );
            }
        }
    }

    #[test]
    fn recursive_types_preserve_child_names_nullability_metadata_and_vector_dimensions() {
        let definition = json!({"type": "list", "item": {
            "name": "item", "nullable": false, "metadata": {"element": "document"},
            "type": "struct", "fields": [
                {"name": "a.b", "nullable": true, "type": "vector", "dimension": 3,
                    "metadata": {"model": "embedding"}},
                {"name": "at", "nullable": false, "type": "timestamp", "unit": "microsecond"}
            ]
        }});
        let vector = Field::new(
            "a.b",
            DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, true)), 3),
            true,
        )
        .with_metadata(HashMap::from([("model".into(), "embedding".into())]));
        let at = Field::new(
            "at",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            false,
        );
        let item = Field::new("item", DataType::Struct(vec![vector, at].into()), false)
            .with_metadata(HashMap::from([("element".into(), "document".into())]));
        assert_type(definition, DataType::List(Arc::new(item)));
    }

    #[test]
    fn invalid_types_are_rejected_by_creation_and_standalone_conversion() {
        for definition in [
            json!({"type": "unknown"}),
            json!({"type": "list"}),
            json!({"type": "struct"}),
            json!({"type": "struct", "fields": []}),
            json!({"type": "struct", "fields": [
                {"name": "same", "type": "string", "nullable": true},
                {"name": "same", "type": "int64", "nullable": true}
            ]}),
            json!({"type": "struct", "fields": [{"name": " ", "type": "int64", "nullable": true}]}),
            json!({"type": "vector"}),
            json!({"type": "vector", "dimension": 0}),
            json!({"type": "vector", "dimension": -1}),
            json!({"type": "timestamp"}),
            json!({"type": "timestamp", "unit": "bad"}),
            json!({"type": "timestamp", "unit": "second", "timezone": "invalid/timezone"}),
        ] {
            let data_type: TypeDefinition = serde_json::from_value(definition.clone()).unwrap();
            let error = data_type.into_arrow().unwrap_err();
            let mut field = definition;
            field["name"] = "column".into();
            field["nullable"] = true.into();
            assert_eq!(
                explicit_schema(&json!([field]).to_string()).unwrap_err(),
                error
            );
        }
    }
}

impl From<&Field> for FieldDefinition {
    fn from(field: &Field) -> Self {
        let is_blob = crate::blob::schema::is_input(field);
        let mut metadata = field.metadata().clone();
        if is_blob {
            metadata.remove(crate::blob::schema::EXTENSION);
        }
        Self {
            name: field.name().clone(),
            nullable: field.is_nullable(),
            metadata,
            data_type: if is_blob {
                TypeDefinition::Blob
            } else {
                field.data_type().into()
            },
        }
    }
}
impl From<&DataType> for TypeDefinition {
    fn from(value: &DataType) -> Self {
        match value {
            DataType::Utf8 => Self::String,
            DataType::LargeUtf8 => Self::LargeString,
            DataType::LargeBinary => Self::LargeBinary,
            DataType::FixedSizeBinary(width) => Self::FixedSizeBinary { width: *width },
            DataType::Decimal128(precision, scale) => Self::Decimal128 {
                precision: *precision,
                scale: *scale,
            },
            DataType::Decimal256(precision, scale) => Self::Decimal256 {
                precision: *precision,
                scale: *scale,
            },
            DataType::Time32(unit) => Self::Time32 {
                unit: crate::extended_value::unit_name(unit).into(),
            },
            DataType::Time64(unit) => Self::Time64 {
                unit: crate::extended_value::unit_name(unit).into(),
            },
            DataType::Duration(unit) => Self::Duration {
                unit: crate::extended_value::unit_name(unit).into(),
            },
            DataType::Dictionary(key, value) => Self::Dictionary {
                key: Box::new(key.as_ref().into()),
                value: Box::new(value.as_ref().into()),
            },
            DataType::LargeList(item) => Self::LargeList {
                item: Some(Box::new(item.as_ref().into())),
            },
            DataType::Map(item, false) => Self::Map {
                item: Some(Box::new(item.as_ref().into())),
            },
            DataType::Binary => Self::Binary,
            DataType::Boolean => Self::Boolean,
            DataType::Int8 => Self::Int8,
            DataType::Int16 => Self::Int16,
            DataType::Int32 => Self::Int32,
            DataType::Int64 => Self::Int64,
            DataType::UInt8 => Self::Uint8,
            DataType::UInt16 => Self::Uint16,
            DataType::UInt32 => Self::Uint32,
            DataType::UInt64 => Self::Uint64,
            DataType::Date32 => Self::Date32,
            DataType::Date64 => Self::Date64,
            DataType::Float16 => Self::Float16,
            DataType::Float32 => Self::Float32,
            DataType::Float64 => Self::Float64,
            DataType::Timestamp(unit, zone) => Self::Timestamp {
                unit: Some(
                    match unit {
                        arrow_schema::TimeUnit::Second => "second",
                        arrow_schema::TimeUnit::Millisecond => "millisecond",
                        arrow_schema::TimeUnit::Microsecond => "microsecond",
                        arrow_schema::TimeUnit::Nanosecond => "nanosecond",
                    }
                    .into(),
                ),
                timezone: zone.as_ref().map(|zone| zone.to_string()),
            },
            DataType::List(item) => Self::List {
                item: Some(Box::new(item.as_ref().into())),
            },
            DataType::Struct(fields) => Self::Struct {
                fields: Some(fields.iter().map(|field| field.as_ref().into()).collect()),
            },
            DataType::FixedSizeList(item, dimension)
                if crate::value::vector_element_type(item.data_type()) =>
            {
                Self::Vector {
                    dimension: Some(*dimension),
                    item: Some(Box::new(item.as_ref().into())),
                }
            }
            other => Self::Unsupported {
                arrow_type: format!("{other:?}"),
            },
        }
    }
}
