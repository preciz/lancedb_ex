//! Stored-field validation and canonical JSON values shared by all literal writes.
use crate::error::Failure;
use arrow_schema::{DataType, Field};
use serde_json::Value;

pub(crate) fn normalize(value: &mut Value, field: &Field) -> Result<(), Failure> {
    if value.is_null() {
        return if field.is_nullable() {
            Ok(())
        } else {
            Err(mismatch(field).into())
        };
    }
    if crate::json_value::is_json(field) {
        return crate::json_value::normalize(value, field);
    }
    if let DataType::Dictionary(_, item) = field.data_type() {
        return normalize(value, &field.clone().with_data_type(item.as_ref().clone()));
    }
    if crate::blob::schema::is_input(field) {
        crate::blob::schema::normalize(value)?;
    }
    if matches!(
        field.data_type(),
        DataType::Utf8
            | DataType::LargeUtf8
            | DataType::FixedSizeBinary(_)
            | DataType::Decimal128(_, _)
            | DataType::Decimal256(_, _)
            | DataType::Time32(_)
            | DataType::Time64(_)
            | DataType::Binary
            | DataType::LargeBinary
            | DataType::Date32
            | DataType::Date64
            | DataType::Timestamp(_, _)
    ) {
        crate::binary::normalize(value, field.data_type())?;
    }
    if matches!(field.data_type(), DataType::Date32 | DataType::Date64) {
        *value = crate::date::value(value, field.data_type())?.into();
        return Ok(());
    }
    if let DataType::Timestamp(unit, timezone) = field.data_type() {
        *value = crate::timestamp::value(value, unit, timezone.as_deref())?.into();
        return Ok(());
    }
    match field.data_type() {
        DataType::Decimal128(precision, scale) | DataType::Decimal256(precision, scale) => {
            *value = crate::extended_value::decimal(value, *precision, *scale)?.into();
            return Ok(());
        }
        DataType::Time32(unit) | DataType::Time64(unit) => {
            *value = crate::extended_value::time(value, unit)?.into();
            return Ok(());
        }
        DataType::Map(item, _) => return crate::extended_value::map(value, item),
        DataType::List(item) | DataType::LargeList(item) => {
            let values = value.as_array_mut().ok_or_else(|| mismatch(field))?;
            for (index, value) in values.iter_mut().enumerate() {
                normalize(value, item).map_err(|e| {
                    let message = format!("item [{index}]: {e}");
                    Failure::from_error(e).context(message)
                })?;
            }
            return Ok(());
        }
        DataType::Struct(fields) => {
            let values = value.as_object_mut().ok_or_else(|| mismatch(field))?;
            if values.len() != fields.len() {
                return Err("Struct must contain every child field and no extra fields".into());
            }
            for child in fields {
                let value = values
                    .get_mut(child.name())
                    .ok_or_else(|| format!("Missing struct field '{}'", child.name()))?;
                normalize(value, child).map_err(|e| {
                    let message = format!("field '{}': {e}", child.name());
                    Failure::from_error(e).context(message)
                })?;
            }
            return Ok(());
        }
        _ => {}
    }
    if valid_scalar_or_vector(value, field) {
        Ok(())
    } else {
        Err(mismatch(field).into())
    }
}

fn mismatch(field: &Field) -> String {
    format!(
        "does not match {:?} (nullable: {})",
        field.data_type(),
        field.is_nullable()
    )
}

fn valid_scalar_or_vector(value: &Value, field: &Field) -> bool {
    match field.data_type() {
        DataType::Utf8 | DataType::LargeUtf8 => value.is_string(),
        DataType::Duration(_) => value.as_i64().is_some(),
        DataType::FixedSizeBinary(width) => value.as_str().is_some_and(|s| {
            crate::binary::decode(s).is_ok_and(|bytes| bytes.len() == *width as usize)
        }),
        DataType::Binary | DataType::LargeBinary => value
            .as_str()
            .is_some_and(|s| crate::binary::decode(s).is_ok()),
        DataType::Int8 => value.as_i64().is_some_and(|n| i8::try_from(n).is_ok()),
        DataType::Int16 => value.as_i64().is_some_and(|n| i16::try_from(n).is_ok()),
        DataType::Int32 => value.as_i64().is_some_and(|n| i32::try_from(n).is_ok()),
        DataType::Int64 => value.as_i64().is_some(),
        DataType::UInt8 => value.as_u64().is_some_and(|n| u8::try_from(n).is_ok()),
        DataType::UInt16 => value.as_u64().is_some_and(|n| u16::try_from(n).is_ok()),
        DataType::UInt32 => value.as_u64().is_some_and(|n| u32::try_from(n).is_ok()),
        DataType::UInt64 => value.as_u64().is_some(),
        DataType::Float16 => value.as_f64().is_some_and(|n| n.abs() <= 65504.0),
        DataType::Float64 => value.as_f64().is_some_and(f64::is_finite),
        DataType::Float32 => value.as_f64().is_some_and(|n| n.abs() <= f32::MAX as f64),
        DataType::Boolean => value.is_boolean(),
        DataType::FixedSizeList(item, dimension) if vector_element_type(item.data_type()) => {
            value.as_array().is_some_and(|values| {
                values.len() == *dimension as usize
                    && values
                        .iter()
                        .all(|v| !v.is_null() && valid_scalar_or_vector(v, item))
            })
        }
        _ => false,
    }
}

pub(crate) fn supported_field_type(data_type: &DataType) -> bool {
    match data_type {
        DataType::Utf8
        | DataType::LargeUtf8
        | DataType::FixedSizeBinary(_)
        | DataType::Decimal128(_, _)
        | DataType::Decimal256(_, _)
        | DataType::Time32(_)
        | DataType::Time64(_)
        | DataType::Duration(_)
        | DataType::Binary
        | DataType::LargeBinary
        | DataType::Date32
        | DataType::Date64
        | DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32
        | DataType::UInt64
        | DataType::Float16
        | DataType::Float32
        | DataType::Float64
        | DataType::Boolean
        | DataType::Timestamp(_, _) => true,
        DataType::FixedSizeList(item, dim) => *dim > 0 && vector_element_type(item.data_type()),
        DataType::List(item) | DataType::LargeList(item) | DataType::Map(item, false) => {
            supported_field_type(item.data_type())
        }
        DataType::Dictionary(key, value) => {
            key.is_integer() && crate::extended_value::dictionary_value_type(value)
        }
        DataType::Struct(fields) => {
            !fields.is_empty()
                && fields
                    .iter()
                    .all(|field| supported_field_type(field.data_type()))
        }
        _ => false,
    }
}

/// Encode an already-normalized value using the destination type.
pub(crate) fn sql_literal(value: &Value, data_type: &DataType) -> Result<String, Failure> {
    if value.is_null() {
        return typed_cast("NULL", data_type);
    }
    if matches!(data_type, DataType::Decimal128(_, scale) | DataType::Decimal256(_, scale) if *scale < 0)
    {
        return Err(Failure::new("unsupported", "Arrow 58 cannot cast negative-scale decimal SQL literals; use upsert or merge with complete rows"));
    }
    let expression: Result<String, Failure> = match data_type {
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Decimal128(_, _) | DataType::Decimal256(_, _) => Ok(quote(value.as_str().ok_or("Expected string literal")?)),
        DataType::Binary | DataType::LargeBinary | DataType::FixedSizeBinary(_) => Ok(format!(
            "decode({}, 'hex')",
            quote(value.as_str().ok_or("Expected binary literal")?)
        )),
        data_type if data_type.is_integer() => Ok(quote(&value.to_string())),
        DataType::Time32(_) => Ok(format!("arrow_cast({}, 'Int32')", quote(&value.to_string()))),
        // Cast epoch integers directly: casting a timezone-free timestamp to a
        // zoned timestamp would reinterpret it as local time and shift the instant.
        DataType::Timestamp(_, _) | DataType::Date32 | DataType::Date64 | DataType::Time64(_) | DataType::Duration(_) => Ok(format!(
            "arrow_cast({}, 'Int64')",
            quote(&value.to_string())
        )),
        DataType::Dictionary(_, item) => sql_literal(value, item),
        DataType::Map(_, _) => return Err("Map literal updates are unsupported by the SQL type cast; use upsert or merge with complete rows".into()),
        DataType::List(item) | DataType::LargeList(item) | DataType::FixedSizeList(item, _) => {
            let values = value.as_array().ok_or("Expected list literal")?;
            let expressions = values
                .iter()
                .map(|value| sql_literal(value, item.data_type()))
                .collect::<Result<Vec<_>, _>>()?;
            Ok(format!("make_array({})", expressions.join(",")))
        }
        DataType::Struct(fields) => {
            if fields.iter().any(|field| !field.is_nullable()) {
                return Err("SQL literals cannot cast non-null structs to required child fields; use Table.upsert or Table.merge with complete records".into());
            }
            let values = value.as_object().ok_or("Expected struct literal")?;
            let mut arguments = Vec::new();
            for field in fields {
                arguments.push(quote(field.name()));
                arguments.push(sql_literal(&values[field.name()], field.data_type())?);
            }
            Ok(format!("named_struct({})", arguments.join(",")))
        }
        _ => Ok(value.to_string()),
    };
    let expression = expression?;
    typed_cast(&expression, data_type)
}

fn typed_cast(expression: &str, data_type: &DataType) -> Result<String, Failure> {
    let description = data_type.to_string();
    crate::compat::storage::validate_cast_type(data_type)?;
    Ok(format!("arrow_cast({expression}, {})", quote(&description)))
}

fn quote(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

pub(crate) fn vector_element_type(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Float16 | DataType::Float32 | DataType::Float64 | DataType::UInt8
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_schema::TimeUnit;
    use serde_json::json;
    use std::sync::Arc;

    #[test]
    fn normalization_rejects_coercion_and_preserves_supported_values() {
        for (data_type, good, bad) in [
            (DataType::Utf8, json!("42"), json!(42)),
            (DataType::Int64, json!(i64::MIN), json!(1.5)),
            (DataType::Float32, json!(1.25), json!(1e100)),
            (DataType::Float64, json!(1e100), json!("1.0")),
            (DataType::Boolean, json!(false), json!(0)),
            (
                DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, true)), 2),
                json!([1, 2]),
                json!([1, null]),
            ),
        ] {
            assert!(supported_field_type(&data_type));
            let field = Field::new("value", data_type, false);
            let mut normalized = good.clone();
            normalize(&mut normalized, &field).unwrap();
            assert_eq!(normalized, good);
            assert!(normalize(&mut bad.clone(), &field).is_err());
            assert!(normalize(&mut Value::Null, &field).is_err());
            assert!(normalize(&mut Value::Null, &field.with_nullable(true)).is_ok());
        }
        let unsupported = Field::new("value", DataType::Null, false);
        assert!(!supported_field_type(unsupported.data_type()));
        assert!(normalize(&mut json!("bytes"), &unsupported).is_err());
    }

    #[test]
    fn timestamp_normalization_is_exact_and_idempotent() {
        let field = Field::new(
            "at",
            DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
            false,
        );
        let mut value = json!("1969-12-31T23:59:59.999Z");
        normalize(&mut value, &field).unwrap();
        assert_eq!(value, json!(-1));
        normalize(&mut value, &field).unwrap();
        assert_eq!(value, json!(-1));
        assert!(normalize(&mut json!("1969-12-31T23:59:59.999001Z"), &field).is_err());
        assert!(normalize(&mut json!("1969-12-31T23:59:59.999"), &field).is_err());
    }
}
