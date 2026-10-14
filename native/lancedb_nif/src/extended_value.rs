//! Exact values for the Arrow types that need more than JSON's scalar coercions.
use crate::error::Failure;
use arrow_schema::{DataType, Field, Fields, TimeUnit};
use serde_json::Value;

pub(crate) fn unit(name: &str) -> Result<TimeUnit, Failure> {
    match name {
        "second" => Ok(TimeUnit::Second),
        "millisecond" => Ok(TimeUnit::Millisecond),
        "microsecond" => Ok(TimeUnit::Microsecond),
        "nanosecond" => Ok(TimeUnit::Nanosecond),
        _ => Err("Invalid temporal unit".into()),
    }
}

pub(crate) fn unit_name(unit: &TimeUnit) -> &'static str {
    match unit {
        TimeUnit::Second => "second",
        TimeUnit::Millisecond => "millisecond",
        TimeUnit::Microsecond => "microsecond",
        TimeUnit::Nanosecond => "nanosecond",
    }
}

pub(crate) fn decimal_type(precision: u8, scale: i8, maximum: u8) -> Result<(), Failure> {
    if precision == 0 || precision > maximum || i16::from(scale) > i16::from(precision) {
        return Err("Invalid decimal precision or scale".into());
    }
    Ok(())
}

/// Canonical decimal text, rejecting overflow and any non-zero truncated digits.
/// Floats are deliberately excluded: their decimal representation is not exact.
pub(crate) fn decimal(value: &Value, precision: u8, scale: i8) -> Result<String, Failure> {
    let text = value
        .as_str()
        .ok_or("Decimals require plain decimal strings")?;
    let unsigned = text.strip_prefix(['-', '+']).unwrap_or(text);
    let parts: Vec<_> = unsigned.split('.').collect();
    if parts.is_empty()
        || parts.len() > 2
        || parts
            .iter()
            .any(|p| p.is_empty() || !p.bytes().all(|b| b.is_ascii_digit()))
    {
        return Err("Decimals require plain decimal strings without exponents".into());
    }
    let fraction = parts.get(1).copied().unwrap_or("");
    let digits = format!("{}{}", parts[0], fraction);
    let mut digits = digits.trim_start_matches('0').to_owned();
    if digits.is_empty() {
        digits.push('0');
    }
    // Enough guard digits to reject absurd strings without integer arithmetic overflow.
    let shift =
        i64::from(scale) - i64::try_from(fraction.len()).map_err(|_| "Decimal is too long")?;
    if shift < 0 {
        let removed = usize::try_from(-shift).map_err(|_| "Decimal is too long")?;
        let keep = digits.len().saturating_sub(removed);
        if digits[keep..].bytes().any(|b| b != b'0') {
            return Err("Decimal would lose precision at the column scale".into());
        }
        digits.truncate(keep);
    } else if digits != "0" {
        if digits.len() + shift as usize > precision as usize {
            return Err("Decimal exceeds column precision".into());
        }
        digits.push_str(&"0".repeat(shift as usize));
    }
    let digits = digits.trim_start_matches('0');
    if digits.len() > precision as usize {
        return Err("Decimal exceeds column precision".into());
    }
    let mut digits = if digits.is_empty() {
        "0".to_owned()
    } else {
        digits.to_owned()
    };
    let negative = text.starts_with('-') && digits != "0";
    if scale > 0 {
        let width = scale as usize;
        if digits.len() <= width {
            digits = format!("{}{}", "0".repeat(width + 1 - digits.len()), digits);
        }
        digits.insert(digits.len() - width, '.');
    }
    Ok(format!("{}{digits}", if negative { "-" } else { "" }))
}

pub(crate) fn unit_factor(unit: &TimeUnit) -> i64 {
    match unit {
        TimeUnit::Second => 1,
        TimeUnit::Millisecond => 1_000,
        TimeUnit::Microsecond => 1_000_000,
        TimeUnit::Nanosecond => 1_000_000_000,
    }
}

pub(crate) fn time(value: &Value, unit: &TimeUnit) -> Result<i64, Failure> {
    let factor = unit_factor(unit);
    let ticks = if let Some(ticks) = value.as_i64() {
        ticks
    } else {
        let text = value
            .as_str()
            .ok_or("Times require ISO time strings or integer ticks since midnight")?;
        let b = text.as_bytes();
        if b.len() < 8
            || b[2] != b':'
            || b[5] != b':'
            || ![0, 1, 3, 4, 6, 7].iter().all(|i| b[*i].is_ascii_digit())
        {
            return Err("Time must have HH:MM:SS with optional fractional seconds".into());
        }
        let number = |a: usize| i64::from(b[a] - b'0') * 10 + i64::from(b[a + 1] - b'0');
        let (h, m, s) = (number(0), number(3), number(6));
        if h >= 24 || m >= 60 || s >= 60 {
            return Err("Time must be within a day; leap seconds are unsupported".into());
        }
        let mut nanos = 0;
        if b.len() > 8 {
            if b[8] != b'.'
                || !(10..=18).contains(&b.len())
                || !b[9..].iter().all(u8::is_ascii_digit)
            {
                return Err("Time fraction must contain one through nine digits".into());
            }
            for digit in &b[9..] {
                nanos = nanos * 10 + i64::from(digit - b'0');
            }
            nanos *= 10_i64.pow((18 - b.len()) as u32);
        }
        if nanos % (1_000_000_000 / factor) != 0 {
            return Err("Time would lose precision at the column unit".into());
        }
        (h * 3600 + m * 60 + s) * factor + nanos / (1_000_000_000 / factor)
    };
    validate_time(ticks, factor)?;
    Ok(ticks)
}

pub(crate) fn validate_time(ticks: i64, factor: i64) -> Result<(), Failure> {
    if !(0..86_400 * factor).contains(&ticks) {
        return Err("Time ticks must be within a day".into());
    }
    Ok(())
}

pub(crate) fn dictionary_value_type(value: &DataType) -> bool {
    value.is_integer()
        || matches!(
            value,
            DataType::Utf8
                | DataType::LargeUtf8
                | DataType::Binary
                | DataType::LargeBinary
                | DataType::Float16
                | DataType::Float32
                | DataType::Float64
        )
}

pub(crate) fn map_fields(entries: &Field) -> Result<&Fields, Failure> {
    match entries.data_type() {
        DataType::Struct(fields)
            if !entries.is_nullable() && fields.len() == 2 && !fields[0].is_nullable() =>
        {
            Ok(fields)
        }
        _ => Err(
            "Map entries require a non-nullable struct containing a required key and a value"
                .into(),
        ),
    }
}

pub(crate) fn map(value: &mut Value, entries: &Field) -> Result<(), Failure> {
    let fields = map_fields(entries)?;
    let pairs = match value.take() {
        Value::Object(values) => values
            .into_iter()
            .map(|(key, value)| vec![Value::String(key), value])
            .collect(),
        Value::Array(values) => values
            .into_iter()
            .map(|value| {
                value
                    .as_array()
                    .filter(|p| p.len() == 2)
                    .cloned()
                    .ok_or_else(|| Failure::from("Map values must be key/value pairs"))
            })
            .collect::<Result<Vec<_>, _>>()?,
        _ => return Err("Maps require an object or list of key/value pairs".into()),
    };
    let mut keys = std::collections::HashSet::new();
    let mut values = Vec::with_capacity(pairs.len());
    for mut pair in pairs {
        for (value, field) in pair.iter_mut().zip(fields) {
            crate::value::normalize(value, field)?;
        }
        if !keys.insert(pair[0].to_string()) {
            return Err("Map keys must be unique".into());
        }
        values.push(Value::Object(
            fields
                .iter()
                .zip(pair)
                .map(|(field, value)| (field.name().clone(), value))
                .collect(),
        ));
    }
    *value = Value::Array(values);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codec::{batches_to_json, typed_record_batch};
    use crate::schema::definition::explicit_schema;
    use serde_json::json;

    #[test]
    fn decimal_storage_is_exact_at_precision_and_negative_scale_boundaries() {
        for (bits, precision, scale, input, expected) in [
            (128, 38, 0, "9".repeat(38), "9".repeat(38)),
            (256, 76, 0, "9".repeat(76), "9".repeat(76)),
            (128, 3, 2, "-0.0100".into(), "-0.01".into()),
            (
                128,
                1,
                -128,
                format!("1{}", "0".repeat(128)),
                format!("1{}", "0".repeat(128)),
            ),
            (256, 76, -2, "1200".into(), "1200".into()),
            (128, 3, -2, "0.0000".into(), "000".into()),
        ] {
            let schema = explicit_schema(&json!([{"name":"value", "type":format!("decimal{bits}"), "precision":precision, "scale":scale, "nullable":true}]).to_string()).unwrap();
            let batch =
                typed_record_batch(&json!([{"value":input}]).to_string(), schema.clone()).unwrap();
            let output = batches_to_json(vec![batch.clone()]).unwrap();
            assert_eq!(
                serde_json::from_str::<Value>(&output).unwrap(),
                json!([{"value":expected}])
            );
            assert_eq!(typed_record_batch(&output, schema).unwrap(), batch);
        }
        for value in ["0.001", "1000.00", ".1", "1.", "1e2", "--1", "１２"] {
            assert!(decimal(&json!(value), 5, 2).is_err(), "{value}");
        }
        assert!(decimal_type(0, 0, 38).is_err());
        assert!(decimal_type(39, 0, 38).is_err());
        assert!(decimal_type(2, 3, 38).is_err());
    }

    #[test]
    fn times_enforce_units_day_bounds_and_fractional_precision() {
        for (unit, maximum) in [
            (TimeUnit::Second, 86_399),
            (TimeUnit::Millisecond, 86_399_999),
            (TimeUnit::Microsecond, 86_399_999_999),
            (TimeUnit::Nanosecond, 86_399_999_999_999),
        ] {
            assert_eq!(time(&json!(maximum), &unit).unwrap(), maximum);
            assert!(time(&json!(maximum + 1), &unit).is_err());
            assert!(time(&json!(-1), &unit).is_err());
            assert!(time(&json!("23:59:60"), &unit).is_err());
            assert_eq!(super::unit(unit_name(&unit)).unwrap(), unit);
        }
        assert_eq!(
            time(&json!("00:00:00.000000001"), &TimeUnit::Nanosecond).unwrap(),
            1
        );
        assert!(time(&json!("00:00:00.000000001"), &TimeUnit::Microsecond).is_err());
        assert!(unit("minute").is_err());
    }
}
