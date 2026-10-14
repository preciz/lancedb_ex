//! Lossless timestamp literals shared by record writes and literal updates.
use crate::error::Failure;
use arrow_array::timezone::Tz;
use arrow_array::RecordBatch;
use arrow_schema::{DataType, Schema, TimeUnit};
use serde_json::Value;
use std::sync::Arc;

pub(crate) fn data_type(unit: &str, timezone: Option<String>) -> Result<DataType, Failure> {
    let unit = match unit {
        "second" => TimeUnit::Second,
        "millisecond" => TimeUnit::Millisecond,
        "microsecond" => TimeUnit::Microsecond,
        "nanosecond" => TimeUnit::Nanosecond,
        _ => return Err("Unsupported timestamp unit".into()),
    };
    parse_timezone(timezone.as_deref())?;
    Ok(DataType::Timestamp(unit, timezone.map(Into::into)))
}

fn parse_timezone(timezone: Option<&str>) -> Result<Tz, Failure> {
    timezone.unwrap_or("UTC").parse().map_err(|e| {
        let message = format!("Invalid timestamp timezone: {e}");
        Failure::from_error(e).context(message)
    })
}

pub(crate) fn value(
    value: &Value,
    unit: &TimeUnit,
    timezone: Option<&str>,
) -> Result<i64, Failure> {
    let tz = parse_timezone(timezone)?;
    let factor = crate::extended_value::unit_factor(unit);
    let ticks = if let Some(ticks) = value.as_i64() {
        ticks
    } else if let Some(text) = value.as_str() {
        if !iso_shape(text, timezone.is_some()) {
            return Err("Expected ISO 8601 date and time; timezone-bearing columns require an explicit offset, timezone-free columns forbid offsets".into());
        }
        let date = arrow_cast::parse::string_to_datetime(&tz, text).map_err(Failure::from_error)?;
        let nanos = i64::from(date.timestamp_subsec_nanos());
        if nanos >= 1_000_000_000 || nanos % (1_000_000_000 / factor) != 0 {
            return Err("Timestamp would lose precision at the column's unit (leap seconds are unsupported)".into());
        }
        i64::try_from(
            i128::from(date.timestamp()) * i128::from(factor)
                + i128::from(nanos / (1_000_000_000 / factor)),
        )
        .map_err(|_| "Timestamp overflows signed 64-bit storage")?
    } else {
        return Err(
            "Timestamp values must be ISO 8601 strings or signed 64-bit epoch integers".into(),
        );
    };

    validate(ticks, factor)?;
    Ok(ticks)
}

pub(crate) fn validate(ticks: i64, factor: i64) -> Result<(), Failure> {
    // Keep all accepted values readable and reusable as four-digit-year ISO strings.
    let seconds = ticks.div_euclid(factor);
    if !(-62_135_596_800..253_402_300_800).contains(&seconds) {
        return Err("Timestamp must be within years 0001 through 9999".into());
    }
    Ok(())
}

/// Render instants in UTC. Historical IANA offsets can include seconds, which
/// RFC3339 output rounds to minutes; emitting UTC preserves the exact instant.
pub(crate) fn utc_batch(batch: RecordBatch) -> Result<RecordBatch, Failure> {
    let schema = batch.schema();
    let fields: Vec<_> = schema
        .fields()
        .iter()
        .map(|field| {
            field
                .as_ref()
                .clone()
                .with_data_type(utc_type(field.data_type()))
        })
        .collect();
    if fields
        .iter()
        .zip(schema.fields())
        .all(|(field, original)| field == original.as_ref())
    {
        return Ok(batch);
    }
    let columns = fields
        .iter()
        .zip(batch.columns())
        .map(|(field, column)| {
            arrow_cast::cast(column, field.data_type()).map_err(Failure::from_error)
        })
        .collect::<Result<Vec<_>, _>>()?;
    RecordBatch::try_new(
        Arc::new(Schema::new_with_metadata(fields, schema.metadata().clone())),
        columns,
    )
    .map_err(Failure::from_error)
}

fn utc_type(data_type: &DataType) -> DataType {
    let child = |field: &Arc<arrow_schema::Field>| {
        Arc::new(
            field
                .as_ref()
                .clone()
                .with_data_type(utc_type(field.data_type())),
        )
    };
    match data_type {
        DataType::Timestamp(unit, Some(_)) => DataType::Timestamp(*unit, Some("UTC".into())),
        DataType::List(item) => DataType::List(child(item)),
        DataType::LargeList(item) => DataType::LargeList(child(item)),
        DataType::FixedSizeList(item, size) => DataType::FixedSizeList(child(item), *size),
        DataType::Struct(fields) => DataType::Struct(fields.iter().map(child).collect()),
        other => other.clone(),
    }
}

fn iso_shape(text: &str, zoned: bool) -> bool {
    let b = text.as_bytes();
    if b.len() < 19 || &b[..4] == b"0000" {
        return false;
    }
    for (i, byte) in b[..19].iter().enumerate() {
        let valid = match i {
            4 | 7 => *byte == b'-',
            10 => *byte == b'T',
            13 | 16 => *byte == b':',
            _ => byte.is_ascii_digit(),
        };
        if !valid {
            return false;
        }
    }
    let mut end = 19;
    if b.get(end) == Some(&b'.') {
        end += 1;
        let start = end;
        while b.get(end).is_some_and(u8::is_ascii_digit) {
            end += 1;
        }
        if !(1..=9).contains(&(end - start)) {
            return false;
        }
    }
    let suffix = &b[end..];
    if !zoned {
        return suffix.is_empty();
    }
    suffix == b"Z"
        || (suffix.len() == 6
            && matches!(suffix[0], b'+' | b'-')
            && suffix[3] == b':'
            && [1, 2, 4, 5].iter().all(|i| suffix[*i].is_ascii_digit())
            && &suffix[1..3] < b"24".as_slice()
            && &suffix[4..6] < b"60".as_slice())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codec::{batches_to_json, typed_record_batch};
    use arrow_schema::Field;
    use serde_json::json;

    #[test]
    fn nanosecond_extremes_round_trip_through_iso_json() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "at",
            data_type("nanosecond", Some("UTC".into())).unwrap(),
            true,
        )]));
        for ticks in [i64::MIN, -1, 0, 1, i64::MAX] {
            let batch =
                typed_record_batch(&json!([{"at": ticks}]).to_string(), schema.clone()).unwrap();
            let json = batches_to_json(vec![batch.clone()]).unwrap();
            assert_eq!(typed_record_batch(&json, schema.clone()).unwrap(), batch);
        }
    }

    #[test]
    fn units_check_precision_overflow_and_calendar_bounds() {
        for (unit, scale) in [
            (TimeUnit::Second, 1),
            (TimeUnit::Millisecond, 1_000),
            (TimeUnit::Microsecond, 1_000_000),
            (TimeUnit::Nanosecond, 1_000_000_000),
        ] {
            assert_eq!(
                value(&json!("1969-12-31T23:59:59Z"), &unit, Some("UTC")).unwrap(),
                -scale
            );
            assert_eq!(
                value(&json!("1970-01-01T01:00:00+01:00"), &unit, Some("UTC")).unwrap(),
                0
            );
            assert!(value(&json!(1.5), &unit, None).is_err());
            assert!(value(
                &json!("2024-01-01T00:00:00.0000000001Z"),
                &unit,
                Some("UTC")
            )
            .is_err());
            assert!(value(&json!("2016-12-31T23:59:60Z"), &unit, Some("UTC")).is_err());
        }
        assert!(value(
            &json!("1970-01-01T00:00:00.1Z"),
            &TimeUnit::Second,
            Some("UTC")
        )
        .is_err());
        assert!(value(
            &json!("1970-01-01T00:00:00.0001Z"),
            &TimeUnit::Millisecond,
            Some("UTC")
        )
        .is_err());
        assert!(value(
            &json!("1970-01-01T00:00:00.0000001Z"),
            &TimeUnit::Microsecond,
            Some("UTC")
        )
        .is_err());
        assert!(value(
            &json!("9999-01-01T00:00:00Z"),
            &TimeUnit::Nanosecond,
            Some("UTC")
        )
        .is_err());
        assert!(value(&json!(253_402_300_800_i64), &TimeUnit::Second, None).is_err());
        assert!(value(&json!(-62_135_596_801_i64), &TimeUnit::Second, None).is_err());
        assert!(value(&json!("0000-01-01T00:00:00"), &TimeUnit::Second, None).is_err());
        assert_eq!(
            value(&json!("9999-12-31T23:59:59"), &TimeUnit::Second, None).unwrap(),
            253_402_300_799
        );
    }

    #[test]
    fn historical_timezone_offsets_do_not_change_the_serialized_instant() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "at",
            data_type("second", Some("Europe/Paris".into())).unwrap(),
            true,
        )]));
        let json = r#"[{"at":"1900-01-01T00:00:00Z"},{"at":null}]"#;
        let batch = typed_record_batch(json, schema.clone()).unwrap();
        let output = batches_to_json(vec![batch.clone()]).unwrap();
        assert_eq!(
            serde_json::from_str::<Value>(&output).unwrap(),
            serde_json::from_str::<Value>(json).unwrap()
        );
        assert_eq!(typed_record_batch(&output, schema).unwrap(), batch);
    }
}
