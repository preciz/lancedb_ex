//! Calendar dates use Arrow's epoch units, restricted to ISO years 0001–9999.
use crate::error::Failure;
use arrow_schema::{DataType, TimeUnit};
use serde_json::Value;

pub(crate) fn value(value: &Value, data_type: &DataType) -> Result<i64, Failure> {
    let scale = if data_type == &DataType::Date64 {
        86_400_000
    } else {
        1
    };
    let ticks = if let Some(text) = value.as_str() {
        if text.len() != 10 || text.as_bytes()[4] != b'-' || text.as_bytes()[7] != b'-' {
            return Err("Dates must be YYYY-MM-DD strings or epoch integers".into());
        }
        let seconds = crate::timestamp::value(
            &Value::String(format!("{text}T00:00:00")),
            &TimeUnit::Second,
            None,
        )?;
        seconds / 86_400 * scale
    } else {
        value
            .as_i64()
            .ok_or("Dates must be YYYY-MM-DD strings or epoch integers")?
    };
    validate(ticks, scale)?;
    Ok(ticks)
}

pub(crate) fn validate(ticks: i64, scale: i64) -> Result<(), Failure> {
    if ticks % scale != 0 || !(-719_162 * scale..=2_932_896 * scale).contains(&ticks) {
        return Err(
            "Dates must be whole days in years 0001–9999; date64 uses midnight epoch milliseconds"
                .into(),
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn dates_validate_calendar_range_and_midnight() {
        for (text, days) in [
            ("0001-01-01", -719_162),
            ("1969-12-31", -1),
            ("9999-12-31", 2_932_896),
        ] {
            assert_eq!(value(&json!(text), &DataType::Date32).unwrap(), days);
            assert_eq!(
                value(&json!(text), &DataType::Date64).unwrap(),
                days * 86_400_000
            );
        }
        for bad in [
            json!("2023-02-29"),
            json!("2024-01-01T00:00:00"),
            json!(-719_163),
            json!(2_932_897),
            json!(1.5),
        ] {
            assert!(value(&bad, &DataType::Date32).is_err(), "{bad}");
        }
        assert!(value(&json!(1), &DataType::Date64).is_err());
        assert!(value(&json!(-1), &DataType::Date64).is_err());
    }
}
