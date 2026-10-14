//! Inspect physical Arrow values without JSON conversion or copied buffers.
use crate::error::Failure;
use arrow_array::{types::*, Array, PrimitiveArray};
use arrow_schema::DataType;
use std::ops::Range;

pub(super) enum Scalar {
    Float16,
    Float32,
    Float64,
    Date32,
    Date64,
    Time32(arrow_schema::TimeUnit, i64),
    Time64(arrow_schema::TimeUnit, i64),
    Timestamp(arrow_schema::TimeUnit, i64),
    Decimal128(u8),
    Decimal256(u8),
}

impl Scalar {
    pub(super) fn prepare(data_type: &DataType) -> Option<Self> {
        use crate::extended_value::unit_factor;
        Some(match data_type {
            DataType::Float16 => Self::Float16,
            DataType::Float32 => Self::Float32,
            DataType::Float64 => Self::Float64,
            DataType::Date32 => Self::Date32,
            DataType::Date64 => Self::Date64,
            DataType::Time32(unit) => Self::Time32(*unit, unit_factor(unit)),
            DataType::Time64(unit) => Self::Time64(*unit, unit_factor(unit)),
            DataType::Timestamp(unit, _) => Self::Timestamp(*unit, unit_factor(unit)),
            DataType::Decimal128(precision, _) => Self::Decimal128(*precision),
            DataType::Decimal256(precision, _) => Self::Decimal256(*precision),
            _ => return None,
        })
    }

    pub(super) fn validate(&self, array: &dyn Array, range: Range<usize>) -> Result<(), Failure> {
        macro_rules! primitive {
            ($type:ty, $check:expr) => {
                check::<$type>(array, range, $check)
            };
        }
        macro_rules! finite {
            ($type:ty) => {
                primitive!($type, |value| {
                    if value.is_finite() {
                        Ok(())
                    } else {
                        Err("Floating values must be finite".into())
                    }
                })
            };
        }
        match self {
            Self::Float16 => finite!(Float16Type),
            Self::Float32 => finite!(Float32Type),
            Self::Float64 => finite!(Float64Type),
            Self::Date32 => primitive!(Date32Type, |ticks| crate::date::validate(
                i64::from(ticks),
                1
            )),
            Self::Date64 => {
                primitive!(Date64Type, |ticks| crate::date::validate(ticks, 86_400_000))
            }
            Self::Time32(unit, factor) => {
                let validate =
                    |ticks| crate::extended_value::validate_time(i64::from(ticks), *factor);
                match unit {
                    arrow_schema::TimeUnit::Second => primitive!(Time32SecondType, validate),
                    arrow_schema::TimeUnit::Millisecond => {
                        primitive!(Time32MillisecondType, validate)
                    }
                    _ => unreachable!(),
                }
            }
            Self::Time64(unit, factor) => {
                let validate = |ticks| crate::extended_value::validate_time(ticks, *factor);
                match unit {
                    arrow_schema::TimeUnit::Microsecond => {
                        primitive!(Time64MicrosecondType, validate)
                    }
                    arrow_schema::TimeUnit::Nanosecond => {
                        primitive!(Time64NanosecondType, validate)
                    }
                    _ => unreachable!(),
                }
            }
            Self::Timestamp(unit, factor) => {
                let validate = |ticks| crate::timestamp::validate(ticks, *factor);
                use arrow_schema::TimeUnit;
                match unit {
                    TimeUnit::Second => primitive!(TimestampSecondType, validate),
                    TimeUnit::Millisecond => primitive!(TimestampMillisecondType, validate),
                    TimeUnit::Microsecond => primitive!(TimestampMicrosecondType, validate),
                    TimeUnit::Nanosecond => primitive!(TimestampNanosecondType, validate),
                }
            }
            Self::Decimal128(precision) => primitive!(Decimal128Type, |value| {
                Decimal128Type::validate_decimal_precision(value, *precision, 0)
                    .map_err(Failure::from_error)
            }),
            Self::Decimal256(precision) => primitive!(Decimal256Type, |value| {
                Decimal256Type::validate_decimal_precision(value, *precision, 0)
                    .map_err(Failure::from_error)
            }),
        }
    }
}

fn check<T: ArrowPrimitiveType>(
    array: &dyn Array,
    range: Range<usize>,
    validate: impl Fn(T::Native) -> Result<(), Failure>,
) -> Result<(), Failure> {
    let array = array.as_any().downcast_ref::<PrimitiveArray<T>>().unwrap();
    for row in range {
        if !array.is_null(row) {
            validate(array.value(row)).map_err(|error| {
                let message = format!("Row {row}: {error}");
                error.context(message)
            })?;
        }
    }
    Ok(())
}
