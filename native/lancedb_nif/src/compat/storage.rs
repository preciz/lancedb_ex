use crate::error::Failure;
use arrow_schema::DataType;
/// Lance 10 stores fixed-size-list item types but does not retain their field
/// metadata/name/nullability. Reject lossy table definitions before committing.
pub(crate) fn validate_storage_type(data_type: &DataType) -> Result<(), Failure> {
    match data_type {
        DataType::FixedSizeList(item, _) => {
            if item.name() != "item" || !item.is_nullable() || !item.metadata().is_empty() {
                return Err(Failure::new(
                    "unsupported",
                    "Lance 10 cannot persist custom vector item names, nullability or metadata",
                ));
            }
            validate_storage_type(item.data_type())
        }
        DataType::List(item) | DataType::LargeList(item) | DataType::Map(item, false) => {
            validate_storage_type(item.data_type())
        }
        DataType::Struct(fields) => {
            for field in fields {
                validate_storage_type(field.data_type())?;
            }
            Ok(())
        }
        _ => Ok(()),
    }
}

// Lance's writer checks child nulls independently of the parent struct's mask.
// Reject this unsupported representation before any write begins.
pub(crate) fn supports_null(data_type: &DataType) -> bool {
    match data_type {
        DataType::Struct(fields) => fields
            .iter()
            .all(|field| field.is_nullable() && supports_null(field.data_type())),
        _ => true,
    }
}

pub(crate) fn validate_cast_type(data_type: &DataType) -> Result<(), Failure> {
    let description = data_type.to_string();
    // Arrow 58's parser can retain escape characters in quoted field names.
    // A mismatched struct cast can silently replace those fields with nulls.
    let parsed = description
        .parse::<DataType>()
        .map_err(Failure::from_error)?;
    if &parsed != data_type {
        return Err("Nested update type cannot round trip through the Arrow SQL type parser; use upsert or merge_insert with complete records".into());
    }
    Ok(())
}

/// Lance 10's rewrite mapper addresses casts by leaf name, which is unsafe for nested fields.
pub(crate) fn validate_cast_path(has_conversion: bool, depth: usize) -> Result<(), Failure> {
    if has_conversion && depth > 1 {
        return Err("Lance 10 cannot convert nested field types safely; type conversions require a top-level column (including whole lists)".into());
    }
    Ok(())
}
