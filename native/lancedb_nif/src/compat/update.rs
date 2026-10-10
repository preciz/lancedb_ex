//! LanceDB 0.37.1 reads blob descriptors for updates but requires logical blob input.
use crate::error::Failure;
use arrow_schema::{DataType, Field, Schema};

fn contains_blob(field: &Field) -> bool {
    lancedb::blob::is_blob(field)
        || field.metadata().contains_key("lance-encoding:blob")
        || match field.data_type() {
            DataType::Struct(fields) => fields.iter().any(|field| contains_blob(field)),
            DataType::List(item)
            | DataType::LargeList(item)
            | DataType::FixedSizeList(item, _)
            | DataType::Map(item, _) => contains_blob(item),
            _ => false,
        }
}

pub(crate) fn validate(schema: &Schema) -> Result<(), Failure> {
    if schema.fields().iter().any(|field| contains_blob(field)) {
        return Err(Failure::new("unsupported", "LanceDB 0.37.1 cannot update rows in tables containing blob columns; use upsert or merge with complete records"));
    }
    Ok(())
}
