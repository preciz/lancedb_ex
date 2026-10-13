//! LanceDB 0.40.0 supports scalar updates on blob tables, but not blob columns.
use crate::error::Failure;
use arrow_schema::{DataType, Field};

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

pub(crate) fn validate(field: &Field) -> Result<(), Failure> {
    if contains_blob(field) {
        return Err(Failure::new("unsupported", "LanceDB 0.40.0 cannot directly update columns containing blobs; use upsert or merge with complete records"));
    }
    Ok(())
}
