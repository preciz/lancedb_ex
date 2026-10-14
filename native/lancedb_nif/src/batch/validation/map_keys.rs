use crate::error::Failure;
use arrow_array::{
    types::{Float16Type, Float32Type, Float64Type},
    ArrayRef, PrimitiveArray,
};
use arrow_schema::DataType;
use std::sync::Arc;

// Arrow row encoding supports lists but does not support maps.
// Use the map key buffers as lists without copying them.
// Keep dictionaries and nested fields.
pub(super) fn key_type(data_type: &DataType) -> DataType {
    let child = |field: &Arc<arrow_schema::Field>| {
        Arc::new(
            field
                .as_ref()
                .clone()
                .with_data_type(key_type(field.data_type())),
        )
    };
    match data_type {
        DataType::Map(item, _) | DataType::List(item) => DataType::List(child(item)),
        DataType::LargeList(item) => DataType::LargeList(child(item)),
        DataType::FixedSizeList(item, size) => DataType::FixedSizeList(child(item), *size),
        DataType::Struct(fields) => DataType::Struct(fields.iter().map(child).collect()),
        DataType::Dictionary(key, value) => {
            DataType::Dictionary(key.clone(), Box::new(key_type(value)))
        }
        other => other.clone(),
    }
}

pub(super) fn canonical_keys(array: ArrayRef) -> Result<ArrayRef, Failure> {
    macro_rules! float {
        ($type:ty) => {{
            let array = array
                .as_any()
                .downcast_ref::<PrimitiveArray<$type>>()
                .unwrap();
            // Arrow rows treat signed zeros as different values.
            // Map keys treat signed zeros as equal values.
            let zero =
                <<$type as arrow_array::types::ArrowPrimitiveType>::Native as Default>::default();
            Arc::new(array.unary::<_, $type>(|value| if value == zero { zero } else { value }))
                as ArrayRef
        }};
    }
    Ok(match array.data_type() {
        DataType::Float16 => float!(Float16Type),
        DataType::Float32 => float!(Float32Type),
        DataType::Float64 => float!(Float64Type),
        _ if array.data_type().is_nested()
            || matches!(array.data_type(), DataType::Dictionary(_, _)) =>
        {
            let data = array.to_data();
            let children = data
                .child_data()
                .iter()
                .map(|child| {
                    canonical_keys(arrow_array::make_array(child.clone()))
                        .map(|array| array.to_data())
                })
                .collect::<Result<Vec<_>, _>>()?;
            arrow_array::make_array(
                data.into_builder()
                    .data_type(key_type(array.data_type()))
                    .child_data(children)
                    .build()
                    .map_err(Failure::from_error)?,
            )
        }
        _ => array,
    })
}
