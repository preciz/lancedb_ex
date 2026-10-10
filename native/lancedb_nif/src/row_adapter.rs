//! Preserve physical Arrow schemas while adapting dictionaries/maps to the JSON row codec.
use crate::error::Failure;
use arrow_array::{Array, ArrayRef, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use std::sync::Arc;

pub(crate) fn schema(schema: &Schema, output: bool) -> Arc<Schema> {
    Arc::new(Schema::new_with_metadata(
        schema
            .fields()
            .iter()
            .map(|field| {
                field
                    .as_ref()
                    .clone()
                    .with_data_type(data_type(field.data_type(), output))
            })
            .collect::<Vec<_>>(),
        schema.metadata().clone(),
    ))
}

fn data_type(value: &DataType, output: bool) -> DataType {
    let child = |field: &Arc<Field>| {
        Arc::new(
            field
                .as_ref()
                .clone()
                .with_data_type(data_type(field.data_type(), output)),
        )
    };
    match value {
        DataType::Dictionary(_, value) => data_type(value, output),
        DataType::Map(entries, _) => DataType::List(child(entries)),
        DataType::List(item) => DataType::List(child(item)),
        DataType::LargeList(item) => DataType::LargeList(child(item)),
        DataType::FixedSizeList(item, size) => DataType::FixedSizeList(child(item), *size),
        DataType::Struct(fields) => DataType::Struct(fields.iter().map(child).collect()),
        DataType::Duration(_) if output => DataType::Int64,
        DataType::Decimal128(_, _) | DataType::Decimal256(_, _) if output => DataType::Utf8,
        other => other.clone(),
    }
}

pub(crate) fn batch(batch: RecordBatch, schema: Arc<Schema>) -> Result<RecordBatch, Failure> {
    if batch.schema() == schema {
        return Ok(batch);
    }
    let columns = batch
        .columns()
        .iter()
        .zip(schema.fields())
        .map(|(array, field)| adapt(array.clone(), field.data_type()))
        .collect::<Result<Vec<_>, _>>()?;
    RecordBatch::try_new(schema, columns).map_err(Failure::from_error)
}

fn adapt(array: ArrayRef, target: &DataType) -> Result<ArrayRef, Failure> {
    if array.data_type() == target {
        return Ok(array);
    }
    if target == &DataType::Utf8
        && matches!(
            array.data_type(),
            DataType::Decimal128(_, _) | DataType::Decimal256(_, _)
        )
    {
        return strict_cast(&array, target);
    }
    if let DataType::Dictionary(_, value) = array.data_type() {
        return adapt(strict_cast(&array, value)?, target);
    }
    if let DataType::Dictionary(_, value) = target {
        return strict_cast(&adapt(array, value)?, target);
    }
    let children = match target {
        DataType::Struct(fields) => fields.iter().map(|field| field.data_type()).collect(),
        DataType::List(item)
        | DataType::LargeList(item)
        | DataType::FixedSizeList(item, _)
        | DataType::Map(item, _) => vec![item.data_type()],
        _ => Vec::new(),
    };
    let data = array.to_data();
    let children = data
        .child_data()
        .iter()
        .zip(children)
        .map(|(child, target)| {
            adapt(arrow_array::make_array(child.clone()), target).map(|child| child.to_data())
        })
        .collect::<Result<Vec<_>, _>>()?;
    let data = data
        .into_builder()
        .data_type(target.clone())
        .child_data(children)
        .build()
        .map_err(Failure::from_error)?;
    Ok(arrow_array::make_array(data))
}

fn strict_cast(array: &ArrayRef, target: &DataType) -> Result<ArrayRef, Failure> {
    arrow_cast::cast_with_options(
        array,
        target,
        &arrow_cast::CastOptions {
            safe: false,
            ..Default::default()
        },
    )
    .map_err(Failure::from_error)
}
