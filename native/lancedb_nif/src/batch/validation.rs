//! Checks typed Arrow values for row decoding and batch writes.
use crate::error::Failure;
use arrow::row::{RowConverter, SortField};
use arrow_array::{
    types::{Float16Type, Float32Type, Float64Type},
    Array, ArrayRef, FixedSizeListArray, LargeListArray, ListArray, MapArray, PrimitiveArray,
    RecordBatch, StructArray,
};
use arrow_schema::DataType;
use std::{collections::HashSet, sync::Arc};

enum Constraint {
    Struct(Vec<(usize, Constraint)>),
    List(Box<Constraint>),
    Map {
        keys: RowConverter,
        entries: Option<Box<Constraint>>,
    },
}

impl Constraint {
    fn prepare(data_type: &DataType) -> Result<Option<Self>, Failure> {
        Ok(match data_type {
            DataType::Struct(fields) => {
                let children = fields
                    .iter()
                    .enumerate()
                    .filter_map(|(index, field)| {
                        Self::prepare(field.data_type())
                            .transpose()
                            .map(|child| child.map(|child| (index, child)))
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                if children.is_empty() {
                    None
                } else {
                    Some(Self::Struct(children))
                }
            }
            DataType::List(item) | DataType::LargeList(item) | DataType::FixedSizeList(item, _) => {
                Self::prepare(item.data_type())?.map(|child| Self::List(Box::new(child)))
            }
            DataType::Map(entries, _) => {
                let fields = crate::extended_value::map_fields(entries)?;
                Some(Self::Map {
                    keys: RowConverter::new(vec![SortField::new(key_type(fields[0].data_type()))])
                        .map_err(Failure::from_error)?,
                    entries: Self::prepare(entries.data_type())?.map(Box::new),
                })
            }
            _ => None,
        })
    }

    fn validate(&self, array: &dyn Array, row: usize) -> Result<(), Failure> {
        // Skip child values under a null parent and values outside a slice.
        if array.is_null(row) {
            return Ok(());
        }
        match self {
            Self::Struct(children) => {
                let array = array.as_any().downcast_ref::<StructArray>().unwrap();
                for (index, child) in children {
                    child.validate(array.column(*index).as_ref(), row)?;
                }
            }
            Self::List(child) => {
                let (values, start, end) = match array.data_type() {
                    DataType::List(_) => {
                        let array = array.as_any().downcast_ref::<ListArray>().unwrap();
                        (
                            array.values(),
                            array.value_offsets()[row] as usize,
                            array.value_offsets()[row + 1] as usize,
                        )
                    }
                    DataType::LargeList(_) => {
                        let array = array.as_any().downcast_ref::<LargeListArray>().unwrap();
                        (
                            array.values(),
                            array.value_offsets()[row] as usize,
                            array.value_offsets()[row + 1] as usize,
                        )
                    }
                    DataType::FixedSizeList(_, size) => {
                        let array = array.as_any().downcast_ref::<FixedSizeListArray>().unwrap();
                        let start = array.value_offset(row) as usize;
                        (array.values(), start, start + *size as usize)
                    }
                    _ => unreachable!(),
                };
                for index in start..end {
                    child.validate(values.as_ref(), index)?;
                }
            }
            Self::Map { keys, entries } => {
                let array = array.as_any().downcast_ref::<MapArray>().unwrap();
                let start = array.value_offsets()[row] as usize;
                let end = array.value_offsets()[row + 1] as usize;
                let key_array = canonical_keys(array.keys().slice(start, end - start))?;
                if key_array.logical_null_count() != 0 {
                    return Err("Map keys cannot be null".into());
                }
                let rows = keys
                    .convert_columns(&[key_array])
                    .map_err(Failure::from_error)?;
                let mut seen = HashSet::with_capacity(end - start);
                for key in rows.iter() {
                    if !seen.insert(key) {
                        return Err("Map keys must be unique after Arrow conversion".into());
                    }
                }
                if let Some(entries) = entries {
                    for index in start..end {
                        entries.validate(array.entries(), index)?;
                    }
                }
            }
        }
        Ok(())
    }
}

// Arrow row encoding supports lists but does not support maps.
// Use the map key buffers as lists without copying them.
// Keep dictionaries and nested fields.
fn key_type(data_type: &DataType) -> DataType {
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

fn canonical_keys(array: ArrayRef) -> Result<ArrayRef, Failure> {
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

pub(crate) fn validate(batch: &RecordBatch) -> Result<(), Failure> {
    for (field, array) in batch.schema().fields().iter().zip(batch.columns()) {
        if let Some(constraint) = Constraint::prepare(field.data_type())? {
            for row in 0..batch.num_rows() {
                constraint.validate(array.as_ref(), row).map_err(|error| {
                    let message = format!("Row {row} column '{}': {error}", field.name());
                    error.context(message)
                })?;
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::buffer::{NullBuffer, OffsetBuffer};
    use arrow_array::builder::{Float32Builder, MapBuilder, StringBuilder};
    use arrow_schema::{Field, Schema};
    use futures::TryStreamExt;

    fn maps() -> ArrayRef {
        let mut builder = MapBuilder::new(None, Float32Builder::new(), StringBuilder::new());
        for pairs in [vec![(1.0, "a"), (1.0, "b")], vec![(2.0, "c"), (3.0, "d")]] {
            for (key, value) in pairs {
                builder.keys().append_value(key);
                builder.values().append_value(value);
            }
            builder.append(true).unwrap();
        }
        Arc::new(builder.finish())
    }

    fn batch(array: ArrayRef) -> RecordBatch {
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "value",
                array.data_type().clone(),
                true,
            )])),
            vec![array],
        )
        .unwrap()
    }

    #[test]
    fn typed_batch_constraints_respect_slices_and_null_parents() {
        let maps = maps();
        assert!(validate(&batch(maps.clone())).is_err());
        assert!(validate(&batch(maps.slice(1, 1))).is_ok());
        let item = Arc::new(Field::new("item", maps.data_type().clone(), true));
        let nulls = Some(NullBuffer::from(vec![false, true]));
        let arrays: Vec<ArrayRef> = vec![
            Arc::new(StructArray::new(
                vec![item.clone()].into(),
                vec![maps.clone()],
                nulls.clone(),
            )),
            Arc::new(ListArray::new(
                item.clone(),
                OffsetBuffer::new(vec![0_i32, 1, 2].into()),
                maps.clone(),
                nulls.clone(),
            )),
            Arc::new(LargeListArray::new(
                item.clone(),
                OffsetBuffer::new(vec![0_i64, 1, 2].into()),
                maps.clone(),
                nulls.clone(),
            )),
            Arc::new(FixedSizeListArray::new(item, 1, maps, nulls)),
        ];
        for array in arrays {
            assert!(
                validate(&batch(array.clone())).is_ok(),
                "{:?}",
                array.data_type()
            );
            assert!(validate(&batch(array.slice(1, 1))).is_ok());
        }
    }

    #[test]
    fn native_batches_are_validated_before_the_writer_receives_them() {
        crate::resources::RUNTIME.block_on(async {
            let bad = batch(maps());
            let writer = crate::ingestion::worker::Writer::spawn(bad.schema(), |stream| async {
                let batches: Vec<RecordBatch> =
                    stream.try_collect().await.map_err(Failure::from_error)?;
                assert_eq!(batches.len(), 1);
                assert_eq!(batches[0].num_rows(), 1);
                Ok(42)
            });
            assert!(writer.push(bad.clone()).await.is_err());
            writer.push(bad.slice(1, 1)).await.unwrap();
            let result = writer.finish().await.unwrap();
            assert_eq!(result.inserted, 1);
            assert_eq!(result.version, 42);
        });
    }

    #[test]
    fn nested_map_keys_and_dictionary_keys_use_typed_values() {
        for key_type in [
            serde_json::json!({"type":"dictionary", "key":{"type":"int8"}, "value":{"type":"float32"}}),
            serde_json::json!({"type":"struct", "fields":[{"name":"key", "type":"float32", "nullable":true}]}),
            serde_json::json!({"type":"list", "item":{"name":"item", "type":"float32", "nullable":true}}),
            serde_json::json!({"type":"map", "item":{"name":"entries", "nullable":false, "type":"struct", "fields":[{"name":"key", "nullable":false, "type":"float32"}, {"name":"value", "nullable":true, "type":"string"}]}}),
        ] {
            let mut key = key_type.clone();
            key["name"] = "key".into();
            key["nullable"] = false.into();
            let schema = crate::schema::definition::explicit_schema(&serde_json::json!([
                {"name":"value", "nullable":true, "type":"map", "item":{"name":"entries", "nullable":false, "type":"struct", "fields":[key, {"name":"value", "nullable":true, "type":"string"}]}}
            ]).to_string()).unwrap();
            let wrap = |number: i64| match key_type["type"].as_str().unwrap() {
                "struct" => serde_json::json!({"key":number}),
                "list" => serde_json::json!([number]),
                "map" => serde_json::json!([[number, "key"]]),
                _ => serde_json::json!(number),
            };
            let rows =
                serde_json::json!([{"value":[[wrap(16_777_216), "a"], [wrap(16_777_217), "b"]]}]);
            let error = crate::codec::typed_record_batch(&rows.to_string(), schema).unwrap_err();
            assert!(error.message.contains("unique"), "{error}");
        }
    }
}
