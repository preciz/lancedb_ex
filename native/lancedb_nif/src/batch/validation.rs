//! Compile logical value constraints once, then inspect only reachable Arrow values.
use crate::error::Failure;
use arrow::row::{RowConverter, SortField};
use arrow_array::{
    cast::AsArray, downcast_dictionary_array, types::ArrowDictionaryKeyType, Array,
    DictionaryArray, FixedSizeListArray, LargeListArray, ListArray, MapArray, RecordBatch,
    StructArray,
};
use arrow_schema::{DataType, Field, SchemaRef};
use std::{collections::HashSet, ops::Range};

mod map_keys;
mod scalar;
use map_keys::{canonical_keys, key_type};
use scalar::Scalar;

pub(crate) struct Validation {
    schema: SchemaRef,
    columns: Vec<(usize, Constraint)>,
}

struct Constraint {
    nullable: bool,
    kind: Kind,
}

enum Kind {
    Nullability,
    Scalar(Scalar),
    Struct(Vec<(usize, Constraint)>),
    List(Box<Constraint>),
    Dictionary(Box<Constraint>),
    Blob,
    Json,
    Map {
        keys: RowConverter,
        entries: Option<Box<Constraint>>,
    },
}

impl Validation {
    pub(crate) fn prepare(schema: SchemaRef) -> Result<Self, Failure> {
        Self::prepare_checks(schema, true)
    }

    fn prepare_checks(schema: SchemaRef, writing: bool) -> Result<Self, Failure> {
        let columns = schema
            .fields()
            .iter()
            .enumerate()
            .filter_map(|(index, field)| {
                Constraint::prepare(field, writing)
                    .transpose()
                    .map(|child| child.map(|child| (index, child)))
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Self { schema, columns })
    }

    pub(crate) fn validate(&self, batch: &RecordBatch) -> Result<(), Failure> {
        if self.schema.fields() != batch.schema().fields() {
            return Err("Batch fields do not match destination schema".into());
        }
        for (index, constraint) in &self.columns {
            constraint
                .validate(batch.column(*index).as_ref(), 0..batch.num_rows())
                .map_err(|error| {
                    let message = format!("Column '{}': {error}", self.schema.field(*index).name());
                    error.context(message)
                })?;
        }
        Ok(())
    }
}

impl Constraint {
    fn prepare(field: &Field, writing: bool) -> Result<Option<Self>, Failure> {
        let data_type = field.data_type();
        let kind = if lance_arrow::json::is_json_field(field) {
            Some(Kind::Json)
        } else if writing && crate::blob::schema::is_input(field) {
            Some(Kind::Blob)
        } else if let Some(scalar) = Scalar::prepare(data_type).filter(|_| writing) {
            Some(Kind::Scalar(scalar))
        } else {
            match data_type {
                DataType::Struct(fields) => {
                    let children = fields
                        .iter()
                        .enumerate()
                        .filter_map(|(index, field)| {
                            Self::prepare(field, writing)
                                .transpose()
                                .map(|child| child.map(|child| (index, child)))
                        })
                        .collect::<Result<Vec<_>, _>>()?;
                    (!children.is_empty()).then_some(Kind::Struct(children))
                }
                DataType::List(item) | DataType::LargeList(item) => {
                    Self::prepare(item, writing)?.map(|child| Kind::List(Box::new(child)))
                }
                DataType::FixedSizeList(item, _) => {
                    // Vector parents may be null, but their elements never may.
                    let item = item.as_ref().clone().with_nullable(!writing);
                    Self::prepare(&item, writing)?.map(|child| Kind::List(Box::new(child)))
                }
                DataType::Dictionary(_, value) => {
                    // A required dictionary also rejects keys referring to null values.
                    let value = Field::new("value", value.as_ref().clone(), field.is_nullable());
                    Self::prepare(&value, writing)?.map(|child| Kind::Dictionary(Box::new(child)))
                }
                DataType::Map(entries, _) if !writing => {
                    Self::prepare(entries, false)?.map(|child| Kind::List(Box::new(child)))
                }
                DataType::Map(entries, _) => {
                    let fields = crate::extended_value::map_fields(entries)?;
                    Some(Kind::Map {
                        keys: RowConverter::new(vec![SortField::new(key_type(
                            fields[0].data_type(),
                        ))])
                        .map_err(Failure::from_error)?,
                        entries: Self::prepare(entries, writing)?.map(Box::new),
                    })
                }
                _ => None,
            }
        };
        Ok(kind
            .or_else(|| (writing && !field.is_nullable()).then_some(Kind::Nullability))
            .map(|kind| Self {
                nullable: !writing || field.is_nullable(),
                kind,
            }))
    }

    fn validate(&self, array: &dyn Array, range: Range<usize>) -> Result<(), Failure> {
        if !self.nullable && array.null_count() != 0 {
            for row in range.clone() {
                if array.is_null(row) {
                    return Err(format!("Row {row}: Required values cannot be null").into());
                }
            }
        }
        match &self.kind {
            Kind::Nullability => {}
            Kind::Json => {
                let values = array.as_binary::<i64>();
                for row in range.filter(|row| !values.is_null(*row)) {
                    crate::compat::json::validate(values.value(row))?;
                }
            }
            Kind::Scalar(scalar) => scalar.validate(array, range)?,
            Kind::Struct(children) => {
                let parent = array.as_struct();
                for (index, child) in children {
                    let values = parent.column(*index).as_ref();
                    if parent.null_count() == 0 {
                        child.validate(values, range.clone())?;
                    } else {
                        for row in range.clone().filter(|row| !parent.is_null(*row)) {
                            child.validate(values, row..row + 1)?;
                        }
                    }
                }
            }
            Kind::List(child) => {
                for row in range.filter(|row| !array.is_null(*row)) {
                    let (values, start, end): (&dyn Array, usize, usize) = match array.data_type() {
                        DataType::List(_) => {
                            let list = array.as_any().downcast_ref::<ListArray>().unwrap();
                            (
                                list.values().as_ref(),
                                list.value_offsets()[row] as usize,
                                list.value_offsets()[row + 1] as usize,
                            )
                        }
                        DataType::LargeList(_) => {
                            let list = array.as_any().downcast_ref::<LargeListArray>().unwrap();
                            (
                                list.values().as_ref(),
                                list.value_offsets()[row] as usize,
                                list.value_offsets()[row + 1] as usize,
                            )
                        }
                        DataType::FixedSizeList(_, size) => {
                            let list = array.as_any().downcast_ref::<FixedSizeListArray>().unwrap();
                            let start = list.value_offset(row) as usize;
                            (list.values().as_ref(), start, start + *size as usize)
                        }
                        DataType::Map(_, _) => {
                            let map = array.as_any().downcast_ref::<MapArray>().unwrap();
                            (
                                map.entries(),
                                map.value_offsets()[row] as usize,
                                map.value_offsets()[row + 1] as usize,
                            )
                        }
                        _ => unreachable!(),
                    };
                    child.validate(values, start..end)?;
                }
            }
            Kind::Dictionary(child) => {
                arrow_array::downcast_dictionary_array! {
                    array => validate_dictionary(array, child, range)?,
                    _ => unreachable!(),
                }
            }
            Kind::Blob => {
                let array = array.as_any().downcast_ref::<StructArray>().unwrap();
                let data = array.column(0);
                let uri = array.column(1).as_string::<i32>();
                for row in range.filter(|row| !array.is_null(*row)) {
                    if data.is_null(row) == uri.is_null(row) {
                        return Err("Blob values must supply exactly one of data or uri; use nil for a null blob".into());
                    }
                    if !uri.is_null(row) && uri.value(row).trim().is_empty() {
                        return Err("Blob URI must be a non-empty UTF-8 string".into());
                    }
                }
            }
            Kind::Map { keys, entries } => {
                let map = array.as_any().downcast_ref::<MapArray>().unwrap();
                for row in range.filter(|row| !map.is_null(*row)) {
                    let start = map.value_offsets()[row] as usize;
                    let end = map.value_offsets()[row + 1] as usize;
                    let key_array = canonical_keys(map.keys().slice(start, end - start))?;
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
                        entries.validate(map.entries(), start..end)?;
                    }
                }
            }
        }
        Ok(())
    }
}

fn validate_dictionary<K: ArrowDictionaryKeyType>(
    array: &DictionaryArray<K>,
    child: &Constraint,
    range: Range<usize>,
) -> Result<(), Failure> {
    for row in range {
        if let Some(index) = array.key(row) {
            child.validate(array.values().as_ref(), index..index + 1)?;
        }
    }
    Ok(())
}

pub(crate) fn validate(batch: &RecordBatch) -> Result<(), Failure> {
    Validation::prepare(batch.schema())?.validate(batch)
}

// Row conversion must guard the SDK's unchecked JSONB decoder, including IPC
// imports and existing datasets. Other output values retain Arrow's semantics.
pub(crate) fn validate_json(batch: &RecordBatch) -> Result<(), Failure> {
    Validation::prepare_checks(batch.schema(), false)?.validate(batch)
}

#[cfg(test)]
mod tests;
