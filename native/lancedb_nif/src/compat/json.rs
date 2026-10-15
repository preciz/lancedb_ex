//! Validate the JSONB layout used by Lance 13 / jsonb 0.5 before SDK decoding.
//! Its decoder assumes valid UTF-8 and can panic on malformed numeric payloads.
//! Walk borrowed payloads; never materialize JSON rows or invoke that decoder.
use crate::error::Failure;
use arrow_array::{
    builder::{GenericStringBuilder, LargeBinaryBuilder},
    cast::AsArray,
    Array, ArrayRef, FixedSizeListArray, GenericStringArray, LargeBinaryArray, LargeListArray,
    ListArray, MapArray, OffsetSizeTrait, RecordBatch, StructArray,
};
use arrow_schema::{DataType, Field};
use std::{ops::Range, sync::Arc};

const JSONB_NULL: &[u8] = &[0x20, 0, 0, 0, 0, 0, 0, 0];

pub(crate) fn validate(bytes: &[u8]) -> Result<(), Failure> {
    container(bytes, 0).map_err(|message| Failure::from(format!("Invalid JSONB input: {message}")))
}

/// Replace unreachable non-null JSON/JSONB child slots beneath null parents or
/// outside list/map offsets before SDK conversion visits physical child arrays.
pub(crate) fn mask_unreachable(batch: &RecordBatch) -> Result<RecordBatch, Failure> {
    let schema = batch.schema();
    let range = 0..batch.num_rows();
    let mut changed = false;
    let mut columns = Vec::with_capacity(batch.num_columns());
    for (index, field) in schema.fields().iter().enumerate() {
        let column = batch.column(index);
        if is_nested_with_json(field) {
            if let Some(masked) = mask_array(field, column, std::slice::from_ref(&range))? {
                changed = true;
                columns.push(masked);
                continue;
            }
        }
        columns.push(column.clone());
    }
    if changed {
        RecordBatch::try_new(schema, columns).map_err(Failure::from_error)
    } else {
        Ok(batch.clone())
    }
}

fn has_any_json_fields(field: &Field) -> bool {
    lance_arrow::json::has_json_fields(field) || lance_arrow::json::has_arrow_json_fields(field)
}

fn is_nested_with_json(field: &Field) -> bool {
    !crate::json_value::is_json(field) && has_any_json_fields(field)
}

fn reachable_mask(len: usize, ranges: &[Range<usize>]) -> Vec<bool> {
    let mut reachable = vec![false; len];
    for range in ranges {
        reachable[range.clone()].fill(true);
    }
    reachable
}

fn mask_string_array<O: OffsetSizeTrait>(
    array: &GenericStringArray<O>,
    ranges: &[Range<usize>],
) -> Option<ArrayRef> {
    let reachable = reachable_mask(array.len(), ranges);
    if (0..array.len()).all(|i| reachable[i] || array.is_null(i) || array.value(i) == "null") {
        return None;
    }
    let mut builder =
        GenericStringBuilder::<O>::with_capacity(array.len(), array.value_data().len());
    for (i, is_reachable) in reachable.into_iter().enumerate() {
        if array.is_null(i) {
            builder.append_null();
        } else if is_reachable {
            builder.append_value(array.value(i));
        } else {
            builder.append_value("null");
        }
    }
    Some(Arc::new(builder.finish()))
}

fn mask_array(
    field: &Field,
    array: &ArrayRef,
    ranges: &[Range<usize>],
) -> Result<Option<ArrayRef>, Failure> {
    if lance_arrow::json::is_json_field(field) {
        let binary = array
            .as_any()
            .downcast_ref::<LargeBinaryArray>()
            .ok_or("Expected LargeBinaryArray for lance.json field")?;
        let reachable = reachable_mask(binary.len(), ranges);
        if (0..binary.len())
            .all(|i| reachable[i] || binary.is_null(i) || binary.value(i) == JSONB_NULL)
        {
            return Ok(None);
        }
        let mut builder =
            LargeBinaryBuilder::with_capacity(binary.len(), binary.value_data().len());
        for (i, is_reachable) in reachable.into_iter().enumerate() {
            if binary.is_null(i) {
                builder.append_null();
            } else if is_reachable {
                builder.append_value(binary.value(i));
            } else {
                builder.append_value(JSONB_NULL);
            }
        }
        return Ok(Some(Arc::new(builder.finish())));
    }
    if lance_arrow::json::is_arrow_json_field(field) {
        return match array.data_type() {
            DataType::Utf8 => Ok(mask_string_array::<i32>(array.as_string(), ranges)),
            DataType::LargeUtf8 => Ok(mask_string_array::<i64>(array.as_string(), ranges)),
            _ => Err("Expected Utf8 or LargeUtf8 array for arrow.json field".into()),
        };
    }
    match field.data_type() {
        DataType::Struct(fields) => {
            let struct_array = array
                .as_any()
                .downcast_ref::<StructArray>()
                .ok_or("Expected StructArray")?;
            let child_ranges;
            let active_ranges: &[Range<usize>] = if struct_array.null_count() == 0 {
                ranges
            } else {
                child_ranges = ranges
                    .iter()
                    .cloned()
                    .flatten()
                    .filter(|&row| !struct_array.is_null(row))
                    .map(|row| row..row + 1)
                    .collect::<Vec<_>>();
                &child_ranges
            };
            let mut changed = false;
            let mut new_columns = Vec::with_capacity(fields.len());
            for (child_field, column) in fields.iter().zip(struct_array.columns()) {
                if has_any_json_fields(child_field) {
                    if let Some(masked) = mask_array(child_field, column, active_ranges)? {
                        changed = true;
                        new_columns.push(masked);
                        continue;
                    }
                }
                new_columns.push(column.clone());
            }
            if changed {
                let masked = StructArray::try_new_with_length(
                    fields.clone(),
                    new_columns,
                    struct_array.nulls().cloned(),
                    struct_array.len(),
                )
                .map_err(Failure::from_error)?;
                Ok(Some(Arc::new(masked)))
            } else {
                Ok(None)
            }
        }
        DataType::List(item) => {
            if !has_any_json_fields(item) {
                return Ok(None);
            }
            let list_array = array
                .as_any()
                .downcast_ref::<ListArray>()
                .ok_or("Expected ListArray")?;
            let offsets = list_array.value_offsets();
            let child_ranges: Vec<_> = ranges
                .iter()
                .cloned()
                .flatten()
                .filter(|&row| !list_array.is_null(row))
                .map(|row| offsets[row] as usize..offsets[row + 1] as usize)
                .filter(|range| !range.is_empty())
                .collect();
            let Some(new_values) = mask_array(item, list_array.values(), &child_ranges)? else {
                return Ok(None);
            };
            let masked = ListArray::try_new(
                item.clone(),
                list_array.offsets().clone(),
                new_values,
                list_array.nulls().cloned(),
            )
            .map_err(Failure::from_error)?;
            Ok(Some(Arc::new(masked)))
        }
        DataType::LargeList(item) => {
            if !has_any_json_fields(item) {
                return Ok(None);
            }
            let list_array = array
                .as_any()
                .downcast_ref::<LargeListArray>()
                .ok_or("Expected LargeListArray")?;
            let offsets = list_array.value_offsets();
            let child_ranges: Vec<_> = ranges
                .iter()
                .cloned()
                .flatten()
                .filter(|&row| !list_array.is_null(row))
                .map(|row| offsets[row] as usize..offsets[row + 1] as usize)
                .filter(|range| !range.is_empty())
                .collect();
            let Some(new_values) = mask_array(item, list_array.values(), &child_ranges)? else {
                return Ok(None);
            };
            let masked = LargeListArray::try_new(
                item.clone(),
                list_array.offsets().clone(),
                new_values,
                list_array.nulls().cloned(),
            )
            .map_err(Failure::from_error)?;
            Ok(Some(Arc::new(masked)))
        }
        DataType::FixedSizeList(item, size) => {
            if !has_any_json_fields(item) {
                return Ok(None);
            }
            let list_array = array
                .as_any()
                .downcast_ref::<FixedSizeListArray>()
                .ok_or("Expected FixedSizeListArray")?;
            let step = *size as usize;
            let child_ranges: Vec<_> = ranges
                .iter()
                .cloned()
                .flatten()
                .filter(|&row| !list_array.is_null(row))
                .map(|row| {
                    let start = list_array.value_offset(row) as usize;
                    start..start + step
                })
                .filter(|range| !range.is_empty())
                .collect();
            let Some(new_values) = mask_array(item, list_array.values(), &child_ranges)? else {
                return Ok(None);
            };
            let masked = FixedSizeListArray::try_new_with_length(
                item.clone(),
                *size,
                new_values,
                list_array.nulls().cloned(),
                list_array.len(),
            )
            .map_err(Failure::from_error)?;
            Ok(Some(Arc::new(masked)))
        }
        DataType::Map(entries, keys_sorted) => {
            if !has_any_json_fields(entries) {
                return Ok(None);
            }
            let map_array = array
                .as_any()
                .downcast_ref::<MapArray>()
                .ok_or("Expected MapArray")?;
            let offsets = map_array.value_offsets();
            let child_ranges: Vec<_> = ranges
                .iter()
                .cloned()
                .flatten()
                .filter(|&row| !map_array.is_null(row))
                .map(|row| offsets[row] as usize..offsets[row + 1] as usize)
                .filter(|range| !range.is_empty())
                .collect();
            let entries_array: ArrayRef = Arc::new(map_array.entries().clone());
            let Some(new_entries) = mask_array(entries, &entries_array, &child_ranges)? else {
                return Ok(None);
            };
            let entries_struct = new_entries
                .as_any()
                .downcast_ref::<StructArray>()
                .ok_or("Expected MapArray entries to be StructArray")?
                .clone();
            let masked = MapArray::try_new(
                entries.clone(),
                map_array.offsets().clone(),
                entries_struct,
                map_array.nulls().cloned(),
                *keys_sorted,
            )
            .map_err(Failure::from_error)?;
            Ok(Some(Arc::new(masked)))
        }
        _ => Ok(None),
    }
}

fn container(bytes: &[u8], depth: usize) -> Result<(), &'static str> {
    if depth > 64 {
        return Err("nesting exceeds 64 levels");
    }
    let header = word(bytes)?;
    let count = (header & 0x1fff_ffff) as usize;
    let object = header & 0xe000_0000 == 0x4000_0000;
    let entries = match header & 0xe000_0000 {
        0x2000_0000 if count == 0 => 1,
        0x8000_0000 => count,
        0x4000_0000 => count.checked_mul(2).ok_or("entry count overflow")?,
        _ => return Err("invalid container header"),
    };
    let start = entries
        .checked_mul(4)
        .and_then(|n| n.checked_add(4))
        .ok_or("entry count overflow")?;
    let headers = bytes.get(4..start).ok_or("truncated entry headers")?;
    let mut payload = bytes.get(start..).ok_or("truncated payload")?;
    let mut previous_key = None;
    for (index, header) in headers.as_chunks::<4>().0.iter().enumerate() {
        let entry = word(header)?;
        // The pinned decoder supports lengths, not the JSONB offset flag.
        if entry & 0x8000_0000 != 0 {
            return Err("offset entries are unsupported");
        }
        let length = (entry & 0x0fff_ffff) as usize;
        let value = payload.get(..length).ok_or("truncated entry payload")?;
        payload = &payload[length..];
        let kind = entry & 0x7000_0000;
        if object && index < count {
            if kind != 0x1000_0000 {
                return Err("object keys must be strings");
            }
            let key = std::str::from_utf8(value).map_err(|_| "object key is not UTF-8")?;
            if previous_key.is_some_and(|previous| previous >= key) {
                return Err("object keys must be unique and sorted");
            }
            previous_key = Some(key);
        } else {
            match kind {
                0x0000_0000 | 0x3000_0000 | 0x4000_0000 if value.is_empty() => {}
                0x1000_0000 => {
                    std::str::from_utf8(value).map_err(|_| "string is not UTF-8")?;
                }
                0x2000_0000 => number(value)?,
                0x5000_0000 => container(value, depth + 1)?,
                _ => return Err("invalid JSON value tag or length"),
            }
        }
    }
    if !payload.is_empty() {
        return Err("trailing payload bytes");
    }
    Ok(())
}

fn word(bytes: &[u8]) -> Result<u32, &'static str> {
    let bytes = bytes.get(..4).ok_or("truncated header")?;
    Ok(u32::from_be_bytes(bytes.try_into().unwrap()))
}

fn number(bytes: &[u8]) -> Result<(), &'static str> {
    match (bytes.first(), bytes.len()) {
        (Some(0x00), 1) => Ok(()),
        (Some(0x40 | 0x50), 2 | 3 | 5 | 9) => Ok(()),
        (Some(0x60), 9) => {
            let value = f64::from_be_bytes(bytes[1..].try_into().unwrap());
            if value.is_finite() {
                Ok(())
            } else {
                Err("numbers must be finite")
            }
        }
        // Current decimal layouts and the decoder's two legacy precision-byte layouts.
        (Some(0x70), 10 | 18 | 19 | 34 | 35) => Ok(()),
        _ => Err("invalid or non-finite number"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encoder_output_is_valid_and_truncated_payloads_fail_without_panics() {
        for text in [
            "null",
            "true",
            "false",
            "0",
            "-9223372036854775808",
            "18446744073709551615",
            "1.5",
            "1e100",
            "\"snow 雪\"",
            "[]",
            "{}",
            "{\"a\":[1,null,{\"b\":\"雪\"}],\"z\":false}",
        ] {
            let bytes = lance_arrow::json::encode_json(text).unwrap();
            assert!(validate(&bytes).is_ok(), "{text}");
            for length in 0..bytes.len() {
                assert!(validate(&bytes[..length]).is_err(), "{text} at {length}");
            }
            let mut trailing = bytes;
            trailing.push(0);
            assert!(validate(&trailing).is_err());
        }
    }

    #[test]
    fn malformed_strings_keys_numbers_and_depth_are_rejected() {
        for bytes in [
            vec![0x20, 0, 0, 0, 0x10, 0, 0, 1, 0xff],
            vec![0x40, 0, 0, 1, 0x10, 0, 0, 1, 0, 0, 0, 0, 0xff],
            vec![0x20, 0, 0, 0, 0x20, 0, 0, 0],
            vec![0x20, 0, 0, 0, 0x20, 0, 0, 1, 0x10],
            vec![0x20, 0, 0, 0, 0x20, 0, 0, 1, 0x20],
            vec![0x20, 0, 0, 0, 0x20, 0, 0, 1, 0x60],
            vec![
                0x20, 0, 0, 0, 0x20, 0, 0, 9, 0x60, 0x7f, 0xf0, 0, 0, 0, 0, 0, 0,
            ],
            vec![
                0x40, 0, 0, 2, 0x10, 0, 0, 1, 0x10, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, b'a', b'a',
            ],
            vec![
                0x40, 0, 0, 2, 0x10, 0, 0, 1, 0x10, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, b'b', b'a',
            ],
            vec![0x20, 0, 0, 0, 0x80, 0, 0, 0],
            vec![0x80, 0, 0, 0xff],
        ] {
            assert!(validate(&bytes).is_err());
        }
        let mut nested = lance_arrow::json::encode_json("null").unwrap();
        for _ in 0..65 {
            let mut parent = vec![0x80, 0, 0, 1];
            parent.extend_from_slice(&(0x5000_0000 | nested.len() as u32).to_be_bytes());
            parent.extend_from_slice(&nested);
            nested = parent;
        }
        assert!(validate(&nested).is_err());
    }
}
