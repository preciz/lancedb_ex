use crate::error::Failure;
use arrow_array::RecordBatch;
use arrow_schema::{DataType, Field, Schema};
use std::sync::Arc;

/// Infer only the schema; all writes share the same validation and Arrow decoder.
pub(crate) fn inferred_record_batch(rows: Vec<serde_json::Value>) -> Result<RecordBatch, Failure> {
    let schema = infer_schema(&rows)?;
    // Inferred creation has always required a vector on every row.
    for field in schema.fields() {
        if matches!(field.data_type(), DataType::FixedSizeList(_, _))
            && rows
                .iter()
                .any(|row| row.get(field.name()).is_none_or(|value| value.is_null()))
        {
            return Err(format!("Missing or invalid array for key '{}'", field.name()).into());
        }
    }
    rows_to_record_batch(rows, schema)
}

fn infer_schema(rows: &[serde_json::Value]) -> Result<Arc<Schema>, Failure> {
    let first = rows
        .first()
        .ok_or("Cannot create batch from empty array")?
        .as_object()
        .ok_or("Each item in records must be a JSON object")?;
    if first.is_empty() {
        return Err("Schema must contain fields".into());
    }
    let fields = first
        .iter()
        .map(|(name, value)| {
            let data_type = match value {
                serde_json::Value::String(_) => DataType::Utf8,
                serde_json::Value::Bool(_) => DataType::Boolean,
                serde_json::Value::Number(n) if n.is_i64() || n.is_u64() => DataType::Int64,
                serde_json::Value::Number(_) => DataType::Float64,
                serde_json::Value::Array(values) if !values.is_empty() => {
                    let dimension =
                        i32::try_from(values.len()).map_err(|_| "Vector dimension is too large")?;
                    DataType::FixedSizeList(
                        Arc::new(Field::new("item", DataType::Float32, true)),
                        dimension,
                    )
                }
                _ => return Err(format!("Cannot infer supported type for column '{name}'").into()),
            };
            Ok(Field::new(name, data_type, true))
        })
        .collect::<Result<Vec<_>, Failure>>()?;
    Ok(Arc::new(Schema::new(fields)))
}

// Native codec tests retain JSON round trips without a separate input NIF.
#[cfg(test)]
pub(crate) fn typed_record_batch(json: &str, schema: Arc<Schema>) -> Result<RecordBatch, Failure> {
    let rows: Vec<serde_json::Value> = serde_json::from_str(json).map_err(Failure::from_error)?;
    rows_to_record_batch(rows, schema)
}

pub(crate) fn rows_to_record_batch(
    mut rows: Vec<serde_json::Value>,
    schema: Arc<Schema>,
) -> Result<RecordBatch, Failure> {
    for (index, row) in rows.iter_mut().enumerate() {
        let row = row.as_object_mut().ok_or("Records must be objects")?;
        if row.len() != schema.fields().len() {
            return Err(
                format!("Row {index} must contain every table column and no extra fields").into(),
            );
        }
        for field in schema.fields() {
            let value = row
                .get_mut(field.name())
                .ok_or_else(|| format!("Missing column '{}'", field.name()))?;
            crate::value::normalize(value, field).map_err(|e| {
                let message = format!("Row {index} column '{}': {e}", field.name());
                Failure::from_error(e).context(message)
            })?;
        }
    }
    if rows.is_empty() {
        return Ok(RecordBatch::new_empty(schema));
    }
    let mut decoder = arrow_json::ReaderBuilder::new(crate::row_adapter::schema(&schema, false))
        .build_decoder()
        .map_err(Failure::from_error)?;
    decoder.serialize(&rows).map_err(Failure::from_error)?;
    let batch = decoder
        .flush()
        .map_err(Failure::from_error)?
        .ok_or("No records decoded")?;
    let batch = crate::row_adapter::batch(batch, schema)?;
    crate::batch::validation::validate(&batch)?;
    Ok(batch)
}

pub(crate) fn batches_to_json(batches: Vec<RecordBatch>) -> Result<String, Failure> {
    let binary_fields = batches
        .first()
        .map(|batch| crate::binary::fields(batch.schema().fields()));
    let mut buf = Vec::new();
    {
        let mut writer = arrow_json::WriterBuilder::new()
            .with_explicit_nulls(true)
            .with_datetime_format("%Y-%m-%d".into())
            .build::<_, arrow_json::writer::JsonArray>(&mut buf);
        for batch in batches {
            crate::batch::validation::validate_json(&batch)?;
            let batch = lance_arrow::json::convert_lance_json_to_arrow(&batch)
                .map_err(Failure::from_error)?;
            let output_schema = crate::row_adapter::schema(&batch.schema(), true);
            let batch = crate::row_adapter::batch(batch, output_schema)?;
            let batch = crate::timestamp::utc_batch(batch)?;
            writer.write(&batch).map_err(|e| {
                let message = format!("Failed to write batch to JSON: {e}");
                Failure::from_error(e).context(message)
            })?;
        }
        writer.finish().map_err(|e| {
            let message = format!("Failed to finish JSON writer: {e}");
            Failure::from_error(e).context(message)
        })?;
    }
    let json = String::from_utf8(buf).map_err(|e| {
        let message = format!("Invalid UTF-8 in JSON output: {e}");
        Failure::from_error(e).context(message)
    })?;
    match binary_fields.filter(|fields| !fields.is_empty()) {
        Some(fields) => Ok(format!(
            "{{\"rows\":{json},\"binary_fields\":{}}}",
            serde_json::Value::Object(fields)
        )),
        None => Ok(json),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::definition::explicit_schema;
    use serde_json::json;

    #[test]
    fn inferred_and_explicit_batches_agree_on_values_and_schema() {
        let rows = json!([
            {"id": 1, "score": -900.7297297297297, "text": "雪 '", "active": true, "vector": [0.2, 1.0]},
            {"id": 2, "score": 2, "text": null, "active": null, "vector": [1.0, 0.0]}
        ]);
        let json = rows.to_string();
        let inferred = inferred_record_batch(serde_json::from_str(&json).unwrap()).unwrap();
        let explicit = typed_record_batch(&json, inferred.schema()).unwrap();
        assert_eq!(inferred, explicit);
        let score = inferred
            .column_by_name("score")
            .unwrap()
            .as_any()
            .downcast_ref::<arrow_array::Float64Array>()
            .unwrap();
        assert_eq!(score.value(0), -900.7297297297297);
    }

    #[test]
    fn inference_rejects_unsupported_first_values_and_incompatible_rows() {
        for json in [
            "[]",
            "[{}]",
            r#"[{"a":null}]"#,
            r#"[{"a":[]}]"#,
            r#"[{"a":{}}]"#,
            r#"[{"a":1},{"a":"1"}]"#,
            r#"[{"a":[1]},{"a":[1,2]}]"#,
            r#"[{"a":[1]},{"a":null}]"#,
            r#"[{"a":9223372036854775808}]"#,
            r#"[{"a":1},{"b":2}]"#,
        ] {
            assert!(
                inferred_record_batch(serde_json::from_str(json).unwrap()).is_err(),
                "{json}"
            );
        }
    }

    #[test]
    fn typed_decoder_preserves_nulls_and_rejects_invalid_vectors() {
        let schema = explicit_schema(
            r#"[
            {"name":"id","type":"int64","nullable":false,"dimension":null},
            {"name":"vector","type":"vector","nullable":true,"dimension":2}
        ]"#,
        )
        .unwrap();
        assert_eq!(
            typed_record_batch("[]", schema.clone()).unwrap().num_rows(),
            0
        );
        let batch = typed_record_batch(r#"[{"id":1,"vector":null}]"#, schema.clone()).unwrap();
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&batches_to_json(vec![batch]).unwrap())
                .unwrap(),
            json!([{"id":1,"vector":null}])
        );
        for json in [
            r#"[{"id":null,"vector":[1,2]}]"#,
            r#"[{"id":1,"vector":[1]}]"#,
            r#"[{"id":1,"vector":[null,2]}]"#,
            r#"[{"id":1,"vector":[1e100,2]}]"#,
            r#"[{"id":1}]"#,
            r#"[{"id":1,"vector":[1,2],"extra":0}]"#,
        ] {
            assert!(typed_record_batch(json, schema.clone()).is_err(), "{json}");
        }
    }
    #[test]
    fn nested_timestamp_arrays_preserve_instants_nulls_and_child_metadata() {
        let at = Arc::new(Field::new(
            "item",
            DataType::Timestamp(
                arrow_schema::TimeUnit::Nanosecond,
                Some("Europe/Paris".into()),
            ),
            true,
        ));
        let fields = vec![Field::new("times", DataType::List(at), true)];
        let schema = Arc::new(Schema::new(vec![Field::new(
            "data",
            DataType::Struct(fields.into()),
            true,
        )]));
        let input = json!([
            {"data": {"times": ["1900-01-01T00:00:00.000000001Z", null]}},
            {"data": {"times": []}},
            {"data": null}
        ])
        .to_string();
        let batch = typed_record_batch(&input, schema.clone()).unwrap();
        let output = batches_to_json(vec![batch.clone()]).unwrap();
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&output).unwrap(),
            serde_json::from_str::<serde_json::Value>(&input).unwrap()
        );
        assert_eq!(typed_record_batch(&output, schema).unwrap(), batch);
    }
}
