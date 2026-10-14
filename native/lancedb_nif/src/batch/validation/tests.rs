use super::*;
use arrow::buffer::{NullBuffer, OffsetBuffer};
use arrow_array::builder::{Float32Builder, MapBuilder, StringBuilder};
use arrow_array::ArrayRef;
use arrow_schema::{Field, Schema};
use futures::TryStreamExt;
use std::sync::Arc;

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
        })
        .unwrap();
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

#[test]
fn scalar_constraints_validate_the_physical_type_and_only_selected_values() {
    use arrow_array::*;
    macro_rules! cases {
        ($array:ty, $bad:expr, $good:expr) => {{
            let array: ArrayRef = Arc::new(<$array>::from(vec![Some($bad), None, Some($good)]));
            assert!(
                validate(&batch(array.clone())).is_err(),
                "{:?}",
                array.data_type()
            );
            assert!(
                validate(&batch(array.slice(1, 2))).is_ok(),
                "{:?}",
                array.data_type()
            );
        }};
    }
    cases!(Float32Array, f32::INFINITY, f32::MAX);
    cases!(Float32Array, f32::NAN, -f32::MAX);
    cases!(Float64Array, f64::NEG_INFINITY, f64::MAX);
    cases!(
        Float16Array,
        <arrow_array::types::Float16Type as arrow_array::types::ArrowPrimitiveType>::Native::INFINITY,
        <arrow_array::types::Float16Type as arrow_array::types::ArrowPrimitiveType>::Native::MAX
    );
    cases!(Date32Array, -719_163, -719_162);
    cases!(Date32Array, 2_932_897, 2_932_896);
    cases!(Date64Array, 86_400_001, 86_400_000);
    cases!(
        Date64Array,
        -719_163 * 86_400_000_i64,
        -719_162 * 86_400_000_i64
    );
    cases!(Time32SecondArray, 86_400, 86_399);
    cases!(Time32MillisecondArray, 86_400_000, 86_399_999);
    cases!(Time64MicrosecondArray, -1, 86_399_999_999);
    cases!(
        Time64NanosecondArray,
        86_400_000_000_000,
        86_399_999_999_999
    );
    cases!(TimestampSecondArray, -62_135_596_801, -62_135_596_800);
    cases!(
        TimestampMillisecondArray,
        253_402_300_800_000,
        253_402_300_799_999
    );
    cases!(
        TimestampMicrosecondArray,
        -62_135_596_800_000_001,
        -62_135_596_800_000_000
    );
    let nanos: ArrayRef = Arc::new(TimestampNanosecondArray::from(vec![i64::MIN, i64::MAX]));
    assert!(validate(&batch(nanos)).is_ok());
    for array in [
        Arc::new(
            Decimal128Array::from(vec![100_i128, 99])
                .with_precision_and_scale(2, -2)
                .unwrap(),
        ) as ArrayRef,
        Arc::new(
            Decimal256Array::from(vec![
                arrow::datatypes::i256::from_i128(-100),
                arrow::datatypes::i256::from_i128(-99),
            ])
            .with_precision_and_scale(2, 2)
            .unwrap(),
        ) as ArrayRef,
    ] {
        assert!(validate(&batch(array.clone())).is_err());
        assert!(validate(&batch(array.slice(1, 1))).is_ok());
    }
}

#[test]
fn scalar_constraints_skip_null_parents_and_unreferenced_dictionary_entries() {
    use arrow_array::{types::Int8Type, Float32Array, Int8Array};
    let values: ArrayRef = Arc::new(Float32Array::from(vec![f32::NAN, 1.0, f32::INFINITY]));
    let item = Arc::new(Field::new("item", DataType::Float32, true));
    let nulls = Some(NullBuffer::from(vec![false, true, false]));
    for array in [
        Arc::new(StructArray::new(
            vec![item.clone()].into(),
            vec![values.clone()],
            nulls.clone(),
        )) as ArrayRef,
        Arc::new(ListArray::new(
            item.clone(),
            OffsetBuffer::new(vec![0_i32, 1, 2, 3].into()),
            values.clone(),
            nulls.clone(),
        )),
        Arc::new(LargeListArray::new(
            item.clone(),
            OffsetBuffer::new(vec![0_i64, 1, 2, 3].into()),
            values.clone(),
            nulls.clone(),
        )),
        Arc::new(FixedSizeListArray::new(item, 1, values.clone(), nulls)),
        Arc::new(
            DictionaryArray::<Int8Type>::try_new(
                Int8Array::from(vec![None, Some(1), None]),
                values.clone(),
            )
            .unwrap(),
        ),
    ] {
        assert!(
            validate(&batch(array.clone())).is_ok(),
            "{:?}",
            array.data_type()
        );
        assert!(validate(&batch(array.slice(1, 1))).is_ok());
    }
    let dictionary: ArrayRef = Arc::new(
        DictionaryArray::<Int8Type>::try_new(Int8Array::from(vec![0, 1]), values).unwrap(),
    );
    assert!(validate(&batch(dictionary.clone())).is_err());
    assert!(validate(&batch(dictionary.slice(1, 1))).is_ok());
}

#[test]
fn vector_elements_are_required_even_when_the_arrow_item_is_nullable() {
    use arrow_array::Float32Array;
    let values: ArrayRef = Arc::new(Float32Array::from(vec![None, Some(1.0)]));
    let item = Arc::new(Field::new("item", DataType::Float32, true));
    let array: ArrayRef = Arc::new(FixedSizeListArray::new(item, 1, values, None));
    assert!(validate(&batch(array.clone())).is_err());
    assert!(validate(&batch(array.slice(1, 1))).is_ok());
}

#[test]
fn required_children_are_checked_only_when_their_parent_is_present() {
    use arrow_array::Int64Array;
    let values: ArrayRef = Arc::new(Int64Array::from(vec![None, Some(1)]));
    let item = Arc::new(Field::new("item", DataType::Int64, false));
    let array: ArrayRef = Arc::new(StructArray::new(
        vec![item].into(),
        vec![values],
        Some(NullBuffer::from(vec![false, true])),
    ));
    assert!(validate(&batch(array)).is_ok());
}

#[test]
fn blob_inputs_enforce_the_same_exclusive_payload_rule_as_rows() {
    use arrow_array::{LargeBinaryArray, StringArray};
    let field = lancedb::blob("payload", true);
    let DataType::Struct(fields) = field.data_type() else {
        panic!("blob must be a struct")
    };
    let data: ArrayRef = Arc::new(LargeBinaryArray::from(vec![
        Some(b"data".as_slice()),
        None,
        None,
        Some(b"data".as_slice()),
    ]));
    let uri: ArrayRef = Arc::new(StringArray::from(vec![
        None,
        Some("https://example.test/blob"),
        Some(" "),
        Some("https://example.test/blob"),
    ]));
    let array: ArrayRef = Arc::new(StructArray::new(fields.clone(), vec![data, uri], None));
    let schema = Arc::new(Schema::new(vec![field]));
    let batch = RecordBatch::try_new(schema.clone(), vec![array]).unwrap();
    let plan = Validation::prepare(schema).unwrap();
    assert!(plan.validate(&batch.slice(0, 2)).is_ok());
    assert!(plan.validate(&batch.slice(2, 1)).is_err());
    assert!(plan.validate(&batch.slice(3, 1)).is_err());
}

#[test]
fn json_payload_checks_respect_slices_and_null_parent_fields() {
    use arrow_array::LargeBinaryArray;
    let good = lance_arrow::json::encode_json("{\"a\":[1,null]}").unwrap();
    // Truncated string length and invalid UTF-8 would panic RawJsonb::to_string if decoded.
    let bad_truncated = [0x20, 0, 0, 0, 0x10, 0, 0, 99, b'a'];
    let bad_utf8 = [0x20, 0, 0, 0, 0x10, 0, 0, 1, 0xff];
    let field = Arc::new(lance_arrow::json::json_field("value", false));
    let values: ArrayRef = Arc::new(LargeBinaryArray::from(vec![
        Some(bad_truncated.as_slice()),
        Some(good.as_slice()),
        Some(bad_utf8.as_slice()),
    ]));
    let schema = Arc::new(Schema::new(vec![field.clone()]));
    let input = RecordBatch::try_new(schema, vec![values.clone()]).unwrap();
    assert!(validate(&input).is_err());
    assert!(crate::codec::batches_to_json(vec![input.clone()]).is_err());
    assert!(validate(&input.slice(1, 1)).is_ok());
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(
            &crate::codec::batches_to_json(vec![input.slice(1, 1)]).unwrap()
        )
        .unwrap(),
        serde_json::json!({
            "rows": [{"value": "{\"a\":[1,null]}"}],
            "binary_fields": {"value": "json"}
        })
    );

    let nulls = Some(NullBuffer::from(vec![false, true, false]));
    let keys: ArrayRef = Arc::new(arrow_array::StringArray::from(vec!["k0", "k1", "k2"]));
    let map_entries_fields: arrow_schema::Fields = vec![
        Arc::new(Field::new("key", DataType::Utf8, false)),
        field.clone(),
    ]
    .into();
    let map_entries_field = Arc::new(Field::new(
        "entries",
        DataType::Struct(map_entries_fields.clone()),
        false,
    ));
    let map_entries = StructArray::new(map_entries_fields, vec![keys, values.clone()], None);

    for parent in [
        Arc::new(StructArray::new(
            vec![field.clone()].into(),
            vec![values.clone()],
            nulls.clone(),
        )) as ArrayRef,
        Arc::new(ListArray::new(
            field.clone(),
            OffsetBuffer::new(vec![0_i32, 1, 2, 3].into()),
            values.clone(),
            nulls.clone(),
        )),
        Arc::new(LargeListArray::new(
            field.clone(),
            OffsetBuffer::new(vec![0_i64, 1, 2, 3].into()),
            values.clone(),
            nulls.clone(),
        )),
        Arc::new(FixedSizeListArray::new(field, 1, values, nulls.clone())),
        Arc::new(MapArray::new(
            map_entries_field,
            OffsetBuffer::new(vec![0_i32, 1, 2, 3].into()),
            map_entries,
            nulls,
            false,
        )),
    ] {
        let parent_batch = batch(parent);
        assert!(validate(&parent_batch).is_ok());
        assert!(crate::codec::batches_to_json(vec![parent_batch.clone()]).is_ok());
        let sliced = parent_batch.slice(1, 1);
        assert!(validate(&sliced).is_ok());
        assert!(crate::codec::batches_to_json(vec![sliced]).is_ok());
    }
}
