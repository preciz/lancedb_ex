//! Ordering shared by SDK queries and post-fusion results.
use crate::error::Failure;
use arrow::buffer::NullBuffer;
use arrow_array::{make_array, Array, ArrayRef, Float32Array, RecordBatch, StructArray};
use arrow_schema::SortOptions;
use lancedb::query::{ColumnOrdering, QueryBase};
use std::sync::Arc;

pub(crate) type Ordering = Vec<(String, bool, bool)>;

pub(crate) fn apply<T: QueryBase>(query: T, ordering: &Ordering) -> T {
    if ordering.is_empty() {
        return query;
    }
    query.order_by(Some(
        ordering
            .iter()
            .map(|(name, ascending, nulls_first)| ColumnOrdering {
                column_name: name.clone(),
                ascending: *ascending,
                nulls_first: *nulls_first,
            })
            .collect(),
    ))
}

pub(crate) fn sort(batch: RecordBatch, ordering: &Ordering) -> Result<RecordBatch, Failure> {
    if ordering.is_empty() {
        return Ok(batch);
    }
    let columns = ordering
        .iter()
        .map(|(name, ascending, nulls_first)| {
            Ok(arrow::compute::SortColumn {
                values: ordering_column(&batch, name)?,
                options: Some(SortOptions {
                    descending: !ascending,
                    nulls_first: *nulls_first,
                }),
            })
        })
        .collect::<Result<Vec<_>, Failure>>()?;
    let indices =
        arrow::compute::lexsort_to_indices(&columns, None).map_err(Failure::from_error)?;
    take(&batch, &indices)
}

fn ordering_column(batch: &RecordBatch, name: &str) -> Result<ArrayRef, Failure> {
    // Computed projections expose retained sort keys under their full path.
    if let Some(column) = batch.column_by_name(name) {
        return Ok(column.clone());
    }
    let path = lance::datatypes::parse_field_path(name).map_err(Failure::from_error)?;
    let missing = || format!("Unknown ordering column '{name}'");
    let mut column = batch.column_by_name(&path[0]).ok_or_else(missing)?.clone();
    for component in &path[1..] {
        let parent = column
            .as_any()
            .downcast_ref::<StructArray>()
            .ok_or_else(missing)?;
        let child = parent.column_by_name(component).ok_or_else(missing)?;
        column = if parent.nulls().is_some() {
            // A child value below any null parent must also sort as null.
            let nulls = NullBuffer::union(parent.nulls(), child.nulls());
            let data = child
                .to_data()
                .into_builder()
                .nulls(nulls)
                .build()
                .map_err(Failure::from_error)?;
            make_array(data)
        } else {
            child.clone()
        };
    }
    Ok(column)
}

pub(crate) fn take(batch: &RecordBatch, indices: &dyn Array) -> Result<RecordBatch, Failure> {
    let arrays = batch
        .columns()
        .iter()
        .map(|array| arrow::compute::take(array, indices, None))
        .collect::<Result<Vec<_>, _>>()
        .map_err(Failure::from_error)?;
    RecordBatch::try_new(batch.schema(), arrays).map_err(Failure::from_error)
}

/// Match LanceDB 0.37.1's rank-then-min/max or direct min/max normalization.
/// Scores are not inverted: vector distance is smaller-is-better, FTS larger-is-better.
pub(crate) fn normalize(
    batch: RecordBatch,
    column: &str,
    rank: bool,
) -> Result<RecordBatch, Failure> {
    let index = batch
        .schema()
        .index_of(column)
        .map_err(Failure::from_error)?;
    let scores = batch
        .column(index)
        .as_any()
        .downcast_ref::<Float32Array>()
        .ok_or("Expected float32 channel scores")?;
    if batch.num_rows() == 0 {
        return Ok(batch);
    }
    if scores.null_count() > 0 || scores.values().iter().any(|v| !v.is_finite()) {
        return Err("Channel scores must be finite and non-null".into());
    }
    let values: Vec<f32> = if rank {
        arrow::compute::kernels::rank::rank(
            scores,
            Some(SortOptions {
                descending: false,
                nulls_first: false,
            }),
        )
        .map_err(Failure::from_error)?
        .into_iter()
        .map(|v| v as f32)
        .collect()
    } else {
        scores.values().to_vec()
    };
    let min = values.iter().copied().fold(f32::INFINITY, f32::min);
    let max = values.iter().copied().fold(f32::NEG_INFINITY, f32::max);
    let range = if max - min < 10e-5 { max } else { max - min };
    let normalized = Float32Array::from_iter_values(values.into_iter().map(|v| {
        if range == 0.0 {
            v
        } else {
            (v - min) / range
        }
    }));
    if normalized.values().iter().any(|value| !value.is_finite()) {
        return Err("Channel score normalization overflowed float32".into());
    }
    let mut columns = batch.columns().to_vec();
    columns[index] = Arc::new(normalized);
    RecordBatch::try_new(batch.schema(), columns).map_err(Failure::from_error)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_schema::{DataType, Field, Schema};

    #[test]
    fn nested_sort_preserves_null_parents_and_quoted_path_segments() {
        use arrow_array::Int32Array;

        let leaf = Arc::new(Field::new("score.with.dot", DataType::Int32, true));
        let child = StructArray::new(
            vec![leaf].into(),
            vec![Arc::new(Int32Array::from(vec![100, -100, 2, 1]))],
            Some(NullBuffer::from(vec![true, false, true, true])),
        );
        let parent = StructArray::new(
            vec![Arc::new(Field::new(
                "rank` score",
                child.data_type().clone(),
                true,
            ))]
            .into(),
            vec![Arc::new(child)],
            Some(NullBuffer::from(vec![false, true, true, true])),
        );
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int32, false),
                Field::new("data root", parent.data_type().clone(), true),
            ])),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3, 4])),
                Arc::new(parent),
            ],
        )
        .unwrap();
        let ordering = vec![
            (
                "`data root`.`rank`` score`.`score.with.dot`".into(),
                true,
                false,
            ),
            ("id".into(), true, false),
        ];
        for (input, expected) in [
            (batch.clone(), vec![4, 3, 1, 2]),
            (batch.slice(1, 3), vec![4, 3, 2]),
        ] {
            let result = sort(input, &ordering).unwrap();
            let ids = result
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            assert_eq!(ids.values().as_ref(), expected);
        }
        assert!(ordering_column(&batch, "data root.missing").is_err());
        assert!(ordering_column(&batch, "id.missing").is_err());
    }

    fn batch(scores: Vec<f32>) -> RecordBatch {
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "score",
                DataType::Float32,
                false,
            )])),
            vec![Arc::new(Float32Array::from(scores))],
        )
        .unwrap()
    }

    fn values(batch: RecordBatch) -> Vec<f32> {
        batch
            .column(0)
            .as_any()
            .downcast_ref::<Float32Array>()
            .unwrap()
            .values()
            .to_vec()
    }

    #[test]
    fn normalization_matches_sdk_ranks_ties_and_near_constant_scores() {
        assert_eq!(
            values(normalize(batch(vec![0.0, 1.0, 9.0]), "score", false).unwrap()),
            vec![0.0, 1.0 / 9.0, 1.0]
        );
        assert_eq!(
            values(normalize(batch(vec![0.0, 1.0, 9.0]), "score", true).unwrap()),
            vec![0.0, 0.5, 1.0]
        );
        assert_eq!(
            values(normalize(batch(vec![3.0, 3.0, 10.0]), "score", true).unwrap()),
            vec![0.0, 0.0, 1.0]
        );
        assert_eq!(
            values(normalize(batch(vec![2.1, 2.1]), "score", false).unwrap()),
            vec![0.0, 0.0]
        );
        assert_eq!(
            values(normalize(batch(vec![0.0, 0.0]), "score", false).unwrap()),
            vec![0.0, 0.0]
        );
        assert_eq!(
            values(normalize(batch(vec![1.0, 0.9999999]), "score", false).unwrap()),
            vec![1.0 - 0.9999999, 0.0]
        );
        assert!(values(normalize(batch(vec![]), "score", true).unwrap()).is_empty());
    }

    #[test]
    fn normalization_rejects_invalid_scores_and_overflow() {
        for scores in [
            vec![f32::NAN],
            vec![f32::INFINITY],
            vec![-f32::MAX, f32::MAX],
        ] {
            assert!(normalize(batch(scores), "score", false).is_err());
        }
        assert!(normalize(batch(vec![1.0]), "missing", false).is_err());
    }
}
