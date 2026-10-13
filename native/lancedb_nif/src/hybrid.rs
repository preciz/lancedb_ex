use crate::error::Failure;
use crate::fts::TextQuery;
use crate::projection::{project, OutputProjection};
use crate::search_controls::ReadControls;
use arrow_array::Float32Array;
use arrow_array::RecordBatch;
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use futures::TryStreamExt;
use lancedb::query::{ExecutableQuery, QueryBase, Select};
use lancedb::rerankers::rrf::RRFReranker;
use lancedb::rerankers::Reranker;
use std::sync::Arc;

#[derive(serde::Deserialize)]
pub(crate) struct HybridControls {
    pub(crate) query: TextQuery,
    pub(crate) column: Option<String>,
    pub(crate) wand_factor: Option<f32>,
    pub(crate) rrf_k: f32,
    pub(crate) normalize: String,
    pub(crate) custom: bool,
}

/// The same final-result contract is used by RRF, callbacks and diagnostics.
pub(crate) struct HybridResultPlan {
    pub(crate) controls: HybridControls,
    pub(crate) limit: usize,
    pub(crate) read: ReadControls,
    pub(crate) projection: OutputProjection,
    pub(crate) order_by: crate::ranking::Ordering,
}

pub(crate) struct HybridPlan {
    pub(crate) vector: lancedb::query::VectorQuery,
    pub(crate) text: lancedb::query::Query,
    pub(crate) result: HybridResultPlan,
}

pub(crate) struct HybridCandidates {
    vector: Vec<RecordBatch>,
    text: Vec<RecordBatch>,
}

impl HybridPlan {
    pub(crate) async fn candidates(
        self,
        analyze: bool,
    ) -> Result<(HybridCandidates, HybridResultPlan, String), Failure> {
        let (vector, text, description) = if analyze {
            crate::diagnostics::hybrid_candidates(self.vector, self.text).await?
        } else {
            let (vector, text) = collect_channels(self.vector, self.text).await?;
            (vector, text, String::new())
        };
        Ok((HybridCandidates { vector, text }, self.result, description))
    }

    pub(crate) async fn output_schema(self) -> Result<SchemaRef, Failure> {
        let (vector, text) =
            futures::try_join!(self.vector.output_schema(), self.text.output_schema())
                .map_err(Failure::from_error)?;
        let (data, _) = aligned_channels(
            vec![RecordBatch::new_empty(vector)],
            vec![RecordBatch::new_empty(text)],
        )?;
        // Derive metadata and projection without executing either ranking algorithm.
        Ok(self
            .result
            .finish(with_scores(data, Float32Array::from(Vec::<f32>::new()))?)?
            .schema())
    }

    pub(crate) async fn explain(self, verbose: bool) -> Result<String, Failure> {
        hybrid_plan(self.vector, self.text, &self.result, verbose).await
    }
}

impl HybridCandidates {
    pub(crate) async fn fuse(self, plan: &HybridResultPlan) -> Result<RecordBatch, Failure> {
        let (vector, text) = aligned_channels(self.vector, self.text)?;
        let batch = RRFReranker::new(plan.controls.rrf_k)
            .rerank_hybrid("", vector, text)
            .await
            .map_err(|e| {
                let message = format!("Failed to fuse hybrid results: {e}");
                Failure::from_error(e).context(message)
            })?;
        plan.finish(batch)
    }

    pub(crate) fn callback_inputs(
        &self,
        plan: &HybridResultPlan,
    ) -> Result<(String, String), Failure> {
        Ok((
            candidate_json(&self.vector, "_distance", plan)?,
            candidate_json(&self.text, "_score", plan)?,
        ))
    }

    pub(crate) fn merge(self) -> Result<RecordBatch, Failure> {
        let (vector, text) = aligned_channels(self.vector, self.text)?;
        RRFReranker::default()
            .merge_results(vector, text)
            .map_err(Failure::from_error)
    }
}

impl HybridResultPlan {
    pub(crate) fn finish(&self, result: RecordBatch) -> Result<RecordBatch, Failure> {
        let result = crate::ranking::sort(result, &self.order_by)?;
        let offset = self.read.offset.min(result.num_rows());
        let result = result.slice(offset, (result.num_rows() - offset).min(self.limit));
        let indices: Vec<usize> = result
            .schema()
            .fields()
            .iter()
            .enumerate()
            .filter(|(_, field)| self.read.with_row_id || field.name() != "_rowid")
            .map(|(i, _)| i)
            .collect();
        project(
            result.project(&indices).map_err(Failure::from_error)?,
            &self.projection,
        )
    }
}

pub(crate) fn with_scores(
    batch: RecordBatch,
    scores: Float32Array,
) -> Result<RecordBatch, Failure> {
    let mut columns = batch.columns().to_vec();
    columns.push(Arc::new(scores));
    let mut fields = batch.schema().fields().to_vec();
    fields.push(Arc::new(Field::new(
        "_relevance_score",
        DataType::Float32,
        false,
    )));
    RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).map_err(Failure::from_error)
}

fn candidate_json(
    batches: &[RecordBatch],
    score: &str,
    plan: &HybridResultPlan,
) -> Result<String, Failure> {
    let schema = batches.first().ok_or("Missing channel schema")?.schema();
    let batch = arrow::compute::concat_batches(&schema, batches).map_err(Failure::from_error)?;
    let batch = crate::ranking::normalize(batch, score, plan.controls.normalize == "rank")?;
    let projection = plan.projection.as_ref().map(|columns| {
        let mut columns: Vec<_> = columns
            .iter()
            .filter(|(_, name)| !["_rowid", "_relevance_score"].contains(&name.as_str()))
            .cloned()
            .collect();
        columns.push(("_rowid".into(), "_rowid".into()));
        columns.push((score.into(), score.into()));
        columns
    });
    crate::codec::batches_to_json(vec![project(batch, &projection)?])
}

// Execute both channels and align their data columns, including empty results,
// before calling the SDK's RRF reranker, which only needs ranks and row IDs.
pub(crate) async fn hybrid_queries(
    table: &lancedb::Table,
    vector: lancedb::query::VectorQuery,
    hybrid: &HybridControls,
    selection: Option<Select>,
    limit: usize,
    filter: Option<String>,
    read: ReadControls,
) -> Result<(lancedb::query::VectorQuery, lancedb::query::Query), Failure> {
    let mut text = table
        .query()
        .full_text_search(
            hybrid
                .query
                .build(table, hybrid.column.as_deref())
                .await?
                .wand_factor(hybrid.wand_factor),
        )
        .with_row_id()
        .limit(limit);
    if let Some(filter) = filter {
        text = text.only_if(filter);
    }
    if let Some(selection) = selection {
        text = text.select(selection);
    }
    // Postfiltering controls vector candidate selection only. The text channel
    // keeps its SQL prefilter so text matches are not lost before fusion.
    let mut read = read;
    read.prefilter = true;
    Ok((vector.with_row_id(), read.apply(text).with_row_id()))
}

pub(crate) async fn hybrid_plan(
    vector: lancedb::query::VectorQuery,
    text: lancedb::query::Query,
    result: &HybridResultPlan,
    verbose: bool,
) -> Result<String, Failure> {
    let hybrid = &result.controls;
    let limit = result.limit;
    let read = result.read;
    let (vector, text) =
        futures::try_join!(vector.explain_plan(verbose), text.explain_plan(verbose)).map_err(
            |e| {
                let message = format!("Failed to explain hybrid search: {e}");
                Failure::from_error(e).context(message)
            },
        )?;
    let ranking = if hybrid.custom {
        "custom Elixir callback"
    } else {
        "RRF"
    };
    let fusion = if hybrid.custom {
        format!("custom Elixir reranking (limit={limit})")
    } else {
        format!(
            "reciprocal rank fusion (rrf_k={}, limit={limit})",
            hybrid.rrf_k
        )
    };
    let row_id = if read.with_row_id { "retain" } else { "remove" };
    Ok(format!(
        "Vector channel:\n{vector}\nFull-text channel:\n{text}\nFusion: {fusion}\nReranker: {ranking}, normalization={}\nFinal ordering: {:?}\nPagination: offset={}, candidate_limit={}\nOutput: align channel columns, add _relevance_score, {row_id} _rowid\n",
        hybrid.normalize, result.order_by, read.offset, limit + read.offset
    ))
}

pub(crate) async fn collect_channels(
    vector: lancedb::query::VectorQuery,
    text: lancedb::query::Query,
) -> Result<(Vec<RecordBatch>, Vec<RecordBatch>), Failure> {
    let (vector_stream, text_stream) = futures::try_join!(vector.execute(), text.execute())
        .map_err(|e| {
            let message = format!("Failed to execute hybrid search: {e}");
            Failure::from_error(e).context(message)
        })?;
    let vector_schema = vector_stream.schema();
    let text_schema = text_stream.schema();
    let (mut vector, mut text): (Vec<RecordBatch>, Vec<RecordBatch>) =
        futures::try_join!(vector_stream.try_collect(), text_stream.try_collect()).map_err(
            |e| {
                let message = format!("Failed to collect hybrid results: {e}");
                Failure::from_error(e).context(message)
            },
        )?;
    if vector.is_empty() {
        vector.push(RecordBatch::new_empty(vector_schema));
    }
    if text.is_empty() {
        text.push(RecordBatch::new_empty(text_schema));
    }
    Ok((vector, text))
}

pub(crate) fn aligned_channels(
    vector_batches: Vec<RecordBatch>,
    text_batches: Vec<RecordBatch>,
) -> Result<(RecordBatch, RecordBatch), Failure> {
    let sample = vector_batches
        .iter()
        .chain(text_batches.iter())
        .find(|batch| batch.num_rows() > 0)
        .or_else(|| vector_batches.first().or_else(|| text_batches.first()));
    let Some(sample) = sample else {
        return Err("Missing hybrid candidate schemas".into());
    };
    let names: Vec<String> = sample
        .schema()
        .fields()
        .iter()
        .filter(|field| !["_distance", "_score"].contains(&field.name().as_str()))
        .map(|field| field.name().clone())
        .collect();
    let indices = names
        .iter()
        .map(|name| sample.schema().index_of(name))
        .collect::<Result<Vec<_>, _>>()
        .map_err(Failure::from_error)?;
    let schema = sample
        .project(&indices)
        .map_err(Failure::from_error)?
        .schema();
    let align = |batches: Vec<RecordBatch>| -> Result<RecordBatch, Failure> {
        let batches = batches
            .into_iter()
            .filter(|b| b.num_rows() > 0)
            .map(|batch| {
                let indices = names
                    .iter()
                    .map(|name| batch.schema().index_of(name))
                    .collect::<Result<Vec<_>, _>>()?;
                batch.project(&indices)
            })
            .collect::<Result<Vec<_>, _>>()
            .map_err(Failure::from_error)?;
        arrow::compute::concat_batches(&schema, batches.iter()).map_err(Failure::from_error)
    };
    Ok((align(vector_batches)?, align(text_batches)?))
}
