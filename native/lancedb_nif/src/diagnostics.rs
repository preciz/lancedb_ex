//! Plan-only schema inspection and one-pass analysis of the actual query builders.
use crate::error::Failure;
use crate::projection::{project, OutputProjection};
use crate::query::PreparedQuery;
use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use futures::{FutureExt, TryStreamExt};
use lance_datafusion::exec::{execute_plan, format_plan};
use lancedb::query::{ExecutableQuery, QueryExecutionOptions};
use std::{panic::AssertUnwindSafe, time::Instant};

struct Analysis {
    description: String,
    batches: Vec<RecordBatch>,
}

// Retain candidate batches only for hybrid fusion. Plain scans discard batches
// as they are consumed. Unlike the upstream AnalyzeExec drain, propagate errors.
async fn run(
    query: impl ExecutableQuery,
    projection: &OutputProjection,
    retain: bool,
) -> Result<Analysis, Failure> {
    // Analysis drains plans directly, outside the cursor's panic boundary.
    // Contain upstream tokenizer panics for both plain and hybrid analysis.
    AssertUnwindSafe(async {
        let planning = Instant::now();
        let plan = query
            .create_plan(QueryExecutionOptions::default())
            .await
            .map_err(Failure::from_error)?;
        let planning_ms = planning.elapsed().as_secs_f64() * 1000.0;
        let executing = Instant::now();
        let mut stream = execute_plan(plan.clone(), Default::default()).map_err(Failure::from_error)?;
        let mut rows = 0usize;
        let mut batches = Vec::new();
        while let Some(batch) = stream.try_next().await.map_err(Failure::from_error)? {
            let batch = project(batch, projection)?;
            rows += batch.num_rows();
            if retain {
                batches.push(batch);
            }
        }
        drop(stream);
        if retain && batches.is_empty() {
            batches.push(RecordBatch::new_empty(plan.schema()));
        }
        let elapsed_ms = executing.elapsed().as_secs_f64() * 1000.0;
        Ok(Analysis {
            description: format!("{}Plan creation: elapsed_ms={planning_ms:.3}\nExecution: elapsed_ms={elapsed_ms:.3}\nOutput: rows={rows}\n", format_plan(plan)),
            batches,
        })
    })
    .catch_unwind()
    .await
    .unwrap_or_else(|_| Err(Failure::new("execution", "Native query analysis panicked")))
}

impl PreparedQuery {
    pub(crate) async fn analyze(self) -> Result<String, Failure> {
        match self {
            Self::Plain(query, projection) => Ok(run(query, &projection, false).await?.description),
            Self::Vector(query, projection) => {
                Ok(run(query, &projection, false).await?.description)
            }
            Self::Take(query, projection) => Ok(run(query, &projection, false).await?.description),
            Self::Hybrid(plan) => {
                let (candidates, result, description) = plan.candidates(true).await?;
                let fusion = Instant::now();
                let rows = candidates.fuse(&result).await?.num_rows();
                let elapsed_ms = fusion.elapsed().as_secs_f64() * 1000.0;
                Ok(format!(
                    "{description}Fusion: reciprocal rank fusion (rrf_k={}, limit={}, offset={})\nFusion and projection: elapsed_ms={elapsed_ms:.3}\nOutput: rows={rows}\n",
                    result.controls.rrf_k, result.limit, result.read.offset,
                ))
            }
        }
    }

    pub(crate) async fn output_schema(self) -> Result<SchemaRef, Failure> {
        let schema = match self {
            Self::Plain(query, projection) => output_schema(query, projection).await,
            Self::Vector(query, projection) => output_schema(query, projection).await,
            Self::Take(query, projection) => output_schema(query, projection).await,
            Self::Hybrid(plan) => plan.output_schema().await,
        }?;
        // SDK plan schemas describe stored JSONB, but execution emits arrow.json.
        // Apply the same output conversion to every source, including hybrid plans.
        let batch = lance_arrow::json::convert_lance_json_to_arrow(&RecordBatch::new_empty(schema))
            .map_err(Failure::from_error)?;
        Ok(batch.schema())
    }
}

async fn output_schema(
    query: impl ExecutableQuery,
    projection: OutputProjection,
) -> Result<SchemaRef, Failure> {
    let schema = query.output_schema().await.map_err(Failure::from_error)?;
    Ok(project(RecordBatch::new_empty(schema), &projection)?.schema())
}

pub(crate) async fn hybrid_candidates(
    vector: lancedb::query::VectorQuery,
    text: lancedb::query::Query,
) -> Result<(Vec<RecordBatch>, Vec<RecordBatch>, String), Failure> {
    let projection = OutputProjection::default();
    let (vector, text) =
        futures::try_join!(run(vector, &projection, true), run(text, &projection, true))?;
    Ok((
        vector.batches,
        text.batches,
        format!(
            "Vector channel:\n{}Full-text channel:\n{}",
            vector.description, text.description
        ),
    ))
}
