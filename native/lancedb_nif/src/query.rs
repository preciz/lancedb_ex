use crate::error::Failure;
mod builders;
mod request;
mod vector_input;
pub(crate) use request::QueryRequest;

use crate::execution::QueryOperation;
use crate::hybrid::HybridPlan;
use crate::projection::{project, OutputProjection};
use crate::resources::TableResource;
use arrow_array::RecordBatch;
use futures::{Stream, TryStreamExt};
use lancedb::query::{ExecutableQuery, Query, QueryExecutionOptions, TakeQuery, VectorQuery};
use rustler::{Atom, Env, ResourceArc, Term};
use std::pin::Pin;

pub(crate) type BatchStream = Pin<Box<dyn Stream<Item = Result<RecordBatch, Failure>> + Send>>;

pub(crate) enum PreparedQuery {
    Plain(Query, OutputProjection),
    Vector(VectorQuery, OutputProjection),
    Take(TakeQuery, OutputProjection),
    Hybrid(Box<HybridPlan>),
}

impl PreparedQuery {
    pub(crate) async fn explain(self, verbose: bool) -> Result<String, Failure> {
        match self {
            Self::Plain(query, _) => query
                .explain_plan(verbose)
                .await
                .map_err(Failure::from_error),
            Self::Vector(query, _) => query
                .explain_plan(verbose)
                .await
                .map_err(Failure::from_error),
            Self::Take(query, _) => query
                .explain_plan(verbose)
                .await
                .map_err(Failure::from_error),
            Self::Hybrid(plan) => plan.explain(verbose).await,
        }
    }

    pub(crate) async fn execute(self, batch_size: u32) -> Result<BatchStream, Failure> {
        match self {
            Self::Plain(query, projection) => execute_query(query, projection, batch_size).await,
            Self::Vector(query, projection) => execute_query(query, projection, batch_size).await,
            Self::Take(query, projection) => execute_query(query, projection, batch_size).await,
            Self::Hybrid(plan) => {
                let (candidates, result, _) = plan.candidates(false).await?;
                let batch = candidates.fuse(&result).await?;
                Ok(Box::pin(futures::stream::iter([Ok(batch)])))
            }
        }
    }
}

async fn execute_query(
    query: impl ExecutableQuery,
    projection: OutputProjection,
    batch_size: u32,
) -> Result<BatchStream, Failure> {
    let mut options = QueryExecutionOptions::default();
    options.max_batch_length = batch_size;
    let stream = query
        .execute_with_options(options)
        .await
        .map_err(Failure::from_error)?;
    Ok(Box::pin(stream.map_err(Failure::from_error).and_then(
        move |batch| {
            let result = project(batch, &projection);
            futures::future::ready(result)
        },
    )))
}

#[rustler::nif(schedule = "DirtyCpu")]
fn query_execute(
    env: Env<'_>,
    table: ResourceArc<TableResource>,
    request: String,
    operation: QueryOperation,
    reference: Term<'_>,
) -> Result<Atom, Failure> {
    let request: QueryRequest = serde_json::from_str(&request).map_err(Failure::from_error)?;
    let deadline = request.execution.deadline();
    crate::read_task::spawn(env, reference, deadline, async move {
        let query = request.prepare(&table.table).await?;
        match operation {
            QueryOperation::Explain => query.explain(false).await,
            QueryOperation::ExplainVerbose => query.explain(true).await,
            QueryOperation::Analyze => query.analyze().await,
            QueryOperation::Schema => Ok(crate::schema::schema_json(
                query.output_schema().await?.as_ref(),
            )),
        }
    })
}

#[rustler::nif(schedule = "DirtyCpu")]
fn count_rows(
    env: Env<'_>,
    table_res: ResourceArc<TableResource>,
    filter: Option<String>,
    reference: Term<'_>,
) -> Result<Atom, Failure> {
    crate::request::read(env, reference, async move {
        match table_res.table.count_rows(filter).await {
            Ok(count) => Ok(count),
            Err(e) => {
                let message = format!("Failed to count rows: {e}");
                Err(Failure::from_error(e).context(message))
            }
        }
    })
}
