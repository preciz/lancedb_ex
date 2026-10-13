//! SDK 0.40 batches downcast every floating query to float32. Keep its shared
//! scan for float32, and plan typed searches independently for float16/float64.
use arrow_schema::DataType;
use futures::{future::try_join_all, TryStreamExt};
use lance_datafusion::exec::{analyze_plan, execute_plan};
use lancedb::arrow::{SendableRecordBatchStream, SimpleRecordBatchStream};
use lancedb::datafusion::{
    common::ScalarValue,
    physical_plan::{
        displayable,
        expressions::{Column, Literal},
        projection::ProjectionExec,
        union::UnionExec,
        ExecutionPlan, PhysicalExpr,
    },
};
use lancedb::query::{ExecutableQuery, QueryExecutionOptions, VectorQuery};
use lancedb::table::{BaseTable, Table};
use std::sync::Arc;

pub(crate) struct Query {
    query: VectorQuery,
    fallback: Option<Arc<dyn BaseTable>>,
}

impl Query {
    pub(crate) fn new(table: &Table, query: VectorQuery, batch: bool) -> Self {
        let vectors = &query.current_request().query_vector;
        let fallback = (batch
            && vectors.len() > 1
            && matches!(
                vectors[0].data_type(),
                DataType::Float16 | DataType::Float64
            ))
        .then(|| table.base_table().clone());
        Self { query, fallback }
    }
}

impl ExecutableQuery for Query {
    async fn create_plan(
        &self,
        options: QueryExecutionOptions,
    ) -> lancedb::Result<Arc<dyn ExecutionPlan>> {
        let Some(table) = &self.fallback else {
            return self.query.create_plan(options).await;
        };
        let mut request = self.query.current_request().clone();
        let vectors = std::mem::take(&mut request.query_vector);
        let plans = try_join_all(vectors.into_iter().map(|vector| {
            let mut request = request.clone();
            request.query_vector = vec![vector];
            let options = options.clone();
            async move {
                table
                    .create_plan(
                        &lancedb::table::query::AnyQuery::VectorQuery(request),
                        options,
                    )
                    .await
            }
        }))
        .await?;
        let plans = plans
            .into_iter()
            .enumerate()
            .map(|(i, plan)| {
                let mut expressions: Vec<(Arc<dyn PhysicalExpr>, String)> = vec![(
                    Arc::new(Literal::new(ScalarValue::Int32(Some(i as i32)))),
                    "query_index".into(),
                )];
                expressions.extend(plan.schema().fields().iter().enumerate().map(|(i, field)| {
                    (
                        Arc::new(Column::new(field.name(), i)) as Arc<dyn PhysicalExpr>,
                        field.name().clone(),
                    )
                }));
                ProjectionExec::try_new(expressions, plan)
                    .map(|plan| Arc::new(plan) as Arc<dyn ExecutionPlan>)
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(UnionExec::try_new(plans)?)
    }

    async fn execute_with_options(
        &self,
        options: QueryExecutionOptions,
    ) -> lancedb::Result<SendableRecordBatchStream> {
        if self.fallback.is_none() {
            return self.query.execute_with_options(options).await;
        }
        let plan = self.create_plan(options).await?;
        let schema = plan.schema();
        let stream = execute_plan(plan, Default::default())?;
        Ok(Box::pin(SimpleRecordBatchStream {
            schema,
            stream: stream.map_err(Into::into),
        }))
    }

    async fn explain_plan(&self, verbose: bool) -> lancedb::Result<String> {
        if self.fallback.is_none() {
            return self.query.explain_plan(verbose).await;
        }
        let plan = self.create_plan(QueryExecutionOptions::default()).await?;
        let description = displayable(plan.as_ref()).indent(verbose).to_string();
        Ok(description)
    }

    async fn analyze_plan_with_options(
        &self,
        options: QueryExecutionOptions,
    ) -> lancedb::Result<String> {
        if self.fallback.is_none() {
            return self.query.analyze_plan_with_options(options).await;
        }
        let plan = self.create_plan(options).await?;
        Ok(analyze_plan(plan, Default::default()).await?)
    }
}
