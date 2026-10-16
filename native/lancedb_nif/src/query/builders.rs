//! Convert validated requests into SDK builders; execution lives in query.rs.
use super::request::{QueryKind, QueryOptions, QueryRequest, SearchControls};
use super::vector_input::VectorInput;
use super::PreparedQuery;
use crate::error::Failure;
use crate::fts::TextQuery;
use crate::hybrid::{hybrid_queries, HybridPlan, HybridResultPlan};
use crate::projection::ResultPolicy;
use lancedb::query::{QueryBase, VectorQuery};
use lancedb::DistanceType;

impl QueryRequest {
    pub(crate) async fn prepare(self, table: &lancedb::Table) -> Result<PreparedQuery, Failure> {
        let options = self.options;
        match self.kind {
            QueryKind::Take {
                row_ids,
                row_offsets,
            } => prepare_take(table, options, row_ids, row_offsets).await,
            QueryKind::Scan { filter, limit } => prepare_scan(table, options, filter, limit).await,
            QueryKind::Text {
                text,
                wand_factor,
                column,
                limit,
                filter,
            } => prepare_text(table, options, text, column, limit, filter, wand_factor).await,
            QueryKind::Vector {
                input,
                limit,
                filter,
                distance_type,
                controls,
            } => {
                prepare_vector(
                    table,
                    options,
                    input,
                    limit,
                    filter,
                    distance_type,
                    *controls,
                )
                .await
            }
        }
    }
}

async fn prepare_take(
    table: &lancedb::Table,
    options: QueryOptions,
    row_ids: Option<Vec<u64>>,
    row_offsets: Option<Vec<u64>>,
) -> Result<PreparedQuery, Failure> {
    let mut query = match (row_ids, row_offsets) {
        (Some(ids), None) => table.take_row_ids(ids),
        (None, Some(offsets)) => table.take_offsets(offsets),
        _ => return Err("Specify exactly one of row_ids or row_offsets".into()),
    };
    let projection = options
        .projection
        .prepare(table, ResultPolicy::plain(options.read.with_row_id), &[])
        .await?;
    if let Some(selection) = projection.selection {
        query = query.select(selection);
    }
    if options.read.with_row_id {
        query = query.with_row_id();
    }
    Ok(PreparedQuery::Take(query, projection.output))
}

async fn prepare_scan(
    table: &lancedb::Table,
    options: QueryOptions,
    filter: Option<String>,
    limit: Option<usize>,
) -> Result<PreparedQuery, Failure> {
    let mut query = options.read.apply(table.query());
    if let Some(limit) = limit {
        query = query.limit(limit);
    }
    let projection = options
        .projection
        .prepare(
            table,
            ResultPolicy::plain(options.read.with_row_id),
            &options.order_by,
        )
        .await?;
    if let Some(filter) = filter {
        query = query.only_if(crate::compat::ngram::filter(filter)?);
    }
    if let Some(selection) = projection.selection {
        query = query.select(selection);
    }
    query = crate::ranking::apply(query, &options.order_by);

    Ok(PreparedQuery::Plain(query, projection.output))
}

async fn prepare_text(
    table: &lancedb::Table,
    options: QueryOptions,
    text: TextQuery,
    column: Option<String>,
    limit: usize,
    filter: Option<String>,
    wand_factor: Option<f32>,
) -> Result<PreparedQuery, Failure> {
    let fts = text
        .build(table, column.as_deref())
        .await?
        .wand_factor(wand_factor);
    let mut query = options
        .read
        .apply(table.query().full_text_search(fts))
        .limit(limit);
    if let Some(filter) = filter {
        query = query.only_if(crate::compat::ngram::filter(filter)?);
    }
    let projection = options
        .projection
        .prepare(
            table,
            ResultPolicy::text(options.read.with_row_id),
            &options.order_by,
        )
        .await?;
    if let Some(selection) = projection.selection {
        query = query.select(selection);
    }

    query = crate::ranking::apply(query, &options.order_by);
    Ok(PreparedQuery::Plain(query, projection.output))
}

async fn prepare_vector(
    table: &lancedb::Table,
    options: QueryOptions,
    input: VectorInput,
    limit: usize,
    filter: Option<String>,
    distance_type: Option<String>,
    controls: SearchControls,
) -> Result<PreparedQuery, Failure> {
    let filter = filter.map(crate::compat::ngram::filter).transpose()?;
    let batch = input.batch_singleton();
    let mut query = input
        .prepare(
            table,
            controls.vector_column.as_deref(),
            distance_type.as_deref(),
        )
        .await?;

    let mut read = options.read;
    let candidate_limit = if controls.hybrid.is_some() {
        read.offset = 0;
        limit + options.read.offset
    } else {
        limit
    };
    query = read.apply(query).limit(candidate_limit);
    if let Some(column) = &controls.vector_column {
        query = query.column(column);
    }
    let policy = ResultPolicy::vector(options.read.with_row_id, controls.hybrid.is_some());
    let mut projection = options
        .projection
        .prepare(table, policy, &options.order_by)
        .await?;
    if let Some(singleton) = batch {
        let schema = table.schema().await.map_err(Failure::from_error)?;
        projection = projection.batch(schema.as_ref(), singleton);
    }
    let selection = projection.selection.clone();
    if let Some(selection) = selection.clone() {
        query = query.select(selection);
    }
    query = controls.apply(query)?;

    if let Some(filter) = filter.clone() {
        query = query.only_if(filter);
    }

    if let Some(dt) = distance_type {
        let dist = match dt.to_lowercase().as_str() {
            "cosine" => DistanceType::Cosine,
            "dot" => DistanceType::Dot,
            "hamming" => DistanceType::Hamming,
            "l2" => DistanceType::L2,
            _ => return Err("Invalid distance".into()),
        };
        query = query.distance_type(dist);
    }

    if let Some(hybrid) = controls.hybrid {
        let (vector, text) = hybrid_queries(
            table,
            query,
            &hybrid,
            selection,
            candidate_limit,
            filter,
            read,
        )
        .await?;
        Ok(PreparedQuery::Hybrid(Box::new(HybridPlan {
            vector,
            text,
            result: HybridResultPlan {
                controls: hybrid,
                limit,
                read: options.read,
                projection: projection.output,
                order_by: options.order_by,
            },
        })))
    } else {
        query = crate::ranking::apply(query, &options.order_by);
        Ok(PreparedQuery::Vector(
            crate::compat::vector_query::Query::new(table, query, batch.is_some()),
            projection.output,
        ))
    }
}

impl SearchControls {
    fn apply(&self, mut query: VectorQuery) -> Result<VectorQuery, Failure> {
        if let Some(nprobes) = self.nprobes {
            query = query.nprobes(nprobes as usize);
        }
        if let Some(probes) = self.probes {
            query = probes.apply(query)?;
        }
        if let Some((lower, upper)) = self.distance_range {
            query = query.distance_range(lower, upper);
        }
        if let Some(mode) = self.approx_mode {
            query = query.approx_mode(mode);
        }
        if let Some(ef) = self.ef {
            query = query.ef(ef as usize);
        }
        if let Some(factor) = self.refine_factor {
            query = query.refine_factor(factor);
        }
        if self.bypass_vector_index {
            query = query.bypass_vector_index();
        }

        Ok(query)
    }
}
