use crate::analyzer::{fts_builder, saved_analyzer, FtsAnalyzer};
use crate::error::Failure;
use crate::resources::TableResource;
use lancedb::index::scalar::{
    BooleanQuery, BoostQuery, FtsQuery, FullTextSearchQuery, MatchQuery, MultiMatchQuery, Operator,
    PhraseQuery,
};
use lancedb::index::IndexType;
use rustler::{Atom, Env, ResourceArc, Term};

#[derive(serde::Deserialize)]
#[serde(untagged)]
pub(crate) enum TextQuery {
    Text(String),
    Structured(StructuredQuery),
}

#[derive(serde::Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum StructuredQuery {
    Match {
        text: String,
        column: Option<String>,
        operator: String,
        boost: f32,
        fuzziness: u32,
        max_expansions: usize,
        prefix_length: u32,
    },
    Phrase {
        text: String,
        column: Option<String>,
        slop: u32,
    },
    MultiMatch {
        text: String,
        columns: Vec<String>,
        boosts: Vec<f32>,
        operator: String,
    },
    Boolean {
        must: Vec<Self>,
        should: Vec<Self>,
        must_not: Vec<Self>,
    },
    Boost {
        positive: Box<Self>,
        negative: Box<Self>,
        negative_boost: f32,
    },
}

impl TextQuery {
    pub(crate) async fn build(
        &self,
        table: &lancedb::Table,
        column: Option<&str>,
    ) -> Result<FullTextSearchQuery, Failure> {
        match self {
            Self::Text(text) => {
                let query = FullTextSearchQuery::new(text.clone());
                match column {
                    Some(column) => query
                        .with_column(column.into())
                        .map_err(Failure::from_error),
                    None => Ok(query),
                }
            }
            Self::Structured(query) => {
                let mut column = column.map(str::to_owned);
                if column.is_none() && query.needs_column() {
                    // Resolve defaults ourselves: the SDK can lose match options
                    // when expanding implicit columns and overlooks must_not leaves
                    // in its missing-column check. Explicit leaves remain unchanged.
                    let mut columns: Vec<String> = table
                        .list_indices()
                        .await
                        .map_err(Failure::from_error)?
                        .into_iter()
                        .filter(|i| i.index_type == IndexType::FTS)
                        .flat_map(|i| i.columns)
                        .collect();
                    columns.sort();
                    columns.dedup();
                    if columns.len() != 1 {
                        return Err("Structured FTS queries require :text_column or per-query columns unless exactly one text column is indexed".into());
                    }
                    column = columns.pop();
                }
                Ok(FullTextSearchQuery::new_query(
                    query.build(column.as_deref())?,
                ))
            }
        }
    }
}

impl StructuredQuery {
    fn needs_column(&self) -> bool {
        match self {
            Self::Match { column, .. } | Self::Phrase { column, .. } => column.is_none(),
            Self::MultiMatch { .. } => false,
            Self::Boolean {
                must,
                should,
                must_not,
            } => must
                .iter()
                .chain(should)
                .chain(must_not)
                .any(Self::needs_column),
            Self::Boost {
                positive, negative, ..
            } => positive.needs_column() || negative.needs_column(),
        }
    }

    fn build(&self, default_column: Option<&str>) -> Result<FtsQuery, Failure> {
        let column =
            |name: &Option<String>| name.clone().or_else(|| default_column.map(str::to_owned));
        let result = match self {
            Self::Match {
                text,
                column: name,
                operator,
                boost,
                fuzziness,
                max_expansions,
                prefix_length,
            } => FtsQuery::Match(
                MatchQuery::new(text.clone())
                    .with_column(column(name))
                    .with_operator(
                        Operator::try_from(operator.as_str()).map_err(Failure::from_error)?,
                    )
                    .with_boost(*boost)
                    .with_fuzziness(Some(*fuzziness))
                    .with_max_expansions(*max_expansions)
                    .with_prefix_length(*prefix_length),
            ),
            Self::Phrase {
                text,
                column: name,
                slop,
            } => FtsQuery::Phrase(
                PhraseQuery::new(text.clone())
                    .with_column(column(name))
                    .with_slop(*slop),
            ),
            Self::MultiMatch {
                text,
                columns,
                boosts,
                operator,
            } => {
                // Use the builders: upstream MultiMatch serde omits the operator.
                let query = MultiMatchQuery::try_new(text.clone(), columns.clone())
                    .and_then(|q| q.try_with_boosts(boosts.clone()))
                    .map_err(Failure::from_error)?
                    .with_operator(
                        Operator::try_from(operator.as_str()).map_err(Failure::from_error)?,
                    );
                FtsQuery::MultiMatch(query)
            }
            Self::Boolean {
                must,
                should,
                must_not,
            } => {
                let children = |nodes: &[Self]| {
                    nodes
                        .iter()
                        .map(|q| q.build(default_column))
                        .collect::<Result<Vec<_>, _>>()
                };
                if must.is_empty() && should.is_empty() {
                    return Err("Boolean FTS queries require a positive clause".into());
                }
                FtsQuery::Boolean(BooleanQuery {
                    must: children(must)?,
                    should: children(should)?,
                    must_not: children(must_not)?,
                })
            }
            Self::Boost {
                positive,
                negative,
                negative_boost,
            } => FtsQuery::Boost(BoostQuery::new(
                positive.build(default_column)?,
                negative.build(default_column)?,
                Some(*negative_boost),
            )),
        };
        Ok(result)
    }
}

fn tokens_json(
    text: &str,
    builder: &lancedb::index::scalar::FtsIndexBuilder,
) -> Result<String, Failure> {
    let mut tokenizer = builder.build().map_err(Failure::from_error)?;
    let tokens =
        lance_index::scalar::inverted::query::try_collect_query_tokens(text, &mut tokenizer)
            .map_err(Failure::from_error)?;
    Ok(serde_json::Value::Array(
        (0..tokens.len())
            .map(|i| serde_json::json!({"text": tokens.get_token(i), "position": tokens.position(i)}))
            .collect(),
    )
    .to_string())
}

#[rustler::nif(schedule = "DirtyCpu")]
fn tokenize(text: String, analyzer: String) -> Result<String, Failure> {
    let analyzer: FtsAnalyzer = serde_json::from_str(&analyzer).map_err(Failure::from_error)?;
    let builder = fts_builder(Some(analyzer))?;
    crate::compat::fts::tokenize(|| tokens_json(&text, &builder))?
}

#[rustler::nif(schedule = "DirtyCpu")]
fn tokenize_table(
    env: Env<'_>,
    table: ResourceArc<TableResource>,
    text: String,
    selector: String,
    name: String,
    reference: Term<'_>,
) -> Result<Atom, Failure> {
    crate::request::read(env, reference, async move {
        let builder = saved_analyzer(&table.table, &selector, &name).await?;
        crate::compat::fts::tokenize(|| tokens_json(&text, &builder))?
    })
}
