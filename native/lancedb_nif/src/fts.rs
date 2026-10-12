use crate::analyzer::{fts_builder, FtsAnalyzer};
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
        if let Self::Match {
            text,
            prefix_length,
            ..
        } = self
        {
            // Lance 10 slices fuzzy prefixes at byte offsets. Avoid a panic on
            // a non-UTF-8 boundary until upstream supports Unicode prefixes.
            if *prefix_length > 0 && !text.is_ascii() {
                return Err("Positive prefix_length requires ASCII query text".into());
            }
        }
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

fn tokens_json(tokens: Vec<lancedb::table::FtsToken>) -> String {
    serde_json::Value::Array(
        tokens
            .into_iter()
            .map(|token| serde_json::json!({"text": token.text, "position": token.position}))
            .collect(),
    )
    .to_string()
}

#[rustler::nif(schedule = "DirtyCpu")]
fn tokenize(text: String, analyzer: String) -> Result<String, Failure> {
    let analyzer: FtsAnalyzer = serde_json::from_str(&analyzer).map_err(Failure::from_error)?;
    let builder = fts_builder(Some(analyzer))?;
    let tokens = crate::compat::fts::tokenize(|| lancedb::tokenize(&text, &builder))?
        .map_err(Failure::from_error)?;
    Ok(tokens_json(tokens))
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
        let tokens = crate::compat::fts::index(async {
            match selector.as_str() {
                "index" => table.table.tokenize(&text, &name).await,
                "column" => table.table.tokenize_with_column(&text, &name).await,
                _ => Err(lancedb::Error::InvalidInput {
                    message: "Invalid tokenizer selector".into(),
                }),
            }
        })
        .await?;
        Ok(tokens_json(tokens))
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_unicode_prefixes_before_the_sdk_can_slice_a_character() {
        let query: StructuredQuery = serde_json::from_value(serde_json::json!({
            "type": "match", "text": "éclair", "column": "text", "operator": "or",
            "boost": 1.0, "fuzziness": 1, "max_expansions": 50, "prefix_length": 1
        }))
        .unwrap();
        assert!(query.build(None).unwrap_err().contains("ASCII"));
    }
}
