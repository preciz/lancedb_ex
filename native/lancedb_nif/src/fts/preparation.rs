use crate::analyzer::{fts_builder, saved_analyzer, FtsAnalyzer};
use crate::error::Failure;
use crate::resources::TableResource;
use lancedb::index::scalar::FtsIndexBuilder;
use rustler::{Atom, Env, ResourceArc, Term};
use std::collections::HashSet;

fn prepared_json(
    text: &str,
    builder: &FtsIndexBuilder,
    max_terms: Option<usize>,
) -> Result<String, Failure> {
    // These SDK fields are private but their persisted representation is public.
    let params = serde_json::to_value(builder).map_err(Failure::from_error)?;
    if params["base_tokenizer"] != "simple"
        || !matches!(params["lance_tokenizer"].as_str(), None | Some("text"))
    {
        return Err(Failure::new(
            "unsupported",
            "FTS preparation requires plain text with the simple tokenizer",
        ));
    }

    let mut tokenizer = builder.build().map_err(Failure::from_error)?;
    let mut stream = tokenizer.token_stream_for_search(text);
    let mut seen = HashSet::new();
    let mut terms = Vec::new();
    while let Some(token) = stream
        .try_next()
        .map_err(|message| Failure::new("invalid_data", message))?
    {
        // Explicit fuzzy execution re-tokenizes with SimpleTokenizer. Reject
        // normalized terms it would split, rather than silently changing them.
        if token.text.is_empty() || !token.text.chars().all(char::is_alphanumeric) {
            return Err(Failure::new(
                "unsupported",
                "FTS preparation requires normalization to preserve a single simple token",
            ));
        }
        let original = text
            .get(token.offset_from..token.offset_to)
            .ok_or_else(|| Failure::new("invalid_data", "Invalid analyzer token offsets"))?;
        terms.push(serde_json::json!({"original": original, "analyzed": token.text}));
        // Return the first over-limit term so Elixir can raise ArgumentError
        // without matching error messages or returning a truncated preparation.
        if max_terms.is_some_and(|limit| {
            seen.insert(token.text.clone());
            seen.len() > limit
        }) {
            break;
        }
    }
    serde_json::to_string(&terms).map_err(Failure::from_error)
}

#[rustler::nif(schedule = "DirtyCpu")]
fn prepare_fts(
    text: String,
    analyzer: String,
    max_terms: Option<usize>,
) -> Result<String, Failure> {
    let analyzer: FtsAnalyzer = serde_json::from_str(&analyzer).map_err(Failure::from_error)?;
    let builder = fts_builder(Some(analyzer))?;
    crate::compat::fts::tokenize(|| prepared_json(&text, &builder, max_terms))?
}

#[rustler::nif]
fn prepare_fts_table(
    env: Env<'_>,
    table: ResourceArc<TableResource>,
    text: String,
    selector: String,
    name: String,
    max_terms: Option<usize>,
    reference: Term<'_>,
) -> Result<Atom, Failure> {
    crate::request::read(env, reference, async move {
        let builder = saved_analyzer(&table.table, &selector, &name).await?;
        crate::compat::fts::tokenize(|| prepared_json(&text, &builder, max_terms))?
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn offsets_pair_terms_after_filtering_and_normalization() {
        let builder = FtsIndexBuilder::default();
        let terms: serde_json::Value =
            serde_json::from_str(&prepared_json("the Running in cafés", &builder, None).unwrap())
                .unwrap();
        assert_eq!(
            terms,
            serde_json::json!([
                {"original": "Running", "analyzed": "run"},
                {"original": "cafés", "analyzed": "cafe"}
            ])
        );
    }

    #[test]
    fn limit_counts_distinct_terms_and_returns_first_excess() {
        let builder = FtsIndexBuilder::default().stem(false);
        let terms: serde_json::Value = serde_json::from_str(
            &prepared_json("BERLIN Berlin berlin Paris Rome", &builder, Some(1)).unwrap(),
        )
        .unwrap();
        assert_eq!(terms.as_array().unwrap().len(), 4);
        assert_eq!(terms[3]["analyzed"], "paris");
    }

    #[test]
    fn unsupported_analysis_is_rejected_before_tokenization() {
        for builder in [
            FtsIndexBuilder::default().base_tokenizer("ngram".into()),
            FtsIndexBuilder::default().lance_tokenizer("json".into()),
        ] {
            assert_eq!(
                prepared_json("text", &builder, None).unwrap_err().kind,
                "unsupported"
            );
        }
        let builder = FtsIndexBuilder::default().stem(false).ascii_folding(false);
        assert_eq!(
            prepared_json("İstanbul", &builder, None).unwrap_err().kind,
            "unsupported"
        );
    }
}
