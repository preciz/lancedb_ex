use crate::error::Failure;
use lancedb::index::scalar::FtsIndexBuilder;

#[derive(serde::Deserialize)]
pub(crate) struct FtsAnalyzer {
    tokenizer: String,
    document_tokenizer: Option<String>,
    custom_stop_words: Option<Vec<String>>,
    split_identifiers: Option<bool>,
    split_on_numerics: Option<bool>,
    preserve_original: Option<bool>,
    index_operators: Option<bool>,
    language: Option<String>,
    with_position: Option<bool>,
    lower_case: Option<bool>,
    stem: Option<bool>,
    remove_stop_words: Option<bool>,
    ascii_folding: Option<bool>,
    max_token_length: Option<usize>,
    unlimited_token_length: bool,
    ngram_min_length: Option<u32>,
    ngram_max_length: Option<u32>,
    ngram_prefix_only: Option<bool>,
}

pub(crate) fn fts_builder(options: Option<FtsAnalyzer>) -> Result<FtsIndexBuilder, Failure> {
    let Some(o) = options else {
        return Ok(FtsIndexBuilder::default());
    };
    let mut builder = FtsIndexBuilder::default().base_tokenizer(o.tokenizer);
    if let Some(v) = o.document_tokenizer {
        builder = builder.lance_tokenizer(v);
    }
    if let Some(v) = o.custom_stop_words {
        builder = builder.custom_stop_words(Some(v));
    }
    if let Some(v) = o.split_identifiers {
        builder = builder.split_identifiers(v);
    }
    if let Some(v) = o.split_on_numerics {
        builder = builder.split_on_numerics(v);
    }
    if let Some(v) = o.preserve_original {
        builder = builder.preserve_original(v);
    }
    if let Some(v) = o.index_operators {
        builder = builder.index_operators(v);
    }
    if let Some(language) = o.language {
        builder = builder.language(&language).map_err(|e| {
            let message = format!("Invalid FTS language: {e}");
            Failure::from_error(e).context(message)
        })?;
    }
    if let Some(v) = o.with_position {
        builder = builder.with_position(v);
    }
    if let Some(v) = o.lower_case {
        builder = builder.lower_case(v);
    }
    if let Some(v) = o.stem {
        builder = builder.stem(v);
    }
    if let Some(v) = o.remove_stop_words {
        builder = builder.remove_stop_words(v);
    }
    if let Some(v) = o.ascii_folding {
        builder = builder.ascii_folding(v);
    }
    if o.unlimited_token_length {
        builder = builder.max_token_length(None);
    } else if let Some(v) = o.max_token_length {
        builder = builder.max_token_length(Some(v));
    }
    if let Some(v) = o.ngram_min_length {
        builder = builder.ngram_min_length(v);
    }
    if let Some(v) = o.ngram_max_length {
        builder = builder.ngram_max_length(v);
    }
    if let Some(v) = o.ngram_prefix_only {
        builder = builder.ngram_prefix_only(v);
    }
    Ok(builder)
}
