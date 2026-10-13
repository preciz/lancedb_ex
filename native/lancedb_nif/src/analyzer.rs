use crate::error::Failure;
use lancedb::index::scalar::FtsIndexBuilder;
use lancedb::index::IndexType;

// SDK 0.40's public tokenize helpers discard deferred tokenizer errors.
// Resolve their saved configuration and use the fallible collector instead.
pub(crate) async fn saved_analyzer(
    table: &lancedb::Table,
    selector: &str,
    name: &str,
) -> Result<FtsIndexBuilder, Failure> {
    let column = match selector {
        "index" => None,
        "column" => {
            let arrow = table.schema().await.map_err(Failure::from_error)?;
            let schema =
                lance::datatypes::Schema::try_from(arrow.as_ref()).map_err(Failure::from_error)?;
            lance::datatypes::parse_field_path(name).map_err(Failure::from_error)?;
            let fields = schema
                .resolve_case_insensitive(name)
                .ok_or_else(|| format!("Field path '{name}' not found in schema"))?;
            let names: Vec<_> = fields.iter().map(|field| field.name.as_str()).collect();
            Some(lance::datatypes::format_field_path(&names))
        }
        _ => return Err("Invalid tokenizer selector".into()),
    };
    let indices = table.list_indices().await.map_err(Failure::from_error)?;
    let matches: Vec<_> = indices
        .iter()
        .filter(|index| match &column {
            None => index.name == name,
            Some(column) => {
                index.index_type == IndexType::FTS
                    && index.columns.len() == 1
                    && index.columns[0] == *column
            }
        })
        .collect();
    let description = format!("{selector} '{name}'");
    let index = match matches.as_slice() {
        [index] => *index,
        [] => return Err(format!("No FTS index for {description}").into()),
        _ => return Err(format!("FTS index selection is ambiguous for {description}").into()),
    };
    if index.index_type != IndexType::FTS {
        return Err(format!("Index '{name}' is not a full text search index").into());
    }
    let details = index.index_details.as_deref().ok_or_else(|| {
        format!(
            "FTS index '{}' does not include tokenizer details",
            index.name
        )
    })?;
    serde_json::from_str(details).map_err(Failure::from_error)
}

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
