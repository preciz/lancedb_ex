//! Lance 13 loses matches when a `contains` needle yields no n-gram tokens.
//! Use the equivalent `strpos(...) > 0` for those needles, leaving useful
//! literal searches available to both the n-gram and FM index planners.
use crate::error::Failure;
use lance_index::scalar::ngram::{NGRAM_TOKENIZER, TEXT_PREPPER};
use lancedb::datafusion::sql::sqlparser::{
    dialect::GenericDialect,
    tokenizer::{Token, Tokenizer},
};

struct Call {
    name: usize,
    comma: Option<usize>,
    extra_args: bool,
}

pub(crate) fn filter(sql: String) -> Result<String, Failure> {
    if !sql.to_ascii_lowercase().contains("contains") {
        return Ok(sql);
    }
    // Keep original quoting, whitespace and comments. Inspect tokens rather
    // than rewriting text inside literals or quoted column names.
    let tokens = Tokenizer::new(&GenericDialect {}, &sql)
        .with_unescape(false)
        .tokenize()
        .map_err(Failure::from_error)?;
    let mut replacements = std::collections::HashMap::new();
    let mut stack: Vec<Option<Call>> = Vec::new();
    let mut previous = None;
    for (index, token) in tokens.iter().enumerate() {
        match token {
            Token::LParen => {
                let call = previous.and_then(|name| match &tokens[name] {
                    Token::Word(word) if word.value.eq_ignore_ascii_case("contains") => {
                        Some(Call {
                            name,
                            comma: None,
                            extra_args: false,
                        })
                    }
                    _ => None,
                });
                stack.push(call);
            }
            Token::LBracket | Token::LBrace => stack.push(None),
            Token::RBracket | Token::RBrace => {
                stack.pop();
            }
            Token::Comma => {
                if let Some(Some(call)) = stack.last_mut() {
                    if call.comma.replace(index).is_some() {
                        call.extra_args = true;
                    }
                }
            }
            Token::RParen => {
                if let Some(Some(call)) = stack.pop() {
                    if let Some(comma) = call.comma.filter(|_| !call.extra_args) {
                        let mut needle = tokens[comma + 1..index]
                            .iter()
                            .filter(|token| !matches!(token, Token::Whitespace(_)));
                        let literal = match (needle.next(), needle.next()) {
                            (Some(Token::SingleQuotedString(value)), None) => Some(value),
                            _ => None,
                        };
                        // Nonliteral needles may fold to an untokenizable
                        // constant later. Route those through the safe form too.
                        // The SDK already bypasses n-grams for short needles;
                        // keep those intact so FM can still accelerate them.
                        if literal
                            .is_none_or(|value| value.chars().count() >= 3 && !has_tokens(value))
                        {
                            replacements.insert(call.name, "(strpos".to_string());
                            replacements.insert(index, ") > 0)".to_string());
                        }
                    }
                }
            }
            _ => {}
        }
        if !matches!(token, Token::Whitespace(_)) {
            previous = Some(index);
        }
    }
    if replacements.is_empty() {
        return Ok(sql);
    }
    Ok(tokens
        .iter()
        .enumerate()
        .map(|(index, token)| {
            replacements
                .remove(&index)
                .unwrap_or_else(|| token.to_string())
        })
        .collect())
}

fn has_tokens(value: &str) -> bool {
    let mut prepper = TEXT_PREPPER.clone();
    let mut tokenizer = NGRAM_TOKENIZER.clone();
    let mut raw = prepper.token_stream(value);
    while raw.advance() {
        if tokenizer.token_stream(&raw.token().text).advance() {
            return true;
        }
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalized_literals_keep_useful_trigrams_and_preserve_sql() {
        for sql in [
            "contains(text, 'abc')",
            "contains(text, 'ééb')",
            "contains(text, 'café')",
            "contains(text, 'éé')",
            "contains(text, '')",
            "text = 'contains(text, ''!!!'')'",
            "`contains` = '!!!'",
        ] {
            assert_eq!(filter(sql.into()).unwrap(), sql);
        }
    }

    #[test]
    fn untokenizable_and_computed_needles_use_null_preserving_position_checks() {
        for needle in ["'雪山猫'", "'!!!'", "concat('雪', '山猫')", "NULL"] {
            assert_eq!(
                filter(format!("contains(text, {needle})")).unwrap(),
                format!("(strpos(text, {needle}) > 0)")
            );
        }
        assert_eq!(
            filter("NOT CONTAINS /* keep */ (`Text`, '!!!') OR contains(text, 'abc')".into())
                .unwrap(),
            "NOT (strpos /* keep */ (`Text`, '!!!') > 0) OR contains(text, 'abc')"
        );
        assert_eq!(
            filter("contains(concat(text, ','), '!!!')".into()).unwrap(),
            "(strpos(concat(text, ','), '!!!') > 0)"
        );
        assert_eq!(
            filter("contains(text, '雪''山猫')".into()).unwrap(),
            "(strpos(text, '雪''山猫') > 0)"
        );
    }
}
