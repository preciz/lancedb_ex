//! Stable error categories cross the NIF boundary before SDK errors become text.
use std::{any::Any, fmt};

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct Failure {
    pub kind: &'static str,
    pub message: String,
    pub phase: Option<&'static str>,
    pub commit_outcome: Option<&'static str>,
    pub processed_rows: Option<usize>,
}

impl Failure {
    pub(crate) fn new(kind: &'static str, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
            phase: None,
            commit_outcome: None,
            processed_rows: None,
        }
    }

    pub(crate) fn before_write(mut self) -> Self {
        self.phase.get_or_insert("validation");
        self.commit_outcome.get_or_insert("not_started");
        self
    }

    pub(crate) fn executing(mut self) -> Self {
        self.phase.get_or_insert("execution");
        self.commit_outcome.get_or_insert("unknown");
        self
    }

    pub(crate) fn write_error<E: fmt::Display + Any>(error: E) -> Self {
        Self::from_error(error).executing()
    }

    pub(crate) fn progress(mut self, rows: usize) -> Self {
        self.processed_rows = Some(rows);
        self
    }

    pub(crate) fn waiting(mut self) -> Self {
        self.phase = Some("waiting");
        self.commit_outcome = Some("unknown");
        self
    }

    pub(crate) fn from_error<E: fmt::Display + Any>(error: E) -> Self {
        let source = &error as &dyn Any;
        if let Some(error) = source.downcast_ref::<Self>() {
            return error.clone();
        }
        let kind = if let Some(error) = source.downcast_ref::<lancedb::Error>() {
            use lancedb::Error::*;
            match error {
                TableNotFound { .. } | DatabaseNotFound { .. } | IndexNotFound { .. } => {
                    "not_found"
                }
                TableAlreadyExists { .. } | DatabaseAlreadyExists { .. } => "already_exists",
                InvalidTableName { .. } | InvalidInput { .. } | Schema { .. } | Arrow { .. } => {
                    "invalid_data"
                }
                Timeout { .. } => "timeout",
                NotSupported { .. } => "unsupported",
                ObjectStore { .. } | CreateDir { .. } => "storage",
                JobCancelled { .. } => "cancelled",
                Lance { source } => return Self::from_lance(source),
                _ => "execution",
            }
        } else if let Some(error) = source.downcast_ref::<lance::Error>() {
            return Self::from_lance(error);
        } else if source.is::<arrow_schema::ArrowError>() || source.is::<serde_json::Error>() {
            "invalid_data"
        } else {
            "execution"
        };
        Self::new(kind, error.to_string())
    }

    fn from_lance(error: &lance::Error) -> Self {
        let kind = {
            use lance::Error::*;
            match error {
                DatasetNotFound { .. }
                | NotFound { .. }
                | IndexNotFound { .. }
                | RefNotFound { .. }
                | VersionNotFound { .. }
                | FieldNotFound { .. } => "not_found",
                DatasetAlreadyExists { .. } => "already_exists",
                CommitConflict { .. }
                | RetryableCommitConflict { .. }
                | TooMuchWriteContention { .. }
                | RefConflict { .. }
                | VersionConflict { .. } => "conflict",
                InvalidInput { .. }
                | SchemaMismatch { .. }
                | Schema { .. }
                | Arrow { .. }
                | InvalidRef { .. } => "invalid_data",
                Timeout { .. } => "timeout",
                NotSupported { .. } => "unsupported",
                IO { .. } => "storage",
                _ => "execution",
            }
        };
        Self::new(kind, error.to_string())
    }

    pub(crate) fn context(mut self, message: String) -> Self {
        self.message = message;
        self
    }
}

impl From<String> for Failure {
    fn from(message: String) -> Self {
        Self::new("invalid_data", message)
    }
}
impl From<&str> for Failure {
    fn from(message: &str) -> Self {
        message.to_string().into()
    }
}
impl fmt::Display for Failure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.message.fmt(f)
    }
}
impl std::error::Error for Failure {}
impl std::ops::Deref for Failure {
    type Target = str;
    fn deref(&self) -> &str {
        &self.message
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn sdk_categories_survive_context_and_nested_lance_errors() {
        for (error, kind) in [
            (
                lancedb::Error::InvalidInput {
                    message: "bad predicate".into(),
                },
                "invalid_data",
            ),
            (
                lancedb::Error::TableAlreadyExists {
                    name: "docs".into(),
                },
                "already_exists",
            ),
            (
                lancedb::Error::Timeout {
                    message: "deadline".into(),
                },
                "timeout",
            ),
            (
                lancedb::Error::Lance {
                    source: lance::Error::VersionNotFound {
                        message: "missing".into(),
                    },
                },
                "not_found",
            ),
        ] {
            let failure = Failure::from_error(error).context("operation context".into());
            assert_eq!(failure.kind, kind);
            assert_eq!(Failure::from_error(failure.clone()), failure);
        }
    }
}
