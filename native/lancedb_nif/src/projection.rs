use crate::error::Failure;
use arrow_array::RecordBatch;
use arrow_schema::Schema;
use lancedb::query::Select;
use std::sync::Arc;

#[derive(Clone, Default, serde::Deserialize)]
pub(crate) struct ProjectionRequest {
    columns: Option<Vec<String>>,
    select: Option<Vec<(String, String)>>,
}

pub(crate) type OutputProjection = Option<Vec<(String, String)>>;

pub(crate) struct Projection {
    pub(crate) selection: Option<Select>,
    pub(crate) output: OutputProjection,
}

/// Metadata visible to callers, distinct from fields required by SDK channels.
#[derive(Clone, Copy)]
pub(crate) struct ResultPolicy {
    score: Option<&'static str>,
    with_row_id: bool,
    hybrid: bool,
}

impl ResultPolicy {
    pub(crate) fn plain(with_row_id: bool) -> Self {
        Self {
            score: None,
            with_row_id,
            hybrid: false,
        }
    }

    pub(crate) fn text(with_row_id: bool) -> Self {
        Self {
            score: Some("_score"),
            ..Self::plain(with_row_id)
        }
    }

    pub(crate) fn vector(with_row_id: bool, hybrid: bool) -> Self {
        Self {
            score: Some(if hybrid {
                "_relevance_score"
            } else {
                "_distance"
            }),
            with_row_id,
            hybrid,
        }
    }

    fn metadata(self) -> Vec<&'static str> {
        let mut metadata: Vec<_> = self.score.into_iter().collect();
        if self.with_row_id {
            metadata.push("_rowid");
        }
        metadata
    }

    fn finalize(self, mut projection: Projection) -> Projection {
        if self.hybrid {
            projection.selection = projection.hybrid_selection();
            if !self.with_row_id {
                if let Some(columns) = &mut projection.output {
                    columns.retain(|(_, name)| name != "_rowid");
                }
            }
        }
        projection
    }
}

impl ProjectionRequest {
    pub(crate) async fn prepare(
        self,
        table: &lancedb::Table,
        policy: ResultPolicy,
        order_by: &[(String, bool, bool)],
    ) -> Result<Projection, Failure> {
        let projection = self
            .prepare_selection(table, &policy.metadata(), order_by)
            .await?;
        Ok(policy.finalize(projection))
    }

    async fn prepare_selection(
        self,
        table: &lancedb::Table,
        metadata: &[&str],
        order_by: &[(String, bool, bool)],
    ) -> Result<Projection, Failure> {
        if let Some(expressions) = self.select {
            if self.columns.is_some() || expressions.is_empty() {
                return Err("select must be non-empty and cannot be combined with columns".into());
            }
            let schema = table.schema().await.map_err(Failure::from_error)?;
            // Use private aliases so a computed output named like a sort column
            // cannot replace the stored value used for ordering.
            let mut selected = Vec::new();
            let mut output = Vec::new();
            for (index, (name, sql)) in expressions.into_iter().enumerate() {
                let mut internal = format!("__lancedb_select_{index}");
                while schema.field_with_name(&internal).is_ok() {
                    internal.push('_');
                }
                selected.push((internal.clone(), sql));
                output.push((internal, name));
            }
            // Lance 10 requires a physical input even for constant expressions.
            let anchor = schema.fields().first().ok_or("Table has no columns")?;
            selected.push((anchor.name().clone(), quote(anchor.name())));
            for (name, _, _) in order_by {
                if !selected.iter().any(|(alias, _)| alias == name) {
                    selected.push((
                        name.clone(),
                        lance::datatypes::escape_field_path_for_project(name),
                    ));
                }
            }
            for name in metadata {
                if !selected.iter().any(|(alias, _)| alias == name) {
                    selected.push((name.to_string(), quote(name)));
                }
                output.push((name.to_string(), name.to_string()));
            }
            Ok(Projection {
                selection: Some(Select::Dynamic(selected)),
                output: Some(output),
            })
        } else if let Some(mut columns) = self.columns {
            for name in metadata {
                if !columns.iter().any(|column| column == name) {
                    columns.push(name.to_string());
                }
            }
            let output = columns
                .iter()
                .map(|name| (name.clone(), name.clone()))
                .collect();
            for (name, _, _) in order_by {
                if !columns.contains(name) {
                    columns.push(name.clone());
                }
            }
            Ok(Projection {
                selection: Some(Select::Columns(columns)),
                output: Some(output),
            })
        } else {
            Ok(Projection {
                selection: None,
                output: None,
            })
        }
    }
}

impl Projection {
    // The SDK adds query_index after each scanner. A singleton never enters
    // that SDK branch, so produce the same Int32 metadata as a constant instead.
    pub(crate) fn batch(mut self, schema: &Schema, singleton: bool) -> Self {
        if let Some(output) = &mut self.output {
            if !output.iter().any(|(_, name)| name == "query_index") {
                output.push(("query_index".into(), "query_index".into()));
            }
        }
        self.selection = match self.selection {
            Some(Select::Columns(mut columns)) => {
                columns.retain(|name| name != "query_index");
                if singleton {
                    Some(Select::Dynamic(batch_constant(columns)))
                } else {
                    Some(Select::Columns(columns))
                }
            }
            Some(Select::Dynamic(mut expressions)) => {
                expressions.retain(|(name, _)| name != "query_index");
                if singleton {
                    expressions.push(("query_index".into(), "CAST(0 AS INT)".into()));
                }
                Some(Select::Dynamic(expressions))
            }
            None if singleton => Some(Select::Dynamic(batch_constant(
                schema
                    .fields()
                    .iter()
                    .map(|field| field.name().clone())
                    .collect(),
            ))),
            selection => selection,
        };
        self
    }

    // Hybrid relevance is produced by fusion, not either channel's scanner.
    fn hybrid_selection(&self) -> Option<Select> {
        self.selection.clone().map(|selection| match selection {
            Select::Columns(mut columns) => {
                columns.retain(|name| name != "_relevance_score");
                Select::Columns(columns)
            }
            Select::Dynamic(mut columns) => {
                columns.retain(|(name, _)| name != "_relevance_score");
                Select::Dynamic(columns)
            }
            selection => selection,
        })
    }
}

fn batch_constant(columns: Vec<String>) -> Vec<(String, String)> {
    let mut expressions: Vec<_> = columns
        .into_iter()
        .map(|name| {
            let sql = quote(&name);
            (name, sql)
        })
        .collect();
    expressions.push(("query_index".into(), "CAST(0 AS INT)".into()));
    expressions
}

fn quote(name: &str) -> String {
    format!("`{}`", name.replace('`', "``"))
}

pub(crate) fn project(
    batch: RecordBatch,
    output: &OutputProjection,
) -> Result<RecordBatch, Failure> {
    let Some(output) = output else {
        return Ok(batch);
    };
    let schema = batch.schema();
    let indices = output
        .iter()
        .map(|(source, _)| schema.index_of(source))
        .collect::<Result<Vec<_>, _>>()
        .map_err(Failure::from_error)?;
    let fields: Vec<_> = indices
        .iter()
        .zip(output)
        .map(|(index, (_, name))| schema.field(*index).clone().with_name(name))
        .collect();
    let arrays = indices
        .iter()
        .map(|index| batch.column(*index).clone())
        .collect();
    RecordBatch::try_new(
        Arc::new(Schema::new_with_metadata(fields, schema.metadata().clone())),
        arrays,
    )
    .map_err(Failure::from_error)
}
