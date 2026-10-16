//! SQL declarations and explicit materialization use the SDK's dependency and freshness checks.
use crate::{error::Failure, resources::TableResource};
use arrow_schema::{Schema, SchemaRef};
use rustler::{Atom, Env, ResourceArc, Term};
use std::sync::Arc;

pub(crate) fn write_schema(schema: SchemaRef) -> SchemaRef {
    Arc::new(Schema::new_with_metadata(
        schema
            .fields()
            .iter()
            .filter(|field| {
                lancedb::table::computed_columns::computed_column_from_field(field).is_none()
            })
            .cloned()
            .collect::<Vec<_>>(),
        schema.metadata().clone(),
    ))
}

#[rustler::nif(schedule = "DirtyCpu")]
fn add_computed_columns(
    env: Env<'_>,
    table: ResourceArc<TableResource>,
    expressions: Vec<(String, String)>,
    reference: Term<'_>,
) -> Result<Atom, Failure> {
    crate::request::write(env, reference, move |permit| async move {
        let _permit = permit;
        table.ensure_mutable()?;
        let schema = table.table.schema().await.map_err(Failure::from_error)?;
        lancedb::table::computed_columns::validate_declarations(schema, &expressions)
            .map_err(Failure::from_error)?;
        let mut builder = table.table.add_columns();
        for (name, expression) in expressions {
            builder = builder.computed(name, expression);
        }
        builder
            .execute()
            .await
            .map(|result| crate::wire::Version(result.version))
            .map_err(Failure::write_error)
    })
}

#[derive(Clone, rustler::NifMap)]
pub(crate) struct RefreshResult {
    rows_filled: u64,
    version: u64,
}

impl From<lancedb::table::refresh::RefreshColumnResult> for RefreshResult {
    fn from(result: lancedb::table::refresh::RefreshColumnResult) -> Self {
        Self {
            rows_filled: result.rows_filled,
            version: result.version,
        }
    }
}

impl From<lancedb::function::RefreshColumnResult> for RefreshResult {
    fn from(result: lancedb::function::RefreshColumnResult) -> Self {
        Self {
            rows_filled: result.rows_assigned,
            // A no-op may publish no version; the source is its exact visible version.
            version: result.published_version.unwrap_or(result.source_version),
        }
    }
}

#[rustler::nif]
fn refresh_column(
    env: Env<'_>,
    table: ResourceArc<TableResource>,
    column: String,
    reference: Term<'_>,
) -> Result<Atom, Failure> {
    crate::request::write(env, reference, move |permit| async move {
        let _permit = permit;
        table.ensure_mutable()?;
        table
            .table
            .refresh_column(column)
            .await
            .map(|result| crate::wire::Refresh(result.into()))
            .map_err(Failure::write_error)
    })
}

#[rustler::nif]
fn refresh_column_async(
    env: Env<'_>,
    table: ResourceArc<TableResource>,
    column: String,
    reference: Term<'_>,
) -> Result<Atom, Failure> {
    crate::request::write(env, reference, move |permit| async move {
        table.ensure_mutable()?;
        let job = table
            .table
            .refresh_column_async(column)
            .await
            .map_err(Failure::write_error)?;
        Ok(crate::job::JobResource::refresh(job, permit))
    })
}
