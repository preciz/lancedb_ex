use crate::error::Failure;
pub(crate) mod definition;
pub(crate) mod storage;
use definition::TypeDefinition;

use crate::resources::{TableResource, RUNTIME};
use lancedb::query::{ExecutableQuery, QueryBase, Select};
use rustler::ResourceArc;

pub(crate) fn schema_json(schema: &arrow_schema::Schema) -> String {
    let fields: Vec<definition::FieldDefinition> = schema
        .fields()
        .iter()
        .map(|field| field.as_ref().into())
        .collect();
    serde_json::json!({"fields": fields, "metadata": schema.metadata()}).to_string()
}

#[rustler::nif(schedule = "DirtyCpu")]
fn get_schema_json(table_res: ResourceArc<TableResource>) -> Result<String, Failure> {
    RUNTIME.block_on(async {
        let _permit = crate::execution::Permit::read()?;
        match table_res.table.schema().await {
            Ok(schema) => Ok(schema_json(schema.as_ref())),
            Err(e) => {
                let message = format!("Failed to get schema: {e}");
                Err(Failure::from_error(e).context(message))
            }
        }
    })
}

#[rustler::nif(schedule = "DirtyCpu")]
fn add_columns(
    table_res: ResourceArc<TableResource>,
    expressions: Vec<(String, String)>,
) -> Result<crate::wire::Version, Failure> {
    add_columns_impl(table_res, expressions)
        .map(crate::wire::Version)
        .map_err(Failure::before_write)
}

fn add_columns_impl(
    table_res: ResourceArc<TableResource>,
    expressions: Vec<(String, String)>,
) -> Result<u64, Failure> {
    RUNTIME.block_on(async {
        let _permit = crate::execution::Permit::write()?;
        let schema = table_res
            .table
            .schema()
            .await
            .map_err(Failure::from_error)?;
        for (name, _) in &expressions {
            if schema.field_with_name(name).is_ok() {
                return Err(format!("Column '{name}' already exists").into());
            }
        }
        // Plan expressions first so we never commit types that add/upsert cannot encode.
        // The scanner needs a physical column even when every new expression
        // is a constant. Include an existing field with a quoted identifier.
        let anchor = schema.fields().first().ok_or("Table has no columns")?;
        let mut projection = expressions.clone();
        projection.push((
            anchor.name().clone(),
            format!("`{}`", anchor.name().replace('`', "``")),
        ));
        let planned = table_res
            .table
            .query()
            .select(Select::Dynamic(projection))
            .limit(1)
            .execute()
            .await
            .map_err(|e| {
                let message = format!("Invalid column expressions: {e}");
                Failure::write_error(e).context(message)
            })?;
        let planned_schema = planned.schema();
        let fields = expressions
            .iter()
            .map(|(name, _)| {
                planned_schema
                    .field_with_name(name)
                    .cloned()
                    .map_err(Failure::from_error)
            })
            .collect::<Result<Vec<_>, _>>()?;
        storage::StorageSchema::prepare(std::sync::Arc::new(arrow_schema::Schema::new(fields)))?;
        drop(planned);
        table_res
            .table
            .add_columns()
            .transform(lancedb::table::NewColumnTransform::SqlExpressions(
                expressions,
            ))
            .execute()
            .await
            .map(|result| result.version)
            .map_err(|e| {
                let message = format!("Failed to add columns: {e}");
                Failure::write_error(e).context(message)
            })
    })
}

#[derive(serde::Deserialize)]
struct Alteration {
    path: String,
    rename: Option<String>,
    nullable: Option<bool>,
    r#type: Option<TypeDefinition>,
}

// Canonical paths catch quoted aliases and overlapping edits before SDK mutation.
fn paths(schema: &lance::datatypes::Schema, names: &[String]) -> Result<Vec<Vec<String>>, Failure> {
    let mut paths: Vec<Vec<String>> = Vec::new();
    for name in names {
        let path = lance::datatypes::parse_field_path(name).map_err(Failure::from_error)?;
        schema
            .field(name)
            .ok_or_else(|| format!("Column '{name}' does not exist"))?;
        if paths
            .iter()
            .any(|existing| existing.starts_with(&path) || path.starts_with(existing))
        {
            return Err("Column changes must not contain duplicate or overlapping paths; use separate operations".into());
        }
        paths.push(path);
    }
    Ok(paths)
}

fn validate_renames(
    schema: &lance::datatypes::Schema,
    changes: &[Alteration],
    paths: &[Vec<String>],
) -> Result<(), Failure> {
    let mut destinations = std::collections::HashSet::new();
    for (change, path) in changes.iter().zip(paths) {
        if let Some(rename) = &change.rename {
            let (leaf, parent) = path.split_last().ok_or("Empty field path")?;
            let mut siblings = &schema.fields;
            for name in parent {
                siblings = &siblings
                    .iter()
                    .find(|field| &field.name == name)
                    .ok_or("Missing parent field")?
                    .children;
            }
            let mut destination = parent.to_vec();
            destination.push(rename.clone());
            if (rename != leaf && siblings.iter().any(|field| &field.name == rename))
                || !destinations.insert(destination.clone())
            {
                return Err(format!(
                    "Column name '{}' already exists or is used twice",
                    destination.join(".")
                )
                .into());
            }
        }
    }
    Ok(())
}

#[rustler::nif(schedule = "DirtyCpu")]
fn alter_columns(
    table_res: ResourceArc<TableResource>,
    changes_json: String,
) -> Result<crate::wire::Version, Failure> {
    alter_columns_impl(table_res, changes_json)
        .map(crate::wire::Version)
        .map_err(Failure::before_write)
}

fn alter_columns_impl(
    table_res: ResourceArc<TableResource>,
    changes_json: String,
) -> Result<u64, Failure> {
    let changes: Vec<Alteration> =
        serde_json::from_str(&changes_json).map_err(Failure::from_error)?;
    RUNTIME.block_on(async {
        let _permit = crate::execution::Permit::write()?;
        let arrow = table_res
            .table
            .schema()
            .await
            .map_err(Failure::from_error)?;
        let schema =
            lance::datatypes::Schema::try_from(arrow.as_ref()).map_err(Failure::from_error)?;
        let names: Vec<_> = changes.iter().map(|change| change.path.clone()).collect();
        let paths = paths(&schema, &names)?;
        validate_renames(&schema, &changes, &paths)?;
        for (change, path) in changes.iter().zip(&paths) {
            crate::compat::storage::validate_cast_path(change.r#type.is_some(), path.len())?;
        }
        let changes: Vec<_> = changes
            .into_iter()
            .map(|change| {
                let data_type = change
                    .r#type
                    .map(|definition| {
                        storage::prepare_cast(
                            schema.field(&change.path).ok_or("Missing cast field")?,
                            definition.into_arrow()?,
                        )
                    })
                    .transpose()?;
                Ok(lancedb::table::ColumnAlteration {
                    path: change.path,
                    rename: change.rename,
                    nullable: change.nullable,
                    data_type,
                })
            })
            .collect::<Result<Vec<_>, Failure>>()?;
        table_res
            .table
            .alter_columns(&changes)
            .await
            .map(|result| result.version)
            .map_err(|e| {
                let message = format!("Failed to alter columns: {e}");
                Failure::write_error(e).context(message)
            })
    })
}

#[rustler::nif(schedule = "DirtyCpu")]
fn drop_columns(
    table_res: ResourceArc<TableResource>,
    columns: Vec<String>,
) -> Result<crate::wire::Version, Failure> {
    drop_columns_impl(table_res, columns)
        .map(crate::wire::Version)
        .map_err(Failure::before_write)
}

fn drop_columns_impl(
    table_res: ResourceArc<TableResource>,
    columns: Vec<String>,
) -> Result<u64, Failure> {
    RUNTIME.block_on(async {
        let _permit = crate::execution::Permit::write()?;
        let arrow = table_res
            .table
            .schema()
            .await
            .map_err(Failure::from_error)?;
        let schema =
            lance::datatypes::Schema::try_from(arrow.as_ref()).map_err(Failure::from_error)?;
        paths(&schema, &columns)?;
        let removed = schema.project(&columns).map_err(Failure::from_error)?;
        let remaining = schema.exclude(removed).map_err(Failure::from_error)?;
        if remaining.fields.is_empty() {
            return Err("At least one table column must remain".into());
        }
        storage::StorageSchema::prepare(std::sync::Arc::new(arrow_schema::Schema::from(
            &remaining,
        )))?;
        let columns: Vec<&str> = columns.iter().map(String::as_str).collect();
        table_res
            .table
            .drop_columns(&columns)
            .await
            .map(|result| result.version)
            .map_err(|e| {
                let message = format!("Failed to drop columns: {e}");
                Failure::write_error(e).context(message)
            })
    })
}
