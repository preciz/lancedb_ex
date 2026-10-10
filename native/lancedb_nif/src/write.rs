use crate::error::Failure;
use crate::resources::{TableResource, RUNTIME};
use rustler::ResourceArc;

#[derive(rustler::NifMap)]
struct UpdateResult {
    updated: u64,
    version: u64,
}

#[derive(rustler::NifTaggedEnum)]
// Literal data is schema-checked; SQL retains the SDK's expression semantics.
enum UpdateValue {
    Literal(String),
    Sql(String),
}

impl UpdateValue {
    fn expression(self, field: &arrow_schema::Field) -> Result<String, Failure> {
        match self {
            Self::Sql(expression) => Ok(expression),
            Self::Literal(json) => {
                let mut value: serde_json::Value =
                    serde_json::from_str(&json).map_err(Failure::from_error)?;
                crate::value::normalize(&mut value, field).map_err(|e| {
                    let message = format!("Update value for '{}' {e}", field.name());
                    Failure::from_error(e).context(message)
                })?;
                crate::value::sql_literal(&value, field.data_type())
            }
        }
    }
}

#[rustler::nif(schedule = "DirtyCpu")]
fn update_rows(
    table_res: ResourceArc<TableResource>,
    changes: Vec<(String, UpdateValue)>,
    filter: Option<String>,
) -> Result<crate::wire::Counts<UpdateResult>, Failure> {
    update_rows_impl(table_res, changes, filter)
        .map(crate::wire::Counts)
        .map_err(Failure::before_write)
}

fn update_rows_impl(
    table_res: ResourceArc<TableResource>,
    changes: Vec<(String, UpdateValue)>,
    filter: Option<String>,
) -> Result<UpdateResult, Failure> {
    RUNTIME.block_on(async {
        let _permit = crate::execution::Permit::write()?;
        table_res.ensure_mutable()?;
        let schema = table_res
            .table
            .schema()
            .await
            .map_err(Failure::from_error)?;
        crate::compat::update::validate(&schema)?;
        let mut update = table_res.table.update();
        if let Some(filter) = filter {
            update = update.only_if(filter);
        }
        for (column, value) in changes {
            let field = schema
                .field_with_name(&column)
                .map_err(Failure::from_error)?;
            update = update.column(column, value.expression(field)?);
        }
        update
            .execute()
            .await
            .map(|result| UpdateResult {
                updated: result.rows_updated,
                version: result.version,
            })
            .map_err(|e| {
                let message = format!("Failed to update rows: {e}");
                Failure::write_error(e).context(message)
            })
    })
}

#[derive(serde::Deserialize)]
pub(crate) struct MergeOptions {
    update: bool,
    update_condition: Option<String>,
    insert: bool,
    delete: bool,
    delete_condition: Option<String>,
    timeout_ms: Option<u64>,
    use_index: bool,
}

#[derive(Clone, rustler::NifMap)]
pub(crate) struct ConditionalMergeResult {
    inserted: u64,
    updated: u64,
    deleted: u64,
    version: u64,
    attempts: u32,
}

#[derive(rustler::NifMap)]
struct DeleteResult {
    deleted: u64,
    version: u64,
}

#[rustler::nif(schedule = "DirtyIo")]
fn delete_rows_result(
    table: ResourceArc<TableResource>,
    predicate: String,
) -> Result<crate::wire::Counts<DeleteResult>, Failure> {
    delete_rows_result_impl(table, predicate)
        .map(crate::wire::Counts)
        .map_err(Failure::before_write)
}

fn delete_rows_result_impl(
    table: ResourceArc<TableResource>,
    predicate: String,
) -> Result<DeleteResult, Failure> {
    RUNTIME.block_on(async {
        let _permit = crate::execution::Permit::write()?;
        let result = table
            .table
            .delete(&predicate)
            .await
            .map_err(Failure::write_error)?;
        Ok(DeleteResult {
            deleted: result.num_deleted_rows,
            version: result.version,
        })
    })
}

pub(crate) fn merge_builder(
    table: &lancedb::Table,
    keys: &[String],
    options: MergeOptions,
) -> lancedb::table::merge::MergeInsertBuilder {
    let keys: Vec<&str> = keys.iter().map(String::as_str).collect();
    let mut merge = table.merge_insert(&keys);
    if options.update {
        merge.when_matched_update_all(options.update_condition);
    }
    if options.insert {
        merge.when_not_matched_insert_all();
    }
    if options.delete {
        merge.when_not_matched_by_source_delete(options.delete_condition);
    }
    merge.use_index(options.use_index);
    if let Some(ms) = options.timeout_ms {
        merge.timeout(std::time::Duration::from_millis(ms));
    }
    merge
}
impl From<lancedb::table::MergeResult> for ConditionalMergeResult {
    fn from(result: lancedb::table::MergeResult) -> Self {
        Self {
            inserted: result.num_inserted_rows,
            updated: result.num_updated_rows,
            deleted: result.num_deleted_rows,
            version: result.version,
            attempts: result.num_attempts,
        }
    }
}
