//! SDK 0.40 refresh zips evaluated and stored struct arrays using exact child types.
use crate::error::Failure;
use arrow_schema::Field as ArrowField;
use lance::datatypes::{Field, Schema};

pub(crate) fn validate_field_metadata(schema: &Schema, field: &Field) -> Result<(), Failure> {
    let Some(ancestry) = schema.field_ancestry_by_id(field.id) else {
        return Ok(());
    };
    if ancestry.len() < 2 {
        return Ok(());
    }
    let root = ancestry[0].id;
    let inside_computed = ancestry.iter().any(|parent| {
        parent.id != field.id
            && parent.metadata.get("computed_column").map(String::as_str) == Some("true")
    });
    // Identity projections also carry child metadata from their container inputs.
    let container_input = schema.fields.iter().any(|output| {
        if !output.data_type().is_nested() {
            return false;
        }
        lancedb::table::computed_columns::computed_column_from_field(&ArrowField::from(output))
            .is_some_and(|declaration| {
                declaration.inputs.iter().any(|input| {
                    schema
                        .field(input)
                        .and_then(|field| schema.field_ancestry_by_id(field.id))
                        .and_then(|ancestry| ancestry.first().map(|field| field.id))
                        == Some(root)
                })
            })
    });
    if inside_computed || container_input {
        return Err(Failure::new(
            "unsupported",
            "The pinned SDK cannot refresh computed containers after nested metadata edits on them or their inputs; update top-level user metadata instead",
        ));
    }
    Ok(())
}
