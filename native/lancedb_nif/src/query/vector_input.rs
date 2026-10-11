//! Keep batch searches and document multivector scoring distinct, even though
//! the SDK chooses between them from the stored column type.
use crate::error::Failure;
use arrow_schema::{DataType, Field, Schema};
use lance::datatypes::{format_field_path, Schema as LanceSchema};
use lancedb::query::VectorQuery;

#[derive(Default, PartialEq, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
enum SearchMode {
    #[default]
    Single,
    Batch,
    Multivector,
}

impl SearchMode {
    fn name(&self) -> &'static str {
        match self {
            Self::Single => "single-vector",
            Self::Batch => "batch",
            Self::Multivector => "multivector",
        }
    }
}

#[derive(serde::Deserialize)]
pub(super) struct VectorInput {
    vector: Vec<f64>,
    #[serde(default)]
    additional_vectors: Vec<Vec<f64>>,
    #[serde(default)]
    search_mode: SearchMode,
}

impl VectorInput {
    pub(super) fn batch_singleton(&self) -> Option<bool> {
        (self.search_mode == SearchMode::Batch).then_some(self.additional_vectors.is_empty())
    }

    pub(super) async fn prepare(
        self,
        table: &lancedb::Table,
        column: Option<&str>,
        distance: Option<&str>,
    ) -> Result<VectorQuery, Failure> {
        let schema = table.schema().await.map_err(Failure::from_error)?;
        let column = self.column(schema.as_ref(), column)?;
        let lance_schema = LanceSchema::try_from(schema.as_ref()).map_err(Failure::from_error)?;
        let field = lance_schema.field(&column).ok_or("Missing vector field")?;
        let stored_type = field.data_type();
        let element = match &stored_type {
            DataType::FixedSizeList(item, _) => item.data_type(),
            DataType::List(item) => match item.data_type() {
                DataType::FixedSizeList(item, _) => item.data_type(),
                _ => return Err("Invalid multivector type".into()),
            },
            _ => return Err("Invalid vector type".into()),
        };
        if (element == &DataType::UInt8) != (distance == Some("hamming")) {
            return Err(
                "uint8 vectors require Hamming distance; Hamming requires uint8 vectors".into(),
            );
        }
        let mut query = table
            .query()
            .nearest_to(query_array(self.vector, element)?)
            .map_err(Failure::from_error)?
            .column(&column);
        for vector in self.additional_vectors {
            query = query
                .add_query_vector(query_array(vector, element)?)
                .map_err(Failure::from_error)?;
        }
        Ok(query)
    }

    fn column(&self, schema: &Schema, column: Option<&str>) -> Result<String, Failure> {
        if self.search_mode == SearchMode::Batch && schema.field_with_name("query_index").is_ok() {
            return Err("Batch search reserves the stored column name query_index".into());
        }
        if let Some(column) = column {
            let schema = LanceSchema::try_from(schema).map_err(Failure::from_error)?;
            let field = schema
                .field(column)
                .ok_or_else(|| format!("Unknown vector column: {column}"))?;
            let dimension = self.dimension(&field.data_type()).ok_or_else(|| {
                format!(
                    "Vector column '{column}' has a type incompatible with {} search",
                    self.search_mode.name()
                )
            })?;
            if dimension != self.vector.len() {
                return Err(self.dimension_error(&[(column.into(), dimension)]));
            }
            return Ok(column.into());
        }
        let mut candidates = Vec::new();
        for field in schema.fields() {
            self.collect(field, &mut Vec::new(), &mut candidates);
        }
        let matching: Vec<_> = candidates
            .iter()
            .filter(|(_, dimension)| *dimension == self.vector.len())
            .collect();
        match matching.as_slice() {
            [(column, _)] => Ok(column.clone()),
            [] if candidates.is_empty() => Err(format!(
                "No vector column has a type compatible with {} search",
                self.search_mode.name()
            )
            .into()),
            [] => Err(self.dimension_error(&candidates)),
            _ => Err("Expected exactly one matching vector column; specify vector_column".into()),
        }
    }

    fn dimension_error(&self, candidates: &[(String, usize)]) -> Failure {
        if let [(column, dimension)] = candidates {
            return format!(
                "Vector column '{column}' expects dimension {dimension}; query has dimension {}",
                self.vector.len()
            )
            .into();
        }
        let dimensions = candidates
            .iter()
            .map(|(column, dimension)| format!("'{column}': {dimension}"))
            .collect::<Vec<_>>()
            .join(", ");
        format!(
            "Query vector dimension {} does not match any compatible vector column ({dimensions})",
            self.vector.len()
        )
        .into()
    }

    fn collect(
        &self,
        field: &Field,
        path: &mut Vec<String>,
        candidates: &mut Vec<(String, usize)>,
    ) {
        path.push(field.name().clone());
        if let Some(dimension) = self.dimension(field.data_type()) {
            candidates.push((
                format_field_path(&path.iter().map(String::as_str).collect::<Vec<_>>()),
                dimension,
            ));
        } else if let DataType::Struct(fields) = field.data_type() {
            for child in fields {
                self.collect(child, path, candidates);
            }
        }
        path.pop();
    }

    fn dimension(&self, data_type: &DataType) -> Option<usize> {
        let vector = match (data_type, &self.search_mode) {
            (DataType::List(item), SearchMode::Multivector) => item.data_type(),
            (_, SearchMode::Batch | SearchMode::Single) => data_type,
            _ => return None,
        };
        match vector {
            DataType::FixedSizeList(item, dimension)
                if crate::value::vector_element_type(item.data_type())
                    && (self.search_mode != SearchMode::Multivector
                        || item.data_type() == &DataType::Float32) =>
            {
                Some(*dimension as usize)
            }
            _ => None,
        }
    }
}

// nearest_to normally coerces all input to f32. Preserve the stored vector type
// here to avoid losing f64 precision before the scanner sees the query.
struct TypedVector(arrow_array::ArrayRef);
impl lancedb::query::IntoQueryVector for TypedVector {
    fn to_query_vector(self, _: &DataType, _: &str) -> lancedb::Result<arrow_array::ArrayRef> {
        Ok(self.0)
    }
}
fn query_array(values: Vec<f64>, data_type: &DataType) -> Result<TypedVector, Failure> {
    use std::sync::Arc;
    let field = Field::new("query", data_type.clone(), false);
    for value in &values {
        let mut value = if data_type == &DataType::UInt8 {
            if value.fract() != 0.0 || !(0.0..=255.0).contains(value) {
                return Err("Binary query vectors require integers from 0 through 255".into());
            }
            serde_json::json!(*value as u8)
        } else {
            serde_json::json!(value)
        };
        crate::value::normalize(&mut value, &field)?;
    }
    let array: arrow_array::ArrayRef = Arc::new(arrow_array::Float64Array::from(values));
    Ok(TypedVector(
        arrow_cast::cast(&array, data_type).map_err(Failure::from_error)?,
    ))
}
