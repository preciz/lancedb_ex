//! Wire requests contain common options once, independent of execution mode.
use super::vector_input::VectorInput;
use crate::execution::ExecutionOptions;
use crate::fts::TextQuery;
use crate::hybrid::HybridControls;
use crate::projection::ProjectionRequest;
use crate::search_controls::{Probes, ReadControls};

#[derive(serde::Deserialize)]
pub(crate) struct QueryRequest {
    pub(crate) execution: ExecutionOptions,
    #[serde(flatten)]
    pub(super) options: QueryOptions,
    #[serde(flatten)]
    pub(super) kind: QueryKind,
}

#[derive(serde::Deserialize)]
pub(super) struct QueryOptions {
    pub(super) projection: ProjectionRequest,
    pub(super) read: ReadControls,
    pub(super) order_by: crate::ranking::Ordering,
}

#[derive(serde::Deserialize)]
pub(super) struct SearchControls {
    pub(super) vector_column: Option<String>,
    pub(super) nprobes: Option<u32>,
    pub(super) probes: Option<Probes>,
    pub(super) distance_range: Option<(Option<f32>, Option<f32>)>,
    pub(super) ef: Option<u32>,
    pub(super) approx_mode: Option<lancedb::ApproxMode>,
    pub(super) refine_factor: Option<u32>,
    pub(super) bypass_vector_index: bool,
    pub(super) hybrid: Option<HybridControls>,
}

#[derive(serde::Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub(super) enum QueryKind {
    Take {
        row_ids: Option<Vec<u64>>,
        row_offsets: Option<Vec<u64>>,
    },
    Scan {
        filter: Option<String>,
        limit: Option<usize>,
    },
    Text {
        text: TextQuery,
        wand_factor: Option<f32>,
        column: Option<String>,
        limit: usize,
        filter: Option<String>,
    },
    Vector {
        #[serde(flatten)]
        input: VectorInput,
        limit: usize,
        filter: Option<String>,
        distance_type: Option<String>,
        controls: Box<SearchControls>,
    },
}
