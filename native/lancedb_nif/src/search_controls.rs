use crate::error::Failure;
use lancedb::query::{QueryBase, VectorQuery};

#[derive(Clone, Copy, serde::Deserialize)]
pub(crate) struct ReadControls {
    pub(crate) offset: usize,
    pub(crate) with_row_id: bool,
    pub(crate) prefilter: bool,
    fast_search: bool,
}

impl ReadControls {
    pub(crate) fn apply<Q: QueryBase>(self, mut query: Q) -> Q {
        query = query.offset(self.offset);
        if self.with_row_id {
            query = query.with_row_id();
        }
        if !self.prefilter {
            query = query.postfilter();
        }
        if self.fast_search {
            query = query.fast_search();
        }
        query
    }
}

#[derive(Clone, Copy, serde::Deserialize)]
pub(crate) struct Probes {
    minimum: usize,
    maximum: Option<usize>,
}

impl Probes {
    pub(crate) fn apply(self, query: VectorQuery) -> Result<VectorQuery, Failure> {
        // Clear the default cap before setting a minimum above it; apply the
        // requested cap last so intermediate builder states stay valid.
        query
            .maximum_nprobes(None)
            .and_then(|query| query.minimum_nprobes(self.minimum))
            .and_then(|query| query.maximum_nprobes(self.maximum))
            .map_err(Failure::from_error)
    }
}
