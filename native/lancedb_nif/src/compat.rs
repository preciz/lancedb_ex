//! SDK adapters for the pinned versions.
//! See docs/internals.md's SDK compatibility section for removal criteria.
pub(crate) mod fts;
pub(crate) mod ipc;
pub(crate) mod statistics;
pub(crate) mod storage;
pub(crate) use statistics::training_loss;
pub(crate) mod table;
pub(crate) mod update;
