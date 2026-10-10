//! SDK adapters for the pinned versions.
//! See the SDK compatibility section in README.md for removal criteria.
pub(crate) mod fts;
pub(crate) mod statistics;
pub(crate) mod storage;
pub(crate) use statistics::training_loss;
pub(crate) mod table;
pub(crate) mod update;
