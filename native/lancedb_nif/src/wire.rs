//! Explicit tagged write protocol. Engine values are encoded only at this boundary.
use crate::{outcome::WriteOutput, resources::TableResource};
use rustler::{Encoder, Env, ResourceArc, Term};

rustler::atoms! { write, version, table, execution, invalid_data, not_found, already_exists, conflict, timeout, unsupported, storage, cancelled, busy }

pub(crate) struct Counts<T>(pub T);
pub(crate) struct Version<T = u64>(pub T);

impl<T: Encoder> Encoder for Counts<T> {
    fn encode<'a>(&self, env: Env<'a>) -> Term<'a> {
        (write(), &self.0).encode(env)
    }
}
impl<T: Encoder> Encoder for Version<T> {
    fn encode<'a>(&self, env: Env<'a>) -> Term<'a> {
        (version(), &self.0).encode(env)
    }
}
impl Encoder for WriteOutput {
    fn encode<'a>(&self, env: Env<'a>) -> Term<'a> {
        match self {
            Self::Add(value) => (write(), value).encode(env),
            Self::Merge(value) => (write(), value).encode(env),
            Self::Columns(value) => (version(), value).encode(env),
            Self::Created(value) => (
                table(),
                ResourceArc::new(TableResource {
                    table: value.clone(),
                }),
            )
                .encode(env),
        }
    }
}

// Encoding only clones the table Arc; it does not enter SDK code.
impl std::panic::RefUnwindSafe for WriteOutput {}
impl std::panic::UnwindSafe for WriteOutput {}

use crate::error::Failure;

impl Encoder for Failure {
    fn encode<'a>(&self, env: Env<'a>) -> Term<'a> {
        #[derive(rustler::NifMap)]
        struct WireError<'a> {
            kind: rustler::Atom,
            message: String,
            details: std::collections::HashMap<rustler::Atom, Term<'a>>,
        }
        let kind = match self.kind {
            "invalid_data" => invalid_data(),
            "not_found" => not_found(),
            "already_exists" => already_exists(),
            "conflict" => conflict(),
            "timeout" => timeout(),
            "unsupported" => unsupported(),
            "busy" => busy(),
            "storage" => storage(),
            "cancelled" => cancelled(),
            _ => execution(),
        };
        let mut details = std::collections::HashMap::new();
        for (key, value) in [
            ("phase", self.phase),
            ("commit_outcome", self.commit_outcome),
        ] {
            if let Some(value) = value {
                details.insert(
                    rustler::Atom::from_str(env, key).unwrap(),
                    rustler::Atom::from_str(env, value).unwrap().encode(env),
                );
            }
        }
        if let Some(rows) = self.processed_rows {
            details.insert(
                rustler::Atom::from_str(env, "processed_rows").unwrap(),
                rows.encode(env),
            );
        }
        WireError {
            kind,
            message: self.message.clone(),
            details,
        }
        .encode(env)
    }
}
