//! Reference-tagged replies let BEAM callers wait without retaining a scheduler.
use crate::resources::RUNTIME;
use rustler::{Encoder, Env, LocalPid, OwnedEnv, Term};
use std::future::Future;

pub(crate) struct Reply {
    pid: LocalPid,
    env: OwnedEnv,
    reference: rustler::env::SavedTerm,
}

impl Reply {
    pub(crate) fn new(env: Env<'_>, reference: Term<'_>) -> Self {
        let message_env = OwnedEnv::new();
        let reference = message_env.save(reference);
        Self {
            pid: env.pid(),
            env: message_env,
            reference,
        }
    }

    pub(crate) fn send(&mut self, result: impl Encoder) -> Result<(), rustler::env::SendError> {
        self.env.send_and_clear(&self.pid, |env| {
            (self.reference.load(env), result).encode(env)
        })
    }
}

pub(crate) fn spawn<T: Encoder + Send + 'static>(
    env: Env<'_>,
    reference: Term<'_>,
    operation: impl Future<Output = T> + Send + 'static,
) {
    let mut reply = Reply::new(env, reference);
    RUNTIME.spawn(async move {
        let result = operation.await;
        let _ = reply.send(result);
    });
}
