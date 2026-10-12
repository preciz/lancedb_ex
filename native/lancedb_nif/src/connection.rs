use crate::error::Failure;
mod config;
pub(crate) use config::ConnectionConfig;

use crate::resources::{atoms, ConnectionResource};
use rustler::{Atom, Env, ResourceArc, Term};
use std::collections::HashMap;

#[derive(Default, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct ConnectOptions {
    read_consistency_interval_ms: Option<u32>,
    timeout_ms: Option<u32>,
    index_cache_size_bytes: Option<usize>,
    metadata_cache_size_bytes: Option<usize>,
}

#[rustler::nif]
fn connect(
    env: Env<'_>,
    uri: String,
    storage_options: HashMap<String, String>,
    options: String,
    reference: Term<'_>,
) -> Result<Atom, Failure> {
    let options: ConnectOptions = serde_json::from_str(&options).map_err(Failure::from_error)?;
    let deadline = crate::execution::ExecutionOptions {
        timeout_ms: options.timeout_ms,
    }
    .deadline_for("Connection");
    crate::read_task::spawn(env, reference, deadline, async move {
        let config =
            ConnectionConfig::new(uri, storage_options, options.read_consistency_interval_ms);
        let session = crate::cache::session(
            options.index_cache_size_bytes,
            options.metadata_cache_size_bytes,
        );
        let conn = config.connect(session).await.map_err(|e| {
            let message = format!("Failed to connect: {e}");
            Failure::from_error(e).context(message)
        })?;
        Ok(ResourceArc::new(ConnectionResource { conn, config }))
    })
}

#[rustler::nif(schedule = "DirtyCpu")]
fn table_names(
    env: Env<'_>,
    conn_res: ResourceArc<ConnectionResource>,
    start_after: Option<String>,
    limit: Option<u32>,
    reference: Term<'_>,
) -> Result<Atom, Failure> {
    crate::request::read(env, reference, async move {
        let mut builder = conn_res.conn.table_names();
        if let Some(start_after) = start_after {
            builder = builder.start_after(start_after);
        }
        if let Some(limit) = limit {
            builder = builder.limit(limit);
        }
        match builder.execute().await {
            Ok(names) => Ok(names),
            Err(e) => {
                let message = format!("Failed to list tables: {e}");
                Err(Failure::from_error(e).context(message))
            }
        }
    })
}

#[rustler::nif(schedule = "DirtyCpu")]
fn drop_table(
    env: Env<'_>,
    conn_res: ResourceArc<ConnectionResource>,
    name: String,
    reference: Term<'_>,
) -> Result<Atom, Failure> {
    crate::request::write(env, reference, move |permit| async move {
        let _permit = permit;

        match conn_res.conn.drop_table(&name, &[]).await {
            Ok(()) => Ok(atoms::dropped()),
            Err(e) => {
                let message = format!("Failed to drop table: {e}");
                Err(Failure::from_error(e).context(message))
            }
        }
    })
}
