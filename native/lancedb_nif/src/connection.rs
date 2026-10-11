use crate::error::Failure;
mod config;
pub(crate) use config::ConnectionConfig;

use crate::resources::{atoms, ConnectionResource, RUNTIME};
use rustler::{Atom, Env, ResourceArc, Term};
use std::collections::HashMap;

#[derive(Default, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct ConnectOptions {
    read_consistency_interval_ms: Option<u32>,
    timeout_ms: Option<u32>,
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
        let conn = config.connect(None).await.map_err(|e| {
            let message = format!("Failed to connect: {e}");
            Failure::from_error(e).context(message)
        })?;
        Ok(ResourceArc::new(ConnectionResource { conn, config }))
    })
}

#[rustler::nif(schedule = "DirtyCpu")]
fn table_names(
    conn_res: ResourceArc<ConnectionResource>,
    start_after: Option<String>,
    limit: Option<u32>,
) -> Result<Vec<String>, Failure> {
    RUNTIME.block_on(async {
        let _permit = crate::execution::Permit::read()?;
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
fn drop_table(conn_res: ResourceArc<ConnectionResource>, name: String) -> Result<Atom, Failure> {
    RUNTIME.block_on(async {
        let _permit = crate::execution::Permit::write()?;
        match conn_res.conn.drop_table(&name, &[]).await {
            Ok(()) => Ok(atoms::dropped()),
            Err(e) => {
                let message = format!("Failed to drop table: {e}");
                Err(Failure::from_error(e).context(message))
            }
        }
    })
}
