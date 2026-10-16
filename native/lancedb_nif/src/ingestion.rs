//! Shared ingestion execution, configuration and native progress delivery.
use crate::error::Failure;
pub(crate) mod worker;
use lance::dataset::WriteParams;
use lancedb::table::{AddDataBuilder, AddDataMode, WriteOptions};
use rustler::{Encoder, LocalPid, OwnedEnv, Term};

rustler::atoms! { lancedb_write_progress }

#[derive(Default, serde::Deserialize)]
#[serde(default, deny_unknown_fields)]
pub(crate) struct Options {
    mode: Option<String>,
    blob: crate::blob::write_options::WriteOptions,
    write_parallelism: Option<usize>,
    max_rows_per_file: Option<usize>,
    max_rows_per_group: Option<usize>,
    max_bytes_per_file: Option<usize>,
}

impl Options {
    pub(crate) fn is_overwrite(&self) -> bool {
        self.mode.as_deref() == Some("overwrite")
    }

    pub(crate) fn parse(json: &str) -> Result<Self, Failure> {
        let options: Self = serde_json::from_str(json).map_err(Failure::from_error)?;
        if options
            .mode
            .as_deref()
            .is_some_and(|v| !matches!(v, "append" | "overwrite"))
        {
            return Err("Invalid write mode".into());
        }
        for value in [
            options.write_parallelism,
            options.max_rows_per_file,
            options.max_rows_per_group,
            options.max_bytes_per_file,
        ]
        .into_iter()
        .flatten()
        {
            if value == 0 || value > u32::MAX as usize {
                return Err("Write sizes must be positive uint32 integers".into());
            }
        }
        Ok(options)
    }

    pub(crate) fn apply(
        self,
        mut builder: AddDataBuilder,
        progress: Option<ProgressTarget>,
    ) -> Result<AddDataBuilder, Failure> {
        let mut params = WriteParams {
            mode: if self.mode.as_deref() == Some("overwrite") {
                lance::dataset::WriteMode::Overwrite
            } else {
                lance::dataset::WriteMode::Append
            },
            ..Default::default()
        };
        self.blob.apply(&mut params)?;
        if let Some(v) = self.max_rows_per_file {
            params.max_rows_per_file = v;
        }
        if let Some(v) = self.max_rows_per_group {
            params.max_rows_per_group = v;
        }
        if let Some(v) = self.max_bytes_per_file {
            params.max_bytes_per_file = v;
        }
        builder = builder.write_options(WriteOptions {
            lance_write_params: Some(params),
        });
        if self.mode.as_deref() == Some("overwrite") {
            builder = builder.mode(AddDataMode::Overwrite);
        }
        if let Some(v) = self.write_parallelism {
            builder = builder.write_parallelism(v);
        }
        if let Some(mut target) = progress {
            builder = builder.progress(move |p| target.send(p));
        }
        Ok(builder)
    }
}

pub(crate) struct ProgressTarget {
    pid: LocalPid,
    tag_env: OwnedEnv,
    tag: rustler::env::SavedTerm,
    message_env: OwnedEnv,
}

impl ProgressTarget {
    pub(crate) fn new(target: (LocalPid, Term<'_>)) -> Self {
        let tag_env = OwnedEnv::new();
        let tag = tag_env.save(target.1);
        Self {
            pid: target.0,
            tag_env,
            tag,
            message_env: OwnedEnv::new(),
        }
    }

    fn send(&mut self, p: &lancedb::table::write_progress::WriteProgress) {
        let progress = Progress {
            elapsed_ms: p.elapsed().as_millis() as u64,
            output_rows: p.output_rows(),
            output_bytes: p.output_bytes(),
            total_rows: p.total_rows(),
            active_tasks: p.active_tasks(),
            total_tasks: p.total_tasks(),
            done: p.done(),
        };
        let _ = self.message_env.send_and_clear(&self.pid, |env| {
            self.tag_env.run(|tag_env| {
                (
                    lancedb_write_progress(),
                    self.tag.load(tag_env).in_env(env),
                    progress,
                )
                    .encode(env)
            })
        });
    }
}

#[derive(rustler::NifMap)]
struct Progress {
    elapsed_ms: u64,
    output_rows: usize,
    output_bytes: usize,
    total_rows: Option<usize>,
    active_tasks: usize,
    total_tasks: usize,
    done: bool,
}

#[derive(Clone, Debug, rustler::NifMap)]
pub(crate) struct WriteResult {
    pub inserted: usize,
    pub version: u64,
}

/// Both eager and streamed input apply the same SDK options and commit handling.
/// Callers execute this on Tokio threads so progress delivery stays off BEAM schedulers.
pub(crate) async fn execute(
    table: &lancedb::Table,
    data: impl lancedb::data::scannable::Scannable + 'static,
    options: Options,
    progress: Option<ProgressTarget>,
) -> Result<u64, Failure> {
    options
        .apply(table.add(data), progress)?
        .execute()
        .await
        .map(|result| result.version)
        .map_err(Failure::write_error)
}
