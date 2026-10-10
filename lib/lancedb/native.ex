defmodule LanceDB.Native do
  @moduledoc false
  build_file = Path.expand("../../native-build.exs", __DIR__)
  @external_resource build_file
  force_build = System.get_env("LANCEDB_BUILD") in ["1", "true"]

  if force_build do
    # Rustler 0.38 runs unlocked metadata/rustc commands. Resolve the complete
    # workspace with --locked first, and track inputs outside the crate directory.
    workspace_manifest = Path.expand("../../Cargo.toml", __DIR__)
    workspace_lock = Path.expand("../../Cargo.lock", __DIR__)
    @external_resource workspace_manifest
    @external_resource workspace_lock

    case System.cmd("cargo", ["metadata", "--locked", "--format-version=1"],
           cd: Path.dirname(workspace_manifest),
           stderr_to_stdout: true
         ) do
      {_, 0} ->
        :ok

      {output, _} ->
        raise "LanceDB maintainer build requires a current committed Cargo.lock:\n#{output}"
    end
  end

  build =
    if File.exists?(build_file) do
      {build, _binding} = Code.eval_file(build_file)
      build
    else
      if not force_build do
        raise "LanceDB native-build.exs is missing. Use a checkout with a trusted native build manifest."
      end

      %{id: "0.0.0", default_url: ""}
    end

  if not force_build do
    revision = Map.get(build, :source_revision)

    if not (is_binary(revision) and Regex.match?(~r/\A[0-9a-f]{40}\z/, revision)) or
         Map.get(build, :id) != "0.0.0+#{revision}" do
      raise "LanceDB native build ID must be 0.0.0+<full-source-revision> matching source_revision. Invalid or dev versions never opt into Rust compilation."
    end
  end

  environment_url = System.get_env("LANCEDB_NIF_BASE_URL")
  configured_url = Application.compile_env(:lancedb, :nif_base_url, :default)

  base_url =
    environment_url ||
      if(configured_url == :default, do: build.default_url, else: configured_url)

  global_force_build =
    Application.compile_env(
      :rustler_precompiled,
      :force_build_all,
      System.get_env("RUSTLER_PRECOMPILED_FORCE_BUILD_ALL") in ["1", "true"]
    )

  if global_force_build and not force_build do
    raise "LanceDB source builds require explicit maintainer opt-in: LANCEDB_BUILD=true."
  end

  if not force_build and base_url in [nil, ""] do
    raise """
    LanceDB requires a precompiled NIF artifact URL. Set LANCEDB_NIF_BASE_URL or
    configure :lancedb, :nif_base_url. Missing configuration does not trigger a
    source build. Maintainers can explicitly build with LANCEDB_BUILD=true.
    """
  end

  checksum_file = Path.expand("../../checksum-Elixir.LanceDB.Native.exs", __DIR__)
  @external_resource checksum_file

  if not force_build and not File.exists?(checksum_file) do
    raise "LanceDB trusted checksum manifest is missing. Use a complete native build checkout; installation never generates checksums or compiles Rust."
  end

  use RustlerPrecompiled,
    otp_app: :lancedb,
    crate: "lancedb_nif",
    version: build.id,
    base_url: base_url,
    force_build: force_build,
    nif_versions: ["2.15"],
    targets: ~w(
      x86_64-unknown-linux-gnu
      aarch64-unknown-linux-gnu
      aarch64-apple-darwin
      x86_64-apple-darwin
    )

  def blob_columns(_table), do: :erlang.nif_error(:nif_not_loaded)
  def fetch_blobs(_table, _column, _row_ids), do: :erlang.nif_error(:nif_not_loaded)
  def fetch_blob_ranges(_table, _column, _requests), do: :erlang.nif_error(:nif_not_loaded)
  def fetch_blob_files(_table, _column, _row_ids), do: :erlang.nif_error(:nif_not_loaded)
  def blob_read(_blob, _length), do: :erlang.nif_error(:nif_not_loaded)
  def blob_read_ranges(_blob, _ranges), do: :erlang.nif_error(:nif_not_loaded)
  def blob_seek(_blob, _offset), do: :erlang.nif_error(:nif_not_loaded)
  def blob_tell(_blob), do: :erlang.nif_error(:nif_not_loaded)
  def blob_close(_blob), do: :erlang.nif_error(:nif_not_loaded)
  def blob_is_closed(_blob), do: :erlang.nif_error(:nif_not_loaded)
  def blob_size(_blob), do: :erlang.nif_error(:nif_not_loaded)
  def blob_info(_blob), do: :erlang.nif_error(:nif_not_loaded)

  def create_index_async(_table, _column, _options), do: :erlang.nif_error(:nif_not_loaded)
  def job_status(_job), do: :erlang.nif_error(:nif_not_loaded)
  def job_wait(_job, _timeout), do: :erlang.nif_error(:nif_not_loaded)
  def job_cancel(_job), do: :erlang.nif_error(:nif_not_loaded)

  def create_stream_open(_conn, _name, _schema, _metadata, _options),
    do: :erlang.nif_error(:nif_not_loaded)

  def merge_stream_open(_table, _keys, _options), do: :erlang.nif_error(:nif_not_loaded)
  def columns_stream_open(_table, _schema, _version), do: :erlang.nif_error(:nif_not_loaded)

  def execution_limits(_reads, _writes), do: :erlang.nif_error(:nif_not_loaded)

  def metrics_enable, do: :erlang.nif_error(:nif_not_loaded)
  def metrics_catalog, do: :erlang.nif_error(:nif_not_loaded)
  def metrics_snapshot, do: :erlang.nif_error(:nif_not_loaded)

  def connect(_uri, _storage_options, _read_consistency_interval_ms),
    do: :erlang.nif_error(:nif_not_loaded)

  def table_names(_conn, _start_after, _limit), do: :erlang.nif_error(:nif_not_loaded)
  def open_table(_conn, _name, _options), do: :erlang.nif_error(:nif_not_loaded)
  def drop_table(_conn, _name), do: :erlang.nif_error(:nif_not_loaded)

  def batch_infer(_rows), do: :erlang.nif_error(:nif_not_loaded)
  def schema_validate(_schema), do: :erlang.nif_error(:nif_not_loaded)

  def create_index(_table, _column, _options), do: :erlang.nif_error(:nif_not_loaded)
  def list_indices(_table), do: :erlang.nif_error(:nif_not_loaded)
  def table_stats(_table), do: :erlang.nif_error(:nif_not_loaded)
  def index_stats(_table, _name), do: :erlang.nif_error(:nif_not_loaded)
  def index_readiness(_table, _names, _timeout), do: :erlang.nif_error(:nif_not_loaded)
  def drop_index(_table, _name), do: :erlang.nif_error(:nif_not_loaded)

  def optimize_indices(_table, _options), do: :erlang.nif_error(:nif_not_loaded)
  def compact_files(_table, _options), do: :erlang.nif_error(:nif_not_loaded)
  def cleanup_old_versions(_table, _options), do: :erlang.nif_error(:nif_not_loaded)

  def uses_v2_manifest_paths(_table), do: :erlang.nif_error(:nif_not_loaded)
  def migrate_manifest_paths_v2(_table), do: :erlang.nif_error(:nif_not_loaded)

  def ingest_open(_table, _options, _progress), do: :erlang.nif_error(:nif_not_loaded)
  def ingest_push(_writer, _records), do: :erlang.nif_error(:nif_not_loaded)
  def ingest_finish(_writer), do: :erlang.nif_error(:nif_not_loaded)
  def ingest_abort(_writer), do: :erlang.nif_error(:nif_not_loaded)
  def delete_rows_result(_table, _predicate), do: :erlang.nif_error(:nif_not_loaded)

  def count_rows(_table, _filter), do: :erlang.nif_error(:nif_not_loaded)

  def query_execute(_table, _request, _operation), do: :erlang.nif_error(:nif_not_loaded)
  def rerank_candidates(_table, _request, _analyze), do: :erlang.nif_error(:nif_not_loaded)

  def rerank_finish(_resource, _scores, _operation, _batch_size),
    do: :erlang.nif_error(:nif_not_loaded)

  def stream_open(_table, _request, _batch_size), do: :erlang.nif_error(:nif_not_loaded)
  def stream_close(cursor), do: cursor_call(:stream_close, cursor)
  def stream_close(_cursor, _reference), do: :erlang.nif_error(:nif_not_loaded)

  def add_columns(_table, _expressions), do: :erlang.nif_error(:nif_not_loaded)
  def alter_columns(_table, _changes), do: :erlang.nif_error(:nif_not_loaded)
  def drop_columns(_table, _columns), do: :erlang.nif_error(:nif_not_loaded)

  def tokenize(_text, _analyzer), do: :erlang.nif_error(:nif_not_loaded)
  def tokenize_table(_table, _text, _selector, _name), do: :erlang.nif_error(:nif_not_loaded)

  def prewarm_index(_table, _name), do: :erlang.nif_error(:nif_not_loaded)
  def list_tags(_table), do: :erlang.nif_error(:nif_not_loaded)
  def tag_version(_table, _name), do: :erlang.nif_error(:nif_not_loaded)
  def tag_action(_table, _action, _name, _version), do: :erlang.nif_error(:nif_not_loaded)

  def table_version(_table), do: :erlang.nif_error(:nif_not_loaded)
  def list_versions(_table), do: :erlang.nif_error(:nif_not_loaded)
  def snapshot_table(_table, _version, _tag), do: :erlang.nif_error(:nif_not_loaded)
  def restore_version(_table, _version), do: :erlang.nif_error(:nif_not_loaded)

  def get_schema_json(_table), do: :erlang.nif_error(:nif_not_loaded)

  def update_rows(_table, _changes, _filter), do: :erlang.nif_error(:nif_not_loaded)

  def create_branch(_table, _name, _source), do: :erlang.nif_error(:nif_not_loaded)
  def checkout_branch(_table, _name, _version), do: :erlang.nif_error(:nif_not_loaded)
  def list_branches(_table), do: :erlang.nif_error(:nif_not_loaded)
  def current_branch(_table), do: :erlang.nif_error(:nif_not_loaded)
  def delete_branch(_table, _name), do: :erlang.nif_error(:nif_not_loaded)
  def table_uri(_table), do: :erlang.nif_error(:nif_not_loaded)

  def clone_table(_conn, _name, _source_uri, _version, _tag),
    do: :erlang.nif_error(:nif_not_loaded)

  def schema_metadata(_table), do: :erlang.nif_error(:nif_not_loaded)
  def update_schema_metadata(_table, _changes, _replace), do: :erlang.nif_error(:nif_not_loaded)

  def update_field_metadata(_table, _path, _changes, _replace),
    do: :erlang.nif_error(:nif_not_loaded)

  def stream_next_batch(cursor), do: cursor_call(:stream_next_batch, cursor)
  def stream_next_batch(_cursor, _reference), do: :erlang.nif_error(:nif_not_loaded)

  defp cursor_call(operation, cursor) do
    reference = make_ref()

    case apply(__MODULE__, operation, [cursor, reference]) do
      {:ok, :ok} ->
        receive do
          {^reference, result} -> result
        end

      error ->
        error
    end
  end

  def batch_rows(_batch), do: :erlang.nif_error(:nif_not_loaded)

  def batch_size(_batch), do: :erlang.nif_error(:nif_not_loaded)

  def batch_schema(_batch), do: :erlang.nif_error(:nif_not_loaded)

  # Loader compatibility: the committed precompiled build still exports this NIF.
  # Remove this stub after a replacement native build becomes the default.
  def batch_from_json(_rows_json, _schema_json, _metadata), do: :erlang.nif_error(:nif_not_loaded)

  def ingest_push_batch(_writer, _batch), do: :erlang.nif_error(:nif_not_loaded)

  def batch_to_ipc(_batch), do: :erlang.nif_error(:nif_not_loaded)

  def batch_from_ipc(_bytes), do: :erlang.nif_error(:nif_not_loaded)

  def batch_from_terms(_records, _schema_json, _metadata), do: :erlang.nif_error(:nif_not_loaded)
end
