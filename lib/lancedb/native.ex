defmodule LanceDB.Native do
  @moduledoc false
  @type error :: %{kind: atom(), message: String.t(), details: map()}
  @type result(value) :: {:ok, value} | {:error, error()}
  @type counts_output :: {:write, %{atom() => non_neg_integer() | nil}}
  @type version_output :: {:version, pos_integer()}
  @type write_output :: counts_output() | version_output() | {:table, reference()}
  @type query_operation :: :explain | :explain_verbose | :analyze | :schema

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
  configured_url = Application.compile_env(:lancedb_ex, :nif_base_url, :default)

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
    configure :lancedb_ex, :nif_base_url. Missing configuration does not trigger a
    source build. Maintainers can explicitly build with LANCEDB_BUILD=true.
    """
  end

  checksum_file = Path.expand("../../checksum-Elixir.LanceDB.Native.exs", __DIR__)
  @external_resource checksum_file

  if not force_build and not File.exists?(checksum_file) do
    raise "LanceDB trusted checksum manifest is missing. Use a complete native build checkout; installation never generates checksums or compiles Rust."
  end

  use RustlerPrecompiled,
    otp_app: :lancedb_ex,
    crate: "lancedb_nif",
    version: build.id,
    base_url: base_url,
    force_build: force_build,
    nif_versions: ["2.15"],
    targets: ~w(
      x86_64-unknown-linux-gnu
      aarch64-unknown-linux-gnu
    )

  @spec blob_columns(reference()) :: result([String.t()])
  def blob_columns(_table), do: :erlang.nif_error(:nif_not_loaded)
  @spec fetch_blobs(reference(), String.t(), [non_neg_integer()]) :: result([binary() | nil])
  def fetch_blobs(_table, _column, _row_ids), do: :erlang.nif_error(:nif_not_loaded)

  @spec fetch_blob_ranges(reference(), String.t(), [LanceDB.Blob.request()]) ::
          result([binary() | nil])
  def fetch_blob_ranges(_table, _column, _requests), do: :erlang.nif_error(:nif_not_loaded)

  @spec fetch_blob_files(reference(), String.t(), [non_neg_integer()]) ::
          result([reference() | nil])
  def fetch_blob_files(_table, _column, _row_ids), do: :erlang.nif_error(:nif_not_loaded)
  @spec blob_read(reference(), non_neg_integer() | nil) :: result(binary())
  def blob_read(_blob, _length), do: :erlang.nif_error(:nif_not_loaded)
  @spec blob_read_ranges(reference(), [LanceDB.Blob.byte_range()]) :: result([binary()])
  def blob_read_ranges(_blob, _ranges), do: :erlang.nif_error(:nif_not_loaded)
  @spec blob_seek(reference(), non_neg_integer()) :: result(:ok)
  def blob_seek(_blob, _offset), do: :erlang.nif_error(:nif_not_loaded)
  @spec blob_tell(reference()) :: result(non_neg_integer())
  def blob_tell(_blob), do: :erlang.nif_error(:nif_not_loaded)
  @spec blob_close(reference()) :: result(:ok)
  def blob_close(_blob), do: :erlang.nif_error(:nif_not_loaded)
  @spec blob_is_closed(reference()) :: boolean()
  def blob_is_closed(_blob), do: :erlang.nif_error(:nif_not_loaded)
  @spec blob_size(reference()) :: non_neg_integer()
  def blob_size(_blob), do: :erlang.nif_error(:nif_not_loaded)
  @spec blob_info(reference()) :: String.t()
  def blob_info(_blob), do: :erlang.nif_error(:nif_not_loaded)

  @spec create_index_async(reference(), String.t(), String.t()) :: result(reference())
  def create_index_async(_table, _column, _options), do: :erlang.nif_error(:nif_not_loaded)
  @spec job_status(reference()) :: result(String.t())
  def job_status(_job), do: :erlang.nif_error(:nif_not_loaded)
  @spec job_wait(reference(), non_neg_integer() | nil) :: result(:created)
  def job_wait(job, timeout), do: async_call(:job_wait, [job, timeout])
  @spec job_wait(reference(), non_neg_integer() | nil, reference()) :: result(:ok)
  def job_wait(_job, _timeout, _reference), do: :erlang.nif_error(:nif_not_loaded)
  @spec job_cancel(reference()) :: result(:ok)
  def job_cancel(_job), do: :erlang.nif_error(:nif_not_loaded)

  @spec create_stream_open(
          reference(),
          String.t(),
          String.t(),
          %{String.t() => String.t()},
          String.t()
        ) :: result(reference())
  def create_stream_open(_conn, _name, _schema, _metadata, _options),
    do: :erlang.nif_error(:nif_not_loaded)

  @spec merge_stream_open(reference(), [String.t()], String.t()) :: result(reference())
  def merge_stream_open(_table, _keys, _options), do: :erlang.nif_error(:nif_not_loaded)
  @spec columns_stream_open(reference(), String.t(), pos_integer() | nil) :: result(reference())
  def columns_stream_open(_table, _schema, _version), do: :erlang.nif_error(:nif_not_loaded)

  @spec execution_limits(pos_integer(), pos_integer()) :: result(:ok)
  def execution_limits(_reads, _writes), do: :erlang.nif_error(:nif_not_loaded)

  @spec metrics_enable() :: result(boolean())
  def metrics_enable, do: :erlang.nif_error(:nif_not_loaded)
  @spec metrics_catalog() :: String.t()
  def metrics_catalog, do: :erlang.nif_error(:nif_not_loaded)
  @spec metrics_snapshot() :: String.t()
  def metrics_snapshot, do: :erlang.nif_error(:nif_not_loaded)

  @spec connect(String.t(), %{String.t() => String.t()}, String.t()) :: result(reference())
  def connect(uri, storage_options, options),
    do: async_call(:connect, [uri, storage_options, options])

  @spec connect(String.t(), %{String.t() => String.t()}, String.t(), reference()) :: result(:ok)
  def connect(_uri, _storage_options, _options, _reference),
    do: :erlang.nif_error(:nif_not_loaded)

  @spec table_names(reference(), String.t() | nil, pos_integer() | nil) :: result([String.t()])
  def table_names(_conn, _start_after, _limit), do: :erlang.nif_error(:nif_not_loaded)
  @spec open_table(reference(), String.t(), String.t()) :: result(reference())
  def open_table(conn, name, options), do: async_call(:open_table, [conn, name, options])
  @spec open_table(reference(), String.t(), String.t(), reference()) :: result(:ok)
  def open_table(_conn, _name, _options, _reference), do: :erlang.nif_error(:nif_not_loaded)
  @spec drop_table(reference(), String.t()) :: result(:dropped)
  def drop_table(_conn, _name), do: :erlang.nif_error(:nif_not_loaded)

  @spec batch_infer([map()]) :: result(reference())
  def batch_infer(_rows), do: :erlang.nif_error(:nif_not_loaded)
  @spec schema_validate(String.t()) :: result(:ok)
  def schema_validate(_schema), do: :erlang.nif_error(:nif_not_loaded)

  @spec create_index(reference(), String.t(), String.t()) :: result(:created)
  def create_index(_table, _column, _options), do: :erlang.nif_error(:nif_not_loaded)
  @spec list_indices(reference()) :: result(String.t())
  def list_indices(_table), do: :erlang.nif_error(:nif_not_loaded)
  @spec table_stats(reference()) :: result(String.t())
  def table_stats(_table), do: :erlang.nif_error(:nif_not_loaded)
  @spec cache_stats(reference()) :: result(String.t())
  def cache_stats(_table), do: :erlang.nif_error(:nif_not_loaded)
  @spec index_stats(reference(), String.t()) :: result(String.t())
  def index_stats(_table, _name), do: :erlang.nif_error(:nif_not_loaded)
  @spec index_readiness(reference(), [String.t()], pos_integer()) :: result([String.t()])
  def index_readiness(_table, _names, _timeout), do: :erlang.nif_error(:nif_not_loaded)
  @spec drop_index(reference(), String.t()) :: result(:dropped)
  def drop_index(_table, _name), do: :erlang.nif_error(:nif_not_loaded)

  @spec optimize_indices(reference(), String.t()) :: result(:optimized)
  def optimize_indices(_table, _options), do: :erlang.nif_error(:nif_not_loaded)
  @spec compact_files(reference(), String.t()) :: result(String.t())
  def compact_files(_table, _options), do: :erlang.nif_error(:nif_not_loaded)
  @spec cleanup_old_versions(reference(), String.t()) :: result(String.t())
  def cleanup_old_versions(_table, _options), do: :erlang.nif_error(:nif_not_loaded)

  @spec uses_v2_manifest_paths(reference()) :: result(boolean())
  def uses_v2_manifest_paths(_table), do: :erlang.nif_error(:nif_not_loaded)
  @spec migrate_manifest_paths_v2(reference()) :: result(:migrated)
  def migrate_manifest_paths_v2(_table), do: :erlang.nif_error(:nif_not_loaded)

  @spec ingest_open(reference(), String.t(), {pid(), term()} | nil) :: result(reference())
  def ingest_open(_table, _options, _progress), do: :erlang.nif_error(:nif_not_loaded)
  @spec ingest_push(reference(), [map()]) :: result(:ok)
  def ingest_push(_writer, _records), do: :erlang.nif_error(:nif_not_loaded)
  @spec ingest_finish(reference()) :: result(write_output())
  def ingest_finish(_writer), do: :erlang.nif_error(:nif_not_loaded)
  @spec ingest_abort(reference()) :: result(:ok)
  def ingest_abort(_writer), do: :erlang.nif_error(:nif_not_loaded)
  @spec delete_rows_result(reference(), String.t()) :: result(counts_output())
  def delete_rows_result(_table, _predicate), do: :erlang.nif_error(:nif_not_loaded)

  @spec count_rows(reference(), String.t() | nil) :: result(non_neg_integer())
  def count_rows(_table, _filter), do: :erlang.nif_error(:nif_not_loaded)

  @spec query_execute(reference(), String.t(), query_operation()) :: result(String.t())
  def query_execute(_table, _request, _operation), do: :erlang.nif_error(:nif_not_loaded)

  @spec rerank_candidates(reference(), String.t(), boolean()) ::
          result({reference(), String.t(), String.t(), String.t()})
  def rerank_candidates(_table, _request, _analyze), do: :erlang.nif_error(:nif_not_loaded)

  @spec rerank_finish(reference(), String.t(), :analyze | :stream, pos_integer()) ::
          result(non_neg_integer() | reference())
  def rerank_finish(_resource, _scores, _operation, _batch_size),
    do: :erlang.nif_error(:nif_not_loaded)

  @spec stream_open(reference(), String.t(), pos_integer()) :: result(reference())
  def stream_open(_table, _request, _batch_size), do: :erlang.nif_error(:nif_not_loaded)
  @spec stream_close(reference()) :: result(:ok)
  def stream_close(cursor), do: async_call(:stream_close, [cursor])
  @spec stream_close(reference(), reference()) :: result(:ok)
  def stream_close(_cursor, _reference), do: :erlang.nif_error(:nif_not_loaded)

  @spec add_columns(reference(), [{String.t(), String.t()}]) :: result(version_output())
  def add_columns(_table, _expressions), do: :erlang.nif_error(:nif_not_loaded)
  @spec alter_columns(reference(), String.t()) :: result(version_output())
  def alter_columns(_table, _changes), do: :erlang.nif_error(:nif_not_loaded)
  @spec drop_columns(reference(), [String.t()]) :: result(version_output())
  def drop_columns(_table, _columns), do: :erlang.nif_error(:nif_not_loaded)

  @spec tokenize(String.t(), String.t()) :: result(String.t())
  def tokenize(_text, _analyzer), do: :erlang.nif_error(:nif_not_loaded)
  @spec tokenize_table(reference(), String.t(), String.t(), String.t()) :: result(String.t())
  def tokenize_table(_table, _text, _selector, _name), do: :erlang.nif_error(:nif_not_loaded)

  @spec prewarm_index(reference(), String.t()) :: result(:prewarmed)
  def prewarm_index(_table, _name), do: :erlang.nif_error(:nif_not_loaded)
  @spec list_tags(reference()) :: result(String.t())
  def list_tags(_table), do: :erlang.nif_error(:nif_not_loaded)
  @spec tag_version(reference(), String.t()) :: result(pos_integer())
  def tag_version(_table, _name), do: :erlang.nif_error(:nif_not_loaded)

  @spec tag_action(reference(), String.t(), String.t(), pos_integer() | nil) ::
          result(:created | :updated | :deleted)
  def tag_action(_table, _action, _name, _version), do: :erlang.nif_error(:nif_not_loaded)

  @spec table_version(reference()) :: result(pos_integer())
  def table_version(_table), do: :erlang.nif_error(:nif_not_loaded)
  @spec list_versions(reference()) :: result(String.t())
  def list_versions(_table), do: :erlang.nif_error(:nif_not_loaded)
  @spec snapshot_table(reference(), pos_integer() | nil, String.t() | nil) :: result(reference())
  def snapshot_table(_table, _version, _tag), do: :erlang.nif_error(:nif_not_loaded)
  @spec restore_version(reference(), pos_integer()) :: result(version_output())
  def restore_version(_table, _version), do: :erlang.nif_error(:nif_not_loaded)

  @spec get_schema_json(reference()) :: result(String.t())
  def get_schema_json(_table), do: :erlang.nif_error(:nif_not_loaded)

  @spec update_rows(reference(), [{String.t(), {:literal | :sql, String.t()}}], String.t() | nil) ::
          result(counts_output())
  def update_rows(_table, _changes, _filter), do: :erlang.nif_error(:nif_not_loaded)

  @spec create_branch(reference(), String.t(), String.t()) :: result(reference())
  def create_branch(_table, _name, _source), do: :erlang.nif_error(:nif_not_loaded)
  @spec checkout_branch(reference(), String.t(), pos_integer() | nil) :: result(reference())
  def checkout_branch(_table, _name, _version), do: :erlang.nif_error(:nif_not_loaded)
  @spec list_branches(reference()) :: result(String.t())
  def list_branches(_table), do: :erlang.nif_error(:nif_not_loaded)
  @spec current_branch(reference()) :: result(String.t())
  def current_branch(_table), do: :erlang.nif_error(:nif_not_loaded)
  @spec delete_branch(reference(), String.t()) :: result(:deleted)
  def delete_branch(_table, _name), do: :erlang.nif_error(:nif_not_loaded)
  @spec table_uri(reference()) :: result(String.t())
  def table_uri(_table), do: :erlang.nif_error(:nif_not_loaded)

  @spec clone_table(reference(), String.t(), String.t(), pos_integer() | nil, String.t() | nil) ::
          result(reference())
  def clone_table(_conn, _name, _source_uri, _version, _tag),
    do: :erlang.nif_error(:nif_not_loaded)

  @spec schema_metadata(reference()) :: result(String.t())
  def schema_metadata(_table), do: :erlang.nif_error(:nif_not_loaded)

  @spec update_schema_metadata(reference(), %{String.t() => String.t() | nil}, boolean()) ::
          result(version_output())
  def update_schema_metadata(_table, _changes, _replace), do: :erlang.nif_error(:nif_not_loaded)

  @spec update_field_metadata(
          reference(),
          String.t(),
          %{String.t() => String.t() | nil},
          boolean()
        ) :: result(version_output())
  def update_field_metadata(_table, _path, _changes, _replace),
    do: :erlang.nif_error(:nif_not_loaded)

  @spec stream_next_batch(reference()) :: result(reference() | nil)
  def stream_next_batch(cursor), do: async_call(:stream_next_batch, [cursor])
  @spec stream_next_batch(reference(), reference()) :: result(:ok)
  def stream_next_batch(_cursor, _reference), do: :erlang.nif_error(:nif_not_loaded)

  defp async_call(operation, args) do
    reference = make_ref()

    case apply(__MODULE__, operation, args ++ [reference]) do
      {:ok, :ok} ->
        receive do
          {^reference, result} -> result
        end

      error ->
        error
    end
  end

  @spec batch_rows(reference()) :: result(String.t())
  def batch_rows(_batch), do: :erlang.nif_error(:nif_not_loaded)

  @spec batch_size(reference()) :: non_neg_integer()
  def batch_size(_batch), do: :erlang.nif_error(:nif_not_loaded)

  @spec batch_schema(reference()) :: String.t()
  def batch_schema(_batch), do: :erlang.nif_error(:nif_not_loaded)

  @spec ingest_push_batch(reference(), reference()) :: result(:ok)
  def ingest_push_batch(_writer, _batch), do: :erlang.nif_error(:nif_not_loaded)

  @spec batch_to_ipc(reference()) :: result(binary())
  def batch_to_ipc(_batch), do: :erlang.nif_error(:nif_not_loaded)

  @spec batch_from_ipc(binary()) :: result([reference()])
  def batch_from_ipc(_bytes), do: :erlang.nif_error(:nif_not_loaded)

  @spec batch_from_terms([map()], String.t(), %{String.t() => String.t()}) :: result(reference())
  def batch_from_terms(_records, _schema_json, _metadata), do: :erlang.nif_error(:nif_not_loaded)
end
