defmodule LanceDB.Connection do
  @moduledoc "A connection to an embedded LanceDB database. Native resources are opaque."
  @enforce_keys [:resource]
  defstruct [:resource]
  @opaque t :: %__MODULE__{resource: reference()}
  @type connection :: t()
  @type table :: LanceDB.Table.t()
  @type result(value) :: LanceDB.result(value)
  alias LanceDB.Backend
  alias LanceDB.Options

  @doc """
  Connects to local or object storage, defaulting to `"./lancedb"`.
  The second argument is a storage-options map passed to the SDK; the third
  is a keyword list of wrapper options:

  * `:read_consistency_interval_ms` — `nil` (SDK default) or an integer from
    0 through 4,294,967,295 milliseconds. Zero checks for external changes on
    every read; a positive interval checks on reads after that interval.
    The default does not automatically refresh after other handles write.
    Historical handles stay pinned. Use `LanceDB.History.latest/1` to refresh.
  * `:index_cache_size_bytes`, `:metadata_cache_size_bytes` — integer byte
    counts from 0 through 9,223,372,036,854,775,807. Omit either option to use
    its SDK default; explicit `nil` is invalid. Either option configures the
    session on the initial SDK connection; zero disables the selected cache.
  * `:timeout_ms` — `nil` (no wrapper deadline) or a positive uint32 millisecond
    deadline for the full native connection. Expiry returns an error with
    `LanceDB.Error.kind == :timeout`.

  Tables created or opened without per-open cache budgets share this session.
  Use separate connections for independent sessions. Per-open cache options
  select a separate session with SDK defaults for omitted budgets. Budgets
  limit session caches, not total native memory. Inspect actual usage with
  `LanceDB.Table.cache_stats/1`.

  Native connection work runs on Tokio; the caller waits by message without
  retaining a BEAM scheduler. Caller death cancels the operation. Cancellation
  is cooperative: synchronous SDK work cannot be preempted, and separately
  spawned SDK tasks may outlive the operation.
  """
  @spec connect(String.t(), map(), keyword()) :: result(connection())
  def connect(uri \\ "./lancedb", storage_options \\ %{}, opts \\ [])
      when is_binary(uri) and is_map(storage_options) do
    LanceDB.Execution.run(:connect, fn ->
      Backend.connect(
        uri,
        LanceDB.StorageOptions.normalize!(storage_options),
        connect_options!(opts)
      )
    end)
  end

  @doc false
  @spec connect_options!(keyword()) :: map()
  def connect_options!(opts) do
    opts =
      Keyword.validate!(opts, [
        :read_consistency_interval_ms,
        :timeout_ms,
        :index_cache_size_bytes,
        :metadata_cache_size_bytes
      ])

    interval = Keyword.get(opts, :read_consistency_interval_ms)

    if !(is_nil(interval) or
           (is_integer(interval) and interval >= 0 and interval <= 4_294_967_295)) do
      raise ArgumentError,
            "read_consistency_interval_ms must be nil or a non-negative uint32 integer"
    end

    Map.new(opts, fn
      {:read_consistency_interval_ms, _} -> {:read_consistency_interval_ms, interval}
      {:timeout_ms, value} -> {:timeout_ms, Options.timeout_ms!(value)}
      {key, value} -> {key, cache_size!(value)}
    end)
  end

  @doc """
  Creates a table from a row list or Source in one commit. Sources require a schema;
  row lists accept `:schema` or infer their types. Options apply to every input form:

  * `:mode` — `:create` (default), `:overwrite`, or `:exist_ok`. Existing tables
    returned by `:exist_ok` retain their settings and need not consume the source.
  * `:schema_metadata` — string metadata merged into the explicit schema.
  * `:location` — explicit table URI; pass it again when opening that location.
  * `:storage_options` — per-table storage overrides, merged with connection settings.
  * `:max_rows_per_file`, `:max_rows_per_group`, `:max_bytes_per_file` — positive
    uint32 writer limits. Byte limits are soft; omitted values use SDK defaults.
  * `:data_storage_version` — `:stable`, `:next`, or a version string: `"legacy"`,
    `"2.0"`, `"2.1"`, `"2.2"`, `"2.3"`. Stable and the default resolve to 2.2 in Lance 13.
  * `:enable_stable_row_ids`, `:enable_v2_manifest_paths` — explicit booleans;
    omitted settings inherit SDK/connection defaults. Blob schemas automatically
    require file format >= 2.2. Enable stable row IDs explicitly to retain blob
    row IDs across compaction.
  * Blob write options — `:external_blob_mode` (`:reference` or `:ingest`),
    `:allow_external_blob_outside_bases` (default false, only for reference mode),
    and positive uint32 `:blob_pack_file_size_threshold` (SDK default 1 GiB).

  Explicit typed format/row-ID/manifest options override corresponding storage
  options. Use file formats supported by every client that will open the table.
  """
  @spec create_table(connection(), String.t(), [map()] | LanceDB.Source.t(), keyword()) ::
          result(table())
  def create_table(conn, name, input, opts \\ []) do
    LanceDB.Execution.run(:create_table, fn ->
      LanceDB.Creation.create(conn, name, input, opts)
    end)
  end

  @doc "Creates an empty table with an explicit schema, accepting the same controls as create_table/4."
  @spec create_empty_table(connection(), String.t(), LanceDB.Schema.t() | list(), keyword()) ::
          result(table())
  def create_empty_table(conn, name, schema, opts \\ []) do
    if is_nil(schema), do: raise(ArgumentError, "an explicit schema is required")
    create_table(conn, name, LanceDB.Source.rows([], schema: schema), opts)
  end

  @doc "Creates a shallow clone sharing source files. Options: source_version or source_tag. Retain the source files while clones reference them."
  @spec clone_table(connection(), String.t(), String.t(), keyword()) :: result(table())
  def clone_table(conn, name, source_uri, opts \\ []) do
    LanceDB.Execution.run(:clone_table, fn ->
      opts = Keyword.validate!(opts, [:source_version, :source_tag])
      name = Options.string!(name, "table name")
      source_uri = Options.string!(source_uri, "source URI")

      if Keyword.has_key?(opts, :source_version) and Keyword.has_key?(opts, :source_tag) do
        raise ArgumentError, "source_version and source_tag cannot be combined"
      end

      version =
        if Keyword.has_key?(opts, :source_version) do
          Options.version!(opts[:source_version])
        end

      tag =
        if Keyword.has_key?(opts, :source_tag) do
          Options.string!(opts[:source_tag], "tag name")
        end

      Backend.clone_table(conn, name, source_uri, version, tag)
    end)
  end

  @doc "Lists names with optional exclusive start_after and positive limit."
  @spec table_names(connection(), keyword()) :: result([String.t()])
  def table_names(conn, opts \\ []) do
    LanceDB.Execution.run(:table_names, fn ->
      opts = Keyword.validate!(opts, [:start_after, :limit])

      start =
        if Keyword.has_key?(opts, :start_after) do
          Options.string!(opts[:start_after], "start_after")
        end

      limit = Keyword.get(opts, :limit)

      if Keyword.has_key?(opts, :limit) and
           not (is_integer(limit) and limit > 0 and limit <= 4_294_967_295) do
        raise ArgumentError, "limit must be a positive uint32 integer"
      end

      Backend.table_names(conn, start, limit)
    end)
  end

  @doc """
  Opens a table, sharing the connection's session unless cache budgets are set.

  * `:branch` — branch name; defaults to `"main"`.
  * `:version` — positive version on that branch, pinning a read-only handle.
  * `:location` — explicit table URI, including custom creation locations.
  * `:storage_options` — a map merged over the connection's storage options.
  * `:index_cache_size_bytes`, `:metadata_cache_size_bytes` — the same byte
    ranges as `connect/3`. Either option creates a dedicated session. The
    omitted cache uses its SDK default, rather than the connection's budget.
    Zero disables that cache; explicit `nil` is invalid. The new session
    keeps the connection's storage and read-consistency configuration.
  * `:timeout_ms` — `nil` or a positive uint32 millisecond deadline covering
    the entire native open, including a dedicated session's connection setup.

  Opening a table does not change other handles. Snapshots and branch checkouts
  retain the opened handle's session; branch creation uses a new SDK session.
  `LanceDB.Table.cache_stats/1` reports the actual session's usage and counters.
  Expiry returns an error with `LanceDB.Error.kind == :timeout`. Native work
  runs on Tokio and cancels on caller death, with the cooperative limits of
  `connect/3`.
  """
  @spec open_table(connection(), String.t(), keyword()) :: result(table())
  def open_table(conn, name, opts \\ []) do
    LanceDB.Execution.run(:open_table, fn ->
      name = Options.string!(name, "table name")

      opts =
        Keyword.validate!(opts, [
          :branch,
          :version,
          :location,
          :storage_options,
          :index_cache_size_bytes,
          :metadata_cache_size_bytes,
          :timeout_ms
        ])

      options =
        Map.new(opts, fn
          {:version, version} ->
            {:version, Options.version!(version)}

          {key, value} when key in [:branch, :location] ->
            {key, Options.string!(value, to_string(key))}

          {:storage_options, values} ->
            {:storage_options, LanceDB.StorageOptions.normalize!(values)}

          {:timeout_ms, value} ->
            {:timeout_ms, Options.timeout_ms!(value)}

          {key, value} ->
            {key, cache_size!(value)}
        end)

      Backend.open_table(conn, name, options)
    end)
  end

  defp cache_size!(value) do
    if !(is_integer(value) and value >= 0 and value <= 9_223_372_036_854_775_807) do
      raise ArgumentError, "cache sizes must be non-negative 64-bit byte counts"
    end

    value
  end

  @doc """
  Drops a table from the database.
  """
  @spec drop_table(connection(), String.t()) :: result(:dropped)
  def drop_table(conn, name) when is_binary(name) do
    LanceDB.Execution.run(:drop_table, fn -> Backend.drop_table(conn, name) end)
  end
end
