defmodule LanceDB.Table do
  alias LanceDB.Backend.SchemaCodec

  @moduledoc """
  Shareable table handles and row operations. Related domains expose history, branches, schema evolution, metadata, blobs and maintenance.
  Snapshots are independent read-only handles; writes never move other snapshots.
  """
  @enforce_keys [:resource]
  defstruct [:resource]
  @opaque t :: %__MODULE__{resource: reference()}
  @type table :: t()
  @type result(value) :: LanceDB.result(value)
  alias LanceDB.Backend
  alias LanceDB.Write

  @doc """
  Returns native cache statistics as a map with string keys, without enabling
  the process-global metrics recorder.

  `"scope"` is `"session"`. The `"index"` and `"metadata"` maps contain
  `"size_bytes"` (SDK-accounted entry bytes), `"num_entries"`, and cumulative
  `"hits"` and `"misses"`. `"capacity_bytes"` is nil because the pinned SDK
  does not expose configured capacity. Usage is not total native memory or RSS.

  Statistics cover the actual session used by the handle, including other
  handles sharing it. Explicit table-open cache budgets create an isolated
  session. Snapshots and branch checkouts preserve their source session; branch
  creation uses a new SDK session. Reads do not reset counters. Concurrent
  activity can change fields during a snapshot.

  Resolving the session follows the handle's read-consistency configuration
  and may check storage. No table URI or storage options are returned.
  """
  @spec cache_stats(table()) :: result(map())
  def cache_stats(table) do
    LanceDB.Execution.run(:cache_stats, fn ->
      table |> Backend.cache_stats() |> LanceDB.Result.decode()
    end)
  end

  @doc """
  Writes a row list or Source using the stored table schema. One commit is the default; commit: :batch explicitly enables incremental insertion. Empty append is a no-op; empty overwrite clears rows.

  Returns `{:ok, %LanceDB.WriteResult{inserted: count, version: commit_version}}`.

  Options:

  * `:mode` — `:append` (default) or `:overwrite`, replacing all rows in one commit.
  * `:write_parallelism` — positive number of native write tasks; `1` disables
    parallel writes. Omitted lets the SDK estimate parallelism.
  * `:max_rows_per_file`, `:max_rows_per_group`, `:max_bytes_per_file` — positive
    uint32 limits passed to the native writer. The byte limit is a soft limit;
    omitted limits use upstream defaults.
  * Blob writes accept `:external_blob_mode` (`:reference`, the default, or `:ingest`
    to copy external bytes), `:allow_external_blob_outside_bases` (default false,
    reference mode only), and positive uint32 `:blob_pack_file_size_threshold`.
  * `:progress` — `{local_pid, tag}` receives
    `{:lancedb_write_progress, tag, progress_map}` messages. Fields are
    `:output_rows`, `:output_bytes`, `:total_rows`, `:elapsed_ms`, `:active_tasks`,
    `:total_tasks`, and `:done`. Counts are cumulative within this write;
    `:total_rows` may initially be nil. `:done` means processing ended, including
    errors; only the API result confirms a successful commit. Progress describes
    native work, not committed rows. The recipient should drain its mailbox.

  Overwrite retains the stored schema. Invalid records fail before writing.
  """
  @spec add(table(), [map()] | LanceDB.Source.t(), keyword()) :: result(LanceDB.WriteResult.t())
  def add(table, records, opts \\ []) do
    LanceDB.Execution.write(
      :add,
      fn -> LanceDB.Ingestion.add(table, records, opts) end,
      &LanceDB.WriteResult.counts/2
    )
  end

  @doc """
  Returns the number of rows in a table, optionally filtered by an SQL string or `LanceDB.Expr`.
  """
  @spec count_rows(table(), String.t() | LanceDB.Expr.t() | nil) :: result(non_neg_integer())
  def count_rows(table, filter \\ nil) do
    LanceDB.Execution.run(:count_rows, fn ->
      Backend.count_rows(table, LanceDB.Options.filter!(filter))
    end)
  end

  @doc """
  Deletes rows matching a SQL string or `LanceDB.Expr` predicate.

  Returns `{:ok, %LanceDB.WriteResult{deleted: count, version: commit_version}}`. A predicate matching no
  rows returns zero deleted rows; its version follows upstream no-op semantics.
  """
  @spec delete(table(), String.t() | LanceDB.Expr.t(), keyword()) ::
          result(LanceDB.WriteResult.t())
  def delete(table, predicate, opts \\ []) do
    LanceDB.Execution.write(
      :delete,
      fn -> Write.delete(table, predicate, opts) end,
      &LanceDB.WriteResult.counts/2
    )
  end

  @doc """
  Returns the schema information for a table.
  """
  @spec schema(table()) :: result(LanceDB.Schema.t())
  def schema(table) do
    LanceDB.Execution.run(:schema, fn ->
      case Backend.get_schema_json(table) do
        {:ok, json_str} -> SchemaCodec.decode(json_str)
        {:error, reason} -> {:error, reason}
      end
    end)
  end

  @doc """
  Inserts new records and replaces matching rows in one merge operation.

  The `:on` option accepts a column name or a non-empty list of column names
  (default `:id`). Incoming keys must be unique, non-null strings or integers.
  Records must include every table column with matching types. Rows absent
  from the input are left unchanged. Existing table keys should also be unique;
  this operation does not add a uniqueness constraint or deduplicate old rows.

  Returns a WriteResult with insertion/update counts and the committed version.
  """
  @spec upsert(table(), [map()] | LanceDB.Source.t(), keyword()) ::
          result(LanceDB.WriteResult.t())
  def upsert(table, records, opts \\ []) do
    LanceDB.Execution.write(
      :upsert,
      fn -> Write.merge(table, records, Write.upsert_options!(opts)) end,
      &LanceDB.WriteResult.counts/2
    )
  end

  @doc """
  Executes a conditional merge. All actions default to disabled; enable at least one.

  Options:
    * `:on` - One or more key columns (default `:id`)
    * `:when_matched_update_all` - `true`, `false`, or an SQL string / `LanceDB.Expr` condition
    * `:when_not_matched_insert_all` - Boolean
    * `:when_not_matched_by_source_delete` - `true`, `false`, or an SQL string / `LanceDB.Expr` condition
    * `:timeout_ms` - Positive timeout in milliseconds; otherwise SDK default
    * `:use_index` - Use the join-key index (default true)

  Update conditions may refer to `source.column` and `target.column`. Deletion
  conditions use unqualified target column names (for example `"tenant = 'a'"`). Source rows must include every table field
  and have unique, non-null string/integer keys; existing target keys should be
  unique too. A failed update condition leaves the matched row unchanged.

  Deletion removes target rows missing from the entire source. An empty source is accepted and can delete all target rows when deletion is enabled.
  Returns a WriteResult with measured counts, version, and merge attempts.
  """
  @spec merge(table(), [map()] | LanceDB.Source.t(), keyword()) :: result(LanceDB.WriteResult.t())
  def merge(table, records, opts \\ []) do
    LanceDB.Execution.write(
      :merge,
      fn -> Write.merge(table, records, opts) end,
      &LanceDB.WriteResult.counts/2
    )
  end

  @doc """
  Updates columns on rows that match the optional `:filter`.

  The filter accepts a SQL string or a `LanceDB.Expr`. Values can be strings,
  numbers, booleans, `nil`, numeric vectors, lists, struct maps, or binaries.
  Values can also be `Calendar.ISO` date or date-time structs.
  Collection literals replace complete top-level columns. The operation checks
  all nested values against the stored schema.

  Timestamp literals use the precision and timezone rules in `LanceDB.Schema`.
  Epoch integers use the destination column's unit. Use `LanceDB.Expr` or
  `{:sql, expression}` for SQL expressions, such as `"score + 1"`.
  Expressions use the previous row values. The operation escapes strings as SQL
  literals. Literal values must match the table schema. Incompatible values
  return an error. Explicit SQL expressions use LanceDB's type conversion rules.

  Without a filter, the operation updates every row. It returns
  `{:ok, %LanceDB.WriteResult{updated: count, version: version}}`.
  The version comes from the update operation, including when no rows match.

  The current SDK cannot update rows in tables that contain blob columns.
  This includes updates to other columns. These updates return an unsupported
  error before execution. Use `upsert/3` or `merge/3` with complete records to
  replace blob rows.

      LanceDB.Table.update(table, %{category: "code"}, filter: "id = '1'")
      LanceDB.Table.update(table, %{score: {:sql, "score + 1"}})
  """
  @spec update(table(), map(), keyword()) ::
          result(LanceDB.WriteResult.t())
  def update(table, changes, opts \\ []) do
    LanceDB.Execution.write(
      :update,
      fn -> Write.update(table, changes, opts) end,
      &LanceDB.WriteResult.counts/2
    )
  end

  @doc "Returns the table's storage URI, suitable as the source URI for `clone_table/4`."
  @spec uri(table()) :: result(String.t())
  def uri(table) do
    LanceDB.Execution.run(:table_uri, fn -> Backend.table_uri(table) end)
  end
end
