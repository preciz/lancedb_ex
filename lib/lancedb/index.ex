defmodule LanceDB.Index do
  alias LanceDB.Backend.ResultCodec
  alias LanceDB.Index.{Definition, Readiness}
  @moduledoc "Index creation, inspection, prewarming and removal."
  alias LanceDB.Backend
  alias LanceDB.Result
  import LanceDB.Options, only: [column_name!: 1]
  @type table :: LanceDB.Table.t()
  @type result(value) :: LanceDB.result(value)
  @doc """
  Waits until every named index exists and has zero unindexed rows.

  Accepts a name or non-empty list of names. Options: `:timeout_ms` (default
  60_000, maximum two hours) and `:poll_interval_ms` (default 1_000, maximum
  60_000). Both must be positive integers. Returns `{:ok, :ready}` or a timeout
  Error containing `details.pending_indexes`. Other inspection errors propagate immediately.

  This follows the SDK's readiness semantics: each name must be observed ready
  once. Concurrent writes or index replacement can invalidate that observation.
  The table handle's read-consistency settings apply to changes by other handles.
  Waiting neither builds nor optimizes indexes, and a timeout does not cancel
  any index jobs. Use `Job.wait/2` to await one specific creation job instead.
  Polling sleeps in the caller without holding native admission capacity.
  """
  @spec wait(table(), atom() | String.t() | [atom() | String.t()], keyword()) :: result(:ready)
  def wait(table, names, opts \\ []) do
    LanceDB.Execution.run(:wait_for_index, fn ->
      Readiness.wait(table, names, opts)
    end)
  end

  @doc "Drops an index by name. Physical files may remain until database cleanup."
  @spec drop(table(), atom() | String.t()) :: result(:dropped)
  def drop(table, name) do
    LanceDB.Execution.run(:drop_index, fn -> Backend.drop_index(table, column_name!(name)) end)
  end

  @doc """
  Loads an existing index into the index cache, returning after prewarming completes.
  Useful before the first search after opening a table. Does not build or update
  indexes; cached pages can later be evicted. Missing indexes return an error.
  For shared background warming, use `LanceDB.TableCache.prewarm/3`.
  """
  @spec prewarm(table(), atom() | String.t()) :: result(:prewarmed)
  def prewarm(table, name) do
    LanceDB.Execution.run(:prewarm_index, fn ->
      Backend.prewarm_index(table, column_name!(name))
    end)
  end

  @doc """
  Returns index statistics as a string-keyed map, or `{:ok, nil}` if absent.
  Includes "loss": the sum of IVF training losses across index segments, or nil
  for non-vector indexes or when any segment does not report loss. Loss is not
  a query recall measurement.
  """
  @spec stats(table(), atom() | String.t()) :: result(map() | nil)
  def stats(table, name) do
    LanceDB.Execution.run(:index_stats, fn ->
      Result.decode(Backend.index_stats(table, column_name!(name)))
    end)
  end

  @doc "Lists Index.Description structs with name, type, columns, size and indexed row counts."
  @spec list(table()) :: result([LanceDB.Index.Description.t()])
  def list(table) do
    LanceDB.Execution.run(:list_indices, fn ->
      ResultCodec.decode(
        Backend.list_indices(table),
        LanceDB.Index.Description,
        :many
      )
    end)
  end

  @doc """
  Creates an index on one column, returning `{:ok, :created}` after completion.

  Types: `:auto` (default), `:btree`, `:bitmap`, `:label_list`, `:fm`, `:ivf_flat`, `:ivf_pq`,
  `:ivf_sq`, `:ivf_rq`, `:hnsw_flat`, `:hnsw_pq`, `:hnsw_sq`, `:fts`.
  `:label_list` accelerates `array_has_any`/`array_has_all` on lists;
  `:fm` accelerates `contains(column, 'substring')` on strings.
  Neither is selected by `:auto`; choose its type explicitly.
  Options: `:name`, `:replace` (default false), and, for vector indexes,
  `:distance` (`:l2`, `:cosine`, `:dot`, or `:hamming` for uint8 vectors), `:num_partitions` or
  `:target_partition_size` (mutually exclusive), `:sample_rate` (SDK default 256),
  and `:max_iterations` (SDK default 50). Counts must be positive uint32 integers.
  `:ivf_pq` and `:hnsw_pq` also accept `:num_sub_vectors`. HNSW types accept
  `:m` (graph neighbors, SDK default 20) and `:ef_construction` (build
  candidates, SDK default 300). All HNSW types use IVF partitions.
  PQ types accept `:num_bits` of 4 or 8 (default 8); 4-bit PQ needs an even
  number of subvectors. `:ivf_rq` accepts 1 through 9 bits (default 1), and its
  vector dimension must be divisible by 8. SQ is fixed at 8 bits.
  `:fts` accepts an `:analyzer`
  keyword list (tokenizer, language, normalization, positions, token length, and
  n-gram settings, custom_stop_words, document_tokenizer (:text/:json), and code
  splitting flags; see the [analyzer guide](search.md#configurable-fts-analyzers)).
  FTS build controls are `:block_size` (128/256),
  positive uint32 `:memory_limit_mb` and `:num_workers`, and `:format_version`
  (`:v1`, `:v2`, `:v3`). Code tokenization and 256-document blocks require V3;
  omitted format selects it automatically when needed. Explicit scalar indexes accept `train: false`
  to create an empty index. Vector indexes require training
  data; their distance must match the distance used for search.
  """
  @spec create(table(), atom() | String.t(), keyword() | Definition.t()) :: result(:created)
  def create(table, column, opts \\ []) do
    LanceDB.Execution.run(:create_index, fn ->
      column = column_name!(column)
      options = Definition.new(opts)
      Backend.create_index(table, column, options)
    end)
  end

  @doc "Starts native index creation and returns a Job."
  @spec create_async(table(), atom() | String.t(), keyword() | Definition.t()) ::
          result(LanceDB.Job.t())
  def create_async(table, column, opts \\ []) do
    LanceDB.Execution.run(:create_index_async, fn ->
      column = LanceDB.Options.column_name!(column)
      options = Definition.new(opts)
      LanceDB.Backend.create_index_async(table, column, options)
    end)
  end

  @doc "Builds a reusable, validated index definition from keyword options."
  @spec new(keyword()) :: Definition.t()
  defdelegate new(opts \\ []), to: LanceDB.Index.Definition
end
