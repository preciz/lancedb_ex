defmodule LanceDB.Query do
  alias LanceDB.Query.{Definition, Executor, Request}

  @moduledoc """
  Pure, reusable queries. Construction validates options without accessing storage.

  `all/1`, `stream/2`, `stream_batches/2`, `explain/2`, `analyze/1` and `schema/1`
  execute the same query. Scan limits default to 100; use `limit(query, nil)` for
  an unlimited scan. Searches default to 10 results. Re-execution of a live table
  may observe new data; use a table snapshot for repeatable reads.

  Both constructors take the table first: `new(table, opts)` builds a scan;
  `new(table, source, opts)` selects an explicit source. Immediate helpers such
  as `search/3` and `query/2` execute these same query values. Result maps have
  string keys; operational failures return `LanceDB.Error`, and stream failures
  raise it during enumeration.
  """
  @enforce_keys [:table]
  defstruct [:table, :definition]

  @opaque t :: %__MODULE__{
            table: LanceDB.Table.t(),
            definition: Definition.t()
          }

  @doc "Constructs a scan. No database work occurs until execution."
  @spec new(LanceDB.Table.t(), keyword()) :: t()
  def new(%LanceDB.Table{} = table, opts \\ []),
    do: build(table, :scan, opts)

  @doc "Selects vector search, retaining filters, projections and explicit limits."
  @spec nearest_to(t(), [number()]) :: t()
  def nearest_to(%__MODULE__{} = query, vector),
    do: select_source(query, {:vector, vector})

  @doc "Selects full-text search with a string or structured FTS query."
  @spec text(t(), LanceDB.FTS.input()) :: t()
  def text(%__MODULE__{} = query, text), do: select_source(query, {:text, text})
  @doc "Selects hybrid search. Rerankers are supplied through options/2."
  @spec hybrid(t(), [number()], LanceDB.FTS.input()) :: t()
  def hybrid(%__MODULE__{} = query, vector, text),
    do: select_source(query, {:hybrid, vector, text})

  @doc "Selects independent searches for a batch of query vectors."
  @spec nearest_to_many(t(), [[number()]]) :: t()
  def nearest_to_many(%__MODULE__{} = query, vectors),
    do: select_source(query, {:batch, vectors})

  @doc "Selects document multivector search."
  @spec multivector(t(), [[number()]]) :: t()
  def multivector(%__MODULE__{} = query, vectors),
    do: select_source(query, {:multivector, vectors})

  @doc "Selects direct row-ID retrieval."
  @spec row_ids(t(), [non_neg_integer()]) :: t()
  def row_ids(%__MODULE__{} = query, ids), do: select_source(query, {:take, :row_ids, ids})
  @doc "Selects direct physical-offset retrieval."
  @spec row_offsets(t(), [non_neg_integer()]) :: t()
  def row_offsets(%__MODULE__{} = query, offsets),
    do: select_source(query, {:take, :row_offsets, offsets})

  @doc "Merges options, validating them against the selected query kind."
  @spec options(t(), keyword()) :: t()
  def options(%__MODULE__{} = query, opts),
    do: %{query | definition: Definition.update(query.definition, opts)}

  @doc "Sets the SQL string or Expr predicate, replacing any previous predicate."
  @spec where(t(), String.t() | LanceDB.Expr.t() | nil) :: t()
  def where(query, predicate), do: options(query, filter: predicate)
  @doc "Sets projected column names."
  @spec select(t(), [atom() | String.t()]) :: t()
  def select(query, columns), do: options(query, columns: columns)
  @doc "Sets SQL string or Expr projections."
  @spec select_as(t(), map() | list()) :: t()
  def select_as(query, expressions), do: options(query, select: expressions)
  @doc "Sets explicit ordering, including per-column null placement."
  @spec order_by(t(), keyword()) :: t()
  def order_by(query, ordering), do: options(query, order_by: ordering)
  @doc "Sets the maximum result count; nil allows unlimited scans."
  @spec limit(t(), pos_integer() | nil) :: t()
  def limit(query, limit), do: options(query, limit: limit)
  @doc "Sets the result offset."
  @spec offset(t(), non_neg_integer()) :: t()
  def offset(query, offset), do: options(query, offset: offset)

  @doc """
  Sets a cooperative execution deadline: nil disables it, otherwise supply a
  positive uint32 millisecond count. For streams it covers the cursor lifetime,
  including consumer pauses; it does not interrupt the caller's Elixir code.
  """
  @spec timeout(t(), pos_integer() | nil) :: t()
  def timeout(query, milliseconds), do: options(query, timeout_ms: milliseconds)

  @doc "Collects the complete query result in memory."
  @spec all(t()) :: LanceDB.result([map()])
  def all(%__MODULE__{} = query), do: execute(query.table, plan(query), :execute)

  @doc """
  Builds a lazy row stream; each enumeration executes the query again.
  The only stream option is `:batch_size` (default 1024); query controls belong
  to the query value. Batches bound returned rows, not query memory. Ending or
  halting enumeration closes the cursor and releases its read admission slot.

  The process that begins enumeration opens and owns the cursor. You may build
  the lazy stream in one process and enumerate it in another. Resume suspended
  enumeration in its original process; an active cursor cannot be transferred
  to another process. Owner exit cancels the cursor.
  """
  @spec stream(t(), keyword()) :: Enumerable.t()
  def stream(%__MODULE__{} = query, opts \\ []), do: read_stream(query, opts, :rows)

  @doc """
  Builds a lazy stream of native Arrow batches. Use Source.batches for
  schema-compatible ingestion.

  Each enumeration opens a cursor owned by the enumerating process, as with
  `stream/2`. Resume suspended enumeration in that same process. Owner exit
  cancels the cursor.
  """
  @spec stream_batches(t(), keyword()) :: Enumerable.t()
  def stream_batches(%__MODULE__{} = query, opts \\ []), do: read_stream(query, opts, :batches)
  @doc "Explains this query without executing it."
  @spec explain(t(), keyword()) :: LanceDB.result(String.t())
  def explain(%__MODULE__{} = query, opts \\ []) do
    opts = Keyword.validate!(opts, [:verbose])
    verbose = Keyword.get(opts, :verbose, false)
    if not is_boolean(verbose), do: raise(ArgumentError, "verbose must be boolean")
    execute(query.table, plan(query), if(verbose, do: :explain_verbose, else: :explain))
  end

  @doc "Executes once and returns execution diagnostics."
  @spec analyze(t()) :: LanceDB.result(String.t())
  def analyze(%__MODULE__{} = query), do: execute(query.table, plan(query), :analyze)

  @doc "Returns an ArrowSchema describing the physical output. Convert explicitly with ArrowSchema.to_schema/1 before table creation."
  @spec schema(t()) :: LanceDB.result(LanceDB.ArrowSchema.t())
  def schema(%__MODULE__{} = query), do: execute(query.table, plan(query), :schema)

  defp read_stream(query, opts, mode) do
    {batch_size, rest} = LanceDB.Options.batch_size!(opts)
    Keyword.validate!(rest, [])
    LanceDB.Stream.from_plan(query.table, plan(query), batch_size, mode)
  end

  defp build(table, source, opts),
    do: %__MODULE__{table: table, definition: Definition.new(source, opts)}

  defp select_source(query, source),
    do: %{query | definition: Definition.source(query.definition, source)}

  defp plan(query), do: Request.compile(query.definition)

  @doc """
  Constructs a query from a table, explicit source, and keyword controls.

  Sources are `:scan`, `{:vector, values}`, `{:text, text}`,
  `{:hybrid, values, text}`, `{:batch, vectors}`, `{:multivector, vectors}`,
  `{:take, :row_ids, values}`, or `{:take, :row_offsets, values}`.
  Each source accepts its corresponding immediate API's options.
  See the [query guide](queries.md) and [search guide](search.md).
  Construction validates arguments without accessing storage.
  """
  @spec new(LanceDB.Table.t(), term(), keyword()) :: t()
  def new(%LanceDB.Table{} = table, source, opts), do: build(table, source, opts)

  @doc """
  Runs a vector query immediately, returning up to 10 rows by default.
  Without `:vector_column`, selects the unique column compatible with the search
  mode and query dimension. Ambiguous matches require an explicit column;
  dimension mismatches return an error naming the query and expected dimensions.
  See the [search controls](queries.md#search-controls) for projection, filtering,
  distances, index tuning, and deadlines.
  """
  @spec search(LanceDB.Table.t(), [number()], keyword()) :: LanceDB.result([map()])
  def search(table, vector, opts \\ []), do: all(new(table, {:vector, vector}, opts))

  @doc "Runs independent vector queries immediately."
  @spec batch_search(LanceDB.Table.t(), [[number()]], keyword()) :: LanceDB.result([map()])
  def batch_search(table, vectors, opts \\ []),
    do: all(new(table, {:batch, vectors}, opts))

  @doc "Runs document multivector search immediately."
  @spec multivector_search(LanceDB.Table.t(), [[number()]], keyword()) :: LanceDB.result([map()])
  def multivector_search(table, vectors, opts \\ []),
    do: all(new(table, {:multivector, vectors}, opts))

  @doc "Runs a full-text query immediately."
  @spec full_text_search(LanceDB.Table.t(), LanceDB.FTS.input(), keyword()) ::
          LanceDB.result([map()])
  def full_text_search(table, text, opts \\ []),
    do: all(new(table, {:text, text}, opts))

  @doc "Runs a hybrid query immediately."
  @spec hybrid_search(LanceDB.Table.t(), [number()], LanceDB.FTS.input(), keyword()) ::
          LanceDB.result([map()])
  def hybrid_search(table, vector, text, opts \\ []),
    do: all(new(table, {:hybrid, vector, text}, opts))

  @doc "Collects a scan immediately. Use new/2 for a reusable query."
  @spec query(LanceDB.Table.t(), keyword()) :: LanceDB.result([map()])
  def query(table, opts \\ []), do: all(new(table, opts))

  @doc """
  Retrieves rows by internal ID. IDs are non-negative uint64 integers from
  `_rowid`, not application keys. Accepts `:columns` or `:select`, `:with_row_id`,
  and `:timeout_ms`; there is no default row limit. Missing IDs contribute no
  rows, duplicates do not repeat rows, and output order is unspecified. Collect
  and use IDs against the same table version.
  """
  @spec take_row_ids(LanceDB.Table.t(), [non_neg_integer()], keyword()) :: LanceDB.result([map()])
  def take_row_ids(table, ids, opts \\ []), do: all(new(table, {:take, :row_ids, ids}, opts))

  @doc """
  Retrieves rows by non-negative uint64 physical offsets, with the same options
  and result-set semantics as `take_row_ids/3`. Updates, deletions, and compaction
  can change offsets; use the same table version for collection and retrieval.
  """
  @spec take_offsets(LanceDB.Table.t(), [non_neg_integer()], keyword()) :: LanceDB.result([map()])
  def take_offsets(table, offsets, opts \\ []),
    do: all(new(table, {:take, :row_offsets, offsets}, opts))

  defp execute(table, plan, operation), do: Executor.execute(table, plan, operation)
end
