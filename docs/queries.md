# Queries

[Back to README](../README.md#documentation).

These examples use `db` and `table` handles. See the [API guide](api.md) for setup.
Match the table schema to each example.

## Read consistency

Use the third argument to `connect/3` to control when table handles check
for changes from other handles:

```elixir
# Check for a newer version on every read.
{:ok, db} = LanceDB.connect("./data/vectors", %{}, read_consistency_interval_ms: 0)

# Check on reads once one second has elapsed since the last consistency check.
{:ok, db} = LanceDB.connect("./data/vectors", %{}, read_consistency_interval_ms: 1_000)
```

If you do not set an interval, or set `nil`, the SDK default applies. The
handle does not automatically refresh after another handle writes. Use
`History.latest/1` to get a new handle at the latest version. Existing
`connect/0`, `connect/1`, and `connect/2` calls keep their behavior. The
second argument is still the storage options map, including for S3 and
MinIO.

The interval must be a non-negative integer in milliseconds, with a maximum
of 4,294,967,295. It applies to tables opened or created through the
connection. Refresh occurs on reads. No background process checks for
updates. Shorter intervals cause more storage requests and can increase read
latency. Writes always check consistency.

Historical checkouts keep their selected version regardless of this setting.
`History.latest/1` returns a new live handle at the latest version. The
source handle stays unchanged.

## Queries without a vector

Use `query/2` to read rows from vector or scalar-only tables:

```elixir
{:ok, rows} =
  LanceDB.Query.query(table,
    filter: "category = 'code'",
    columns: [:id, :text],
    order_by: [asc: :id],
    limit: 20,
    offset: 0
  )

{:ok, matches} = LanceDB.Query.query(table, filter: "id = '1'")
```

The default limit is 100 rows. The default offset is 0. To return all
columns, do not set `:columns`. Column names accept strings or atoms.
`:order_by` accepts an ordered keyword list, such as
`[asc: :category, desc: :id]`. Nulls sort last by default.
Use a field path for nested values, such as `[asc: "data.rank.score"]`.
Put a path segment in backticks if its name contains a dot. Escape a
backtick in a name with two backticks. Nested ordering also works with
computed projections and hybrid search.

Use the `_nulls_first` or `_nulls_last` directions to set null placement.
You can sort by columns that the query does not return. Results use string
keys, keep null values as `nil`, and do not include vector distances. If no
rows match, the result is `{:ok, []}`.

Without `:order_by`, the query does not guarantee row order. For
pagination, order by columns that identify each row uniquely. Concurrent
writes can change rows between page requests. Pagination does not keep a
database snapshot.

## Expression builders

`LanceDB.Expr` creates expressions for filters, projections, updates,
conditional merges, and SQL column backfills. Existing SQL strings stay
valid. Builders keep column names separate from literal values. The builder
escapes literal values as SQL:

```elixir
alias LanceDB.Expr, as: E

predicate = E.all([
  E.gte(E.col(:price), 10),
  E.in_list(E.col(:category), ["books", "O'Reilly"])
])

{:ok, rows} = LanceDB.Query.query(table,
  filter: predicate,
  select: [title: E.col(:title), doubled: E.mul(E.col(:price), 2)]
)
{:ok, _} = LanceDB.Table.update(table, %{price: E.add(E.col(:price), 1)}, filter: predicate)
{:ok, _} = LanceDB.SchemaEvolution.add_columns(table, lower_title: E.call(:lower, [E.col(:title)]))

condition = E.gt(E.col(:revision, relation: :source), E.col(:revision, relation: :target))
{:ok, _} = LanceDB.Table.merge(table, incoming,
  on: :id, when_matched_update_all: condition, when_not_matched_insert_all: true
)
```

Comparison builders are `eq`, `neq`, `gt`, `gte`, `lt`, and `lte`.
Arithmetic builders are `add`, `sub`, `mul`, and `div`. Other builders
include `all`, `any`, `negate`, `in_list`, `like`, `ilike`, `is_null`, and `is_not_null`. `field(E.col(:profile), :name)` selects a struct
child. `col/2` qualifies a column with a merge relation. `call/2` calls SDK
scalar functions.

For example, `arrow_cast` selects a specific Arrow type. `binary/1`
represents arbitrary bytes. Ordinary strings represent text. Integer
literals keep int64 or uint64 precision. Float literals use float64.
`to_sql/1` returns the serialized expression for inspection.

The current SDK's column name and function restrictions still apply.

These expressions use SQL null behavior. `eq(column, nil)` produces an
unknown result. Use `is_null(column)` to find nulls. Empty `all([])` is
true. Empty `any([])` and `in_list(column, [])` are false. Builder input is
a literal unless it is already an `Expr`.

Use `col/1` to identify an atom as a column name.

## Computed projections and direct retrieval

Use `:select` to compute named fields in the database:

```elixir
{:ok, rows} = LanceDB.Query.query(table,
  select: [id: "id", total: "price * quantity", label: "upper(text)"],
  filter: "quantity > 0",
  order_by: [desc: :price]
)
```

`:select` accepts a non-empty map or list of name and expression pairs.
Expressions can be SQL strings or `LanceDB.Expr` values. Names can be
strings or atoms. Output aliases must be unique. The option replaces
`:columns`. It works with scans, vector searches, FTS, hybrid searches,
streams, direct retrieval, and explain plans.

Each expression is a scalar SQL expression. A complete SQL query is not
accepted. The operation writes nothing to the table. Filters and ordering
use **stored columns**, even when an output alias uses a stored column's
name. Quote SQL identifiers with backticks when their names contain
punctuation.

Search results keep `_distance`, `_score`, or `_relevance_score`. They
also include `_rowid` when requested. Computed aliases cannot use these
metadata names. Hybrid expressions execute in both search channels. Use
stored columns in these expressions. Do not use channel scores or the
relevance score from fusion.

Timestamp and null encoding stay unchanged. Plans can show private aliases
and hidden physical columns. Returned rows exclude those fields.

Fetch complete records after a search that returns selected fields:

```elixir
{:ok, matches} = LanceDB.Query.search(table, vector, columns: [:id], with_row_id: true)
row_ids = Enum.map(matches, & &1["_rowid"])
{:ok, records} = LanceDB.Query.take_row_ids(table, row_ids, with_row_id: true)
{:ok, sample} = LanceDB.Query.take_offsets(table, [0, 10, 20], columns: [:id, :text])

rows =
  LanceDB.Query.stream(
    LanceDB.Query.new(table, {:take, :row_ids, row_ids}, select: [label: "upper(text)"]),
    batch_size: 1024
  )

{:ok, plan} =
  LanceDB.Query.explain(LanceDB.Query.new(table, {:take, :row_offsets, [0, 10]}, columns: [:id]),
    verbose: false
  )
```

Both take functions accept `:columns` or `:select`, `:with_row_id`, and
`:timeout_ms`. Input must be a list of non-negative uint64 integers. These
functions have **no default row limit**. Empty input returns no rows.
Duplicate row IDs do not repeat rows; duplicate offsets retain their occurrence
counts. Missing or deleted identifiers return no rows.

These functions do not guarantee result order. Use `_rowid` to match fetched
records with ranked search results when necessary.

Row IDs identify stored rows. They are not application primary keys.
Compaction and updates can change row IDs. Deletions can also change
offsets. For consistent results, collect and use identifiers against the
same table version. Streaming retrieval limits result batch sizes.

The caller must still supply the complete identifier list. `:row_ids` and
`:row_offsets` cannot be combined. They also cannot be combined with vector
or text selectors, filtering, pagination, or ordering options.

## Query timeouts and explain plans

`query/2`, `search/3`, `full_text_search/3`, and `hybrid_search/4` accept
`:timeout_ms`:

```elixir
{:ok, rows} = LanceDB.Query.query(table, filter: "category = 'code'", timeout_ms: 5_000)
{:ok, matches} = LanceDB.Query.search(table, [0.9, 0.1, 0.2], timeout_ms: 2_000)
```

If you do not set a timeout, or set `nil`, the query has no deadline. Set a
positive integer up to 4,294,967,295 milliseconds. One cooperative native
deadline covers planning and result collection. Hybrid search shares the
deadline across both channels and RRF fusion. A timeout returns
`{:error,
%LanceDB.Error{kind: :timeout}}` and drops the query future. The
response can arrive after the deadline.

Native work that does not yield, JSON conversion, and BEAM scheduling can
delay the response. This query deadline does not apply to writes, maintenance,
or `count_rows/2`. Connect and table open have their own optional deadlines;
see [table-open controls](tables.md#table-open-controls-and-listing). Merge
and upsert accept a separate SDK timeout, described in
[conditional merges](writes.md#conditional-merges).

Examine the physical plan before you run a query:

```elixir
{:ok, plan} =
  LanceDB.Query.explain(
    LanceDB.Query.new(table, :scan, filter: "category = 'code'", columns: [:id]),
    verbose: false
  )

IO.puts(plan)

{:ok, vector_plan} =
  LanceDB.Query.explain(
    LanceDB.Query.new(
      table,
      {:vector, [0.9, 0.1, 0.2]},
      vector_column: :vector,
      distance: :cosine,
      nprobes: 20,
      limit: 10,
      timeout_ms: 5000
    ),
    verbose: true
  )

{:ok, text_plan} =
  LanceDB.Query.explain(LanceDB.Query.new(table, {:text, "elixir"}, text_column: :text),
    verbose: false
  )

{:ok, hybrid_plan} =
  LanceDB.Query.explain(
    LanceDB.Query.new(
      table,
      {:hybrid, [0.9, 0.1, 0.2], "elixir"},
      text_column: :text,
      rrf_k: 60
    ),
    verbose: false
  )
```

`Query.explain/2` uses the source and controls stored in the query. Its
`:verbose` option defaults to false. The query's `:timeout_ms` also limits
planning. Plans use the same native query builders as execution. They show
scan or index selection and query operators. Planning can read schema and
index metadata.

It does not execute the result scan or report measured timings and row
counts. Plan text is diagnostic and can change with the SDK.

Hybrid explanations show both channel plans and the package's RRF fusion and
output steps. Ordinary plans can include hidden sort columns. Final results
include these columns only if `:columns` requests them.

## Runtime diagnostics and result schemas

`Query.analyze/1` executes the query and returns its plan with runtime
metrics. Results include planning time, execution time, and output row
count:

```elixir
{:ok, analysis} =
  LanceDB.Query.analyze(
    LanceDB.Query.new(table, {:vector, [0.9, 0.1, 0.2]},
      columns: [:id, :text],
      limit: 10,
      timeout_ms: 5000
    )
  )

IO.puts(analysis)

{:ok, hybrid_analysis} =
  LanceDB.Query.analyze(
    LanceDB.Query.new(table, {:hybrid, [0.9, 0.1, 0.2], "elixir"}, select: [label: "upper(text)"])
  )
```

Analysis uses the same query value as `Query.explain/2`. It has no
verbosity option. It supports scans, vector searches, FTS, hybrid searches,
and retrieval by row ID or offset. Defaults match the related immediate
query API. This includes row limits. Each channel executes once.

Hybrid analysis also runs the configured reranker and reports its final row
count. Ordinary analysis reads results incrementally and discards them.
Hybrid ranking keeps its candidate sets.

Analysis evaluates expressions against data and returns execution errors.
The cooperative `:timeout_ms` includes preparation, planning, execution, and
fusion. Metrics exclude conversion of result rows to JSON and BEAM maps.
Planning time excludes earlier request preparation. Operator metrics can
overlap. Do not add them to calculate total elapsed time.

Metric names and plan text are diagnostic and can change with the SDK.

Use `Query.schema/1` to inspect result fields without executing the result
scan:

```elixir
{:ok, %LanceDB.ArrowSchema{fields: fields}} =
  LanceDB.Query.schema(
    LanceDB.Query.new(table, :scan,
      select: [label: "upper(text)", embedding: "vector"],
      with_row_id: true
    )
  )
```

This function uses the same query value as `Query.analyze/1`. It returns an
ArrowSchema with field names, types, nullability, and metadata. Table
definitions use Schema. Use `ArrowSchema.to_schema/1` to convert an output
schema to a table definition. The output schema includes final computed
aliases and ranking metadata. It excludes hidden planner columns.

Empty tables and empty query results still have an output schema. Hybrid
inspection plans both channels. It uses empty Arrow batches to determine the
fused schema.

Types describe Arrow output before JSON encoding. For example, a timestamp
field has a timestamp type, although returned row maps contain ISO strings.
Planning can read schema and index metadata. `:timeout_ms` limits planning.
Inspection cannot detect errors that occur only when expressions execute
against data.

## Search controls

Choose the vector column and the fields returned by `search/3`:

```elixir
{:ok, matches} =
  LanceDB.Query.search(table, [0.9, 0.1, 0.2],
    vector_column: :vector,
    columns: [:id, :text],
    filter: "category = 'code'",
    distance: :cosine,
    limit: 10,
    nprobes: 20,
    refine_factor: 3
  )
```

`:vector_column` accepts a string or atom. If you do not set this option,
the package selects the unique column compatible with the search mode and
query dimension. Set it when multiple columns match. Dimension errors report
the query dimension and the expected column dimensions. `:columns` accepts
a non-empty list of distinct string or atom field names. To return all
fields, do not set this option, or set it to `nil`.

Results always include `"_distance"`, even if you select other fields.
Projection can exclude vectors and other large fields from results.

For IVF indexes, `:nprobes` sets the number of partitions to search. Higher
values usually improve recall but increase latency. If you do not set a
value, the SDK default applies. For quantized indexes such as IVF PQ,
`:refine_factor` fetches up to `(limit + offset) * refine_factor`
candidates. It reranks candidates with their original vectors. Even a factor
of 1 recalculates candidate distances.

Without refinement, quantized index distances stay approximate. Both options
must be positive integers. They do not improve an exhaustive search without
an index. Match `:distance` to the index metric.

Use an exhaustive search to measure approximate-search recall:

```elixir
{:ok, exact_matches} =
  LanceDB.Query.search(table, [0.9, 0.1, 0.2],
    vector_column: :vector,
    columns: [:id],
    distance: :cosine,
    limit: 10,
    bypass_vector_index: true
  )
```

`bypass_vector_index: true` skips the vector index and calculates distances
from original vectors. It supports the same filter and projection. Large
tables can use much processing time. The default is false. Refinement
improves candidate ranking but does not guarantee globally exact results.
Test these settings with your data and latency requirements.

Invalid or unknown options raise `ArgumentError`. Database errors return
`{:error, reason}`.

### Filtering, distance bounds, and indexed data

Vector search uses `prefilter: true` by default. The SQL predicate limits
rows before nearest-neighbor selection. `prefilter: false` selects vector
candidates before it filters them. This can reduce latency. It can also
return fewer than `limit` rows, including no rows. A larger `:refine_factor`
can help find matches with a quantized index.

```elixir
{:ok, matches} =
  LanceDB.Query.search(table, [0.9, 0.1, 0.2],
    filter: "category = 'code'",
    prefilter: false,
    distance: :cosine,
    distance_range: {nil, 0.25},
    limit: 10
  )
```

`:distance_range` is `{lower, upper}`. The lower bound is inclusive. The
upper bound is exclusive. Either bound can be `nil` for an open range. If
you do not set a range, or set `nil`, this control is disabled. Bounds
round to float32 and must stay in increasing order after rounding.

Bounds use the selected distance metric. `:l2` returns squared Euclidean
distance. Dot distances can be negative. Quantized index results can have
approximate distances. Use refinement or exhaustive search to compare
original vector distances.

`fast_search: true` skips unindexed data when the selected index supports
this operation. It is available for ordinary, vector, full-text, and hybrid
queries. New appends can stay invisible until
`Maintenance.optimize_indices/1` adds them to the indexes. A vector search
without a vector index returns no indexed vector matches. For ordinary
queries, this setting limits scanned fragments only if a scalar filter index
can supply the matches. Without that index, they can still scan data.

This setting does not guarantee a snapshot or fresh data. Combining it with
`bypass_vector_index: true` raises `ArgumentError`. The default is false.

### Adaptive IVF probing

Use `:minimum_nprobes` and `:maximum_nprobes` to search more partitions when
initial probes find too few matches:

```elixir
{:ok, matches} =
  LanceDB.Query.search(table, [0.9, 0.1, 0.2],
    filter: "category = 'code'",
    minimum_nprobes: 2,
    maximum_nprobes: 20
  )
```

If you do not set a bound, its SDK default is 20. Set `maximum_nprobes: nil`
to search all partitions when necessary. The minimum and any numeric maximum
must be positive uint32 integers. The maximum must be at least the minimum.
Do not combine these bounds with `:nprobes`. The `:nprobes` option sets
both bounds to the same value.

Probing controls apply to IVF indexes. They do not affect exhaustive
searches. Additional probes can help find matches but do not guarantee exact
nearest neighbors.

### Search offsets and row IDs

All query types accept `:offset` (default 0) and `:with_row_id` (default
false):

```elixir
{:ok, page} =
  LanceDB.Query.search(table, [0.9, 0.1, 0.2],
    columns: [:id, :text],
    limit: 10,
    offset: 10,
    with_row_id: true
  )
```

Results include integer `"_rowid"` metadata, even when `:columns` is
specified. Row IDs identify stored rows. They are not application keys.
Table rewrites, compaction, and replacement can change them. Use an
application ID column for references that must stay valid.

Search offsets skip ranked results. They increase candidate work to at least
`limit + offset` before optional refinement. Ties, approximate candidate
selection, and concurrent writes can change pages. Hybrid search collects up
to `limit + offset` candidates from each channel. It fuses their ranks and
then skips the requested offset. A different candidate count can change
fused ranks.

This API does not supply a stable pagination cursor.

Hybrid search applies distance bounds, adaptive probes, and `:prefilter`
only to the vector channel. SQL text filtering occurs before BM25 ranking. A
text match can appear outside the vector distance range. `:fast_search`
applies to both channels. `:with_row_id` keeps the row IDs used for fusion
in final output. `Query.explain/2` also accepts the controls that apply to
the query.

## Batch and multivector search

`batch_search/3` runs independent searches against a fixed-size vector
column. Its input is a non-empty list of vectors with the same dimension.
Search options, including `limit` and `offset`, apply to **each query
vector**.

```elixir
vectors = [[1.0, 0.0, 0.2], [0.0, 1.0, 0.8]]
{:ok, rows} = LanceDB.Query.batch_search(table, vectors, limit: 5, columns: [:id])
by_query = Enum.group_by(rows, & &1["query_index"])
rows = LanceDB.Query.stream(LanceDB.Query.new(table, {:batch, vectors}, limit: 5), batch_size: 2)

{:ok, plan} =
  LanceDB.Query.explain(LanceDB.Query.new(table, {:batch, vectors}, limit: 5), verbose: false)

{:ok, analysis} = LanceDB.Query.analyze(LanceDB.Query.new(table, {:batch, vectors}, limit: 5))
{:ok, schema} = LanceDB.Query.schema(LanceDB.Query.new(table, {:batch, vectors}, columns: [:id]))
```

Results are flat maps with `_distance` and zero-based `query_index`. These
fields appear with `columns` or `select` projections and with batches
containing only one vector. A query with no matches contributes no rows. The
SDK can mix results from different queries. Use `query_index` to match
results to inputs. Tables with a stored column named `query_index` cannot
use batch search.

This name is reserved for metadata. Float32 batches can share a flat scan.
Float16 and float64 batches use independent typed query plans to preserve
precision and avoid an SDK 0.40 planning panic. Performance depends on the
index, filters, and query count.

`multivector_search/3` ranks documents that each contain several embeddings,
such as token embeddings. It returns one result set with `_distance`. Lance
sums each query embedding's smallest cosine distance to any embedding in the
document. Equivalently, the distance is the number of query embeddings minus
the sum of their best cosine similarities. Lower scores are better.

Cosine is the default and only supported metric.

```elixir
{:ok, documents} =
  LanceDB.Connection.create_table(
    db,
    "multivector_documents",
    [%{id: 1, embeddings: [[1, 0], [0, 1]]}, %{id: 2, embeddings: [[1, 0]]}],
    schema: [id: :int64, embeddings: {:list, {:vector, 2}}]
  )

{:ok, rows} =
  LanceDB.Query.multivector_search(documents, [[1, 0], [0, 1]],
    vector_column: :embeddings,
    limit: 5,
    columns: [:id]
  )

rows =
  LanceDB.Query.stream(LanceDB.Query.new(documents, {:multivector, [[1, 0], [0, 1]]}, limit: 5),
    batch_size: 1024
  )
```

Use `[[1, 0]]` for a single query embedding. The package checks the stored
column type to distinguish batch requests from multivector requests. If
multiple compatible columns exist, specify `vector_column`. Nested vector
columns are supported. Multivector results do not add `query_index`. Create
vector indexes on multivector columns with `distance: :cosine`.

Use the existing search options for index tuning and exhaustive search.
Indexed results can be approximate.

Batch and multivector sources support streaming and all three query
diagnostics. Each query has one source: batch, multivector, vector, text,
hybrid, scan, or direct retrieval. The package does not expose hybrid batch
or hybrid multivector queries. Query timeouts cover the entire request.
Stream timeouts cover the cursor lifetime.
