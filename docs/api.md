# API guide

[Back to README](../README.md#documentation).

Use `Connection`, `Table`, `Query`, and `Index` for common operations.
Use `History`, `Branch`, `Maintenance`, `Metadata`, `SchemaEvolution`,
and `Blob` for the other operations. `LanceDB.connect` calls
`Connection.connect`.

## API model

The public API uses typed connection and table handles, reusable queries and
schemas, and error and write result structs. These guides describe the current
source; see [installation](installation.md#installation) for release status and
[API migration](migration.md#api-migration) for changes from earlier checkouts.

```elixir
alias LanceDB.{Batch, Query, Schema, Table}
{:ok, db} = LanceDB.connect("./data/vectors")
schema = Schema.new(id: :int64, text: :string, vector: {:vector, 3})
{:ok, table} = LanceDB.Connection.create_empty_table(db, "documents", schema)

{:ok, %LanceDB.WriteResult{inserted: 1}} =
  Table.add(table, [%{id: 1, text: "Elixir", vector: [1.0, 0.0, 0.0]}])

query = table |> Query.new() |> Query.nearest_to([1.0, 0.0, 0.0]) |> Query.limit(20)
{:ok, rows} = Query.all(query)
rows = query |> Query.stream(batch_size: 256) |> Enum.to_list()
{:ok, plan} = Query.explain(query)
{:ok, arrow_schema} = Query.schema(query)
{:ok, output_schema} = LanceDB.ArrowSchema.to_schema(arrow_schema)
{:ok, target} = LanceDB.Connection.create_empty_table(db, "copy", output_schema)

{:ok, %LanceDB.WriteResult{inserted: copied}} =
  LanceDB.Table.add(
    target,
    LanceDB.Source.batches(Query.stream_batches(query)),
    commit: :once
  )

[batch | _] = Enum.to_list(Query.stream_batches(query))
{:ok, ipc} = Batch.to_ipc(batch)
{:ok, imported_batches} = Batch.from_ipc(ipc)
```

`Query.all/1` reads all results immediately. `Query.stream/2` reads results
on demand. Both use the same query options. Scans have a default limit of
100 rows. Searches have a default limit of 10 rows. Use
`Query.limit(query, nil)` to remove a scan limit.

A repeated query against a live handle can read new data. Use
`LanceDB.History.snapshot/2` to select a fixed version without changing the
source handle.

Operation failures return `{:error, %LanceDB.Error{kind: kind}}`. A stream
failure raises `LanceDB.Error` when the caller reads the stream. Match error
kinds instead of message text. Incorrect API arguments raise `ArgumentError`. Result rows have string field names. User metadata and raw statistics have
string keys.

Fixed API results use structs with atom fields.

`LanceDB.Execution.configure(reads: 64, writes: 16)` sets native operation
limits for the whole BEAM VM. Read cursors keep a read slot until they
close. If no slot is available, the operation returns `:busy`. Separate
write slots let cursors supply data to ingestion. These settings limit
concurrent operations. They do not limit memory for each query.

Telemetry covers all public operations. Start and stop events share an
operation ID. Failures use stable error kinds.

## Quick start

```elixir
{:ok, db} = LanceDB.connect("./data/vectors")

{:ok, table} =
  LanceDB.Connection.create_table(db, "documents", [
    %{id: "1", text: "Elixir and OTP", category: "code", vector: [1.0, 0.0, 0.2]},
    %{id: "2", text: "Sourdough bread", category: "food", vector: [0.0, 1.0, 0.8]}
  ])

{:ok, matches} =
  LanceDB.Query.search(table, [0.9, 0.1, 0.2],
    limit: 5,
    distance: :cosine,
    filter: "category = 'code'"
  )

# Results are maps with string keys, including a numeric "_distance".
IO.inspect(matches)

{:ok, %LanceDB.WriteResult{inserted: 1}} =
  LanceDB.Table.add(table, [
    %{id: "3", text: "Phoenix LiveView", category: "code", vector: [0.9, 0.1, 0.3]}
  ])

{:ok, 3} = LanceDB.Table.count_rows(table)
{:ok, 2} = LanceDB.Table.count_rows(table, "category = 'code'")
{:ok, %LanceDB.Schema{fields: fields}} = LanceDB.Table.schema(table)
{:ok, ["documents"]} = LanceDB.Connection.table_names(db)
{:ok, reopened} = LanceDB.Connection.open_table(db, "documents")
{:ok, %LanceDB.WriteResult{}} = LanceDB.Table.delete(reopened, "id = '3'")

# Permanently remove the table:
{:ok, :dropped} = LanceDB.Connection.drop_table(db, "documents")
```

`connect/0` uses `"./lancedb"` by default. Connections and tables are native
resource handles. The BEAM garbage collector releases these handles. Native
operations share a Tokio runtime. Connect, table open, cursor requests, job
waits, and writer cleanup run native waits on Tokio and reply by message,
leaving BEAM schedulers available. Many other native calls use dirty schedulers
and can retain one while waiting for storage.

The current source accepts `:timeout_ms` and session cache budgets in the third
argument to `connect/3`. Table opening accepts its own deadline and per-open
cache budgets. See [table-open controls](tables.md#table-open-controls-and-listing)
for options, cancellation limits, and `Table.cache_stats/1`.

## Records and search

Records use maps with atom or string keys. To create a table without an
explicit schema, provide a non-empty list. Its first record determines the
schema:

- Strings, signed 64-bit integers, floats, and booleans become scalar columns.
- Non-empty numeric lists become fixed-size float32 vector columns.
- Later rows can contain null scalar values. The first row cannot.
- All rows must have the same fields and compatible types.
- Vectors must have consistent dimensions and values representable as float32.
- Define an explicit schema for lists and nested maps. Schema inference does not
infer them.

Appends and upserts validate records against the stored schema, as described
above. Without `:vector_column`, search selects the unique column compatible
with the search mode and query dimension. Multiple matches require an explicit
column; dimension mismatches report the query and expected dimensions.
Supply your own embeddings.

Search supports `:l2` (default), `:cosine`, and `:dot` distances for floating
vectors, and `:hamming` for uint8 vectors. It
accepts a positive `:limit` (default 10) and an optional SQL `:filter`. SQL
predicates pass directly to LanceDB. Construct these expressions safely.

Table creation defaults to `mode: :create`. This mode fails if the table
exists. `mode: :exist_ok` opens an existing table without appending the
supplied records. `mode: :overwrite` replaces the table.

Operations return `{:ok, value}` or `{:error, reason}`. Invalid search
options or creation modes raise `ArgumentError`. Malformed records return
an error tuple. Table inspection returns logical `%LanceDB.Schema{}` values.
Query and batch inspection return `%LanceDB.ArrowSchema{}` descriptions.
Both contain `%LanceDB.Field{}` values.

Before creation, convert physical descriptions with
`ArrowSchema.to_schema/1`. This API does not expose LanceDB Cloud.

## Telemetry

The package sends operation events automatically through `:telemetry`:

* `[:lancedb, :operation, :start]`: System time and operation metadata.
* `[:lancedb, :operation, :stop]`: Duration in native time units and `:ok` or
`:error` status.
* `[:lancedb, :operation, :exception]`: Duration and standard exception
metadata.

Coverage includes `connect`, `add`, `delete`, `update`, `upsert`, and
`merge`. It also includes immediate query and diagnostic execution, plus
query stream open, next, and close calls. A stream has timings for
individual native calls. It has no single span across consumer work.
Standard span context links start and stop events. An `execution_id`
associates open, next, and close calls across each enumeration.

Immediate queries also attach `parent_operation_id`. Re-enumeration and
public calls from callbacks get independent execution IDs. Convenience
methods enter their canonical span once. Successful events do not include
records, SQL, credentials, or table handles. Exception metadata can contain
user data in reasons or stacktraces.

For the current source's per-session index and metadata cache counters, use
`LanceDB.Table.cache_stats/1`. It requires no metrics recorder and reports the
session actually used by the table. See [cache statistics](tables.md#table-open-controls-and-listing)
for shared-session scope and fields. The following metrics instead accumulate
across all connections in the native library. Native storage metrics are opt-in:

```elixir
:ok = LanceDB.Telemetry.enable_native_metrics() # before opening connections
catalog = LanceDB.Telemetry.catalog()
series = LanceDB.Telemetry.snapshot()
:ok = LanceDB.Telemetry.poll()
```

`poll/0` emits `[:lancedb, :native, :metric]` events. Scalars have a `value`
measurement. Histograms have `count` and `sum`, with cumulative `buckets`
in metadata. Metadata also includes the string `name`, `kind`, and
`attributes`. These are cumulative snapshots across the process. Exporters
must not sum repeated counter snapshots or treat histogram sums as
individual observations.

Attribute labels can contain storage paths. Use `catalog/0` to discover
metric names and units. New series appear as operations run.

You cannot disable or reset the native recorder. You can install it again
without changes. A conflicting native recorder returns an error. Schedule
`poll/0` with your own process or telemetry_poller. The package does not
start a polling process.
