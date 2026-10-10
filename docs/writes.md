# Writes and streaming

[Back to README](../README.md#documentation).

These examples use `db` and `table` handles. See the [API guide](api.md) for setup.
Match the table schema to each example.

## Input formats and commits

Select the input format and commit mode separately:

```elixir
alias LanceDB.{Connection, Query, Source, Table}
{:ok, conn} = Connection.connect("./data")
{:ok, table} = Connection.create_table(conn, "items", [%{id: 1}])
source = Source.rows(Stream.map(2..100, &%{id: &1}), batch_size: 25)
{:ok, %LanceDB.WriteResult{inserted: 99, commits: 1}} = Table.add(table, source)
batches = table |> Query.new(limit: nil) |> Query.stream_batches()
{:ok, copy} = Connection.create_empty_table(conn, "copy", id: :int64)
{:ok, %LanceDB.WriteResult{commits: 1}} = Table.add(copy, Source.batches(batches))
```

Insertion uses one commit by default. Use `commit: :batch` to commit each
input batch separately. An empty append changes no rows. An empty overwrite
removes all rows. `WriteResult.commits` counts insertion commits that the
operation confirms. It does not count input batches.

Other writes report `nil` if the SDK cannot give a commit count. Public
writes inside callbacks return the same result structs as direct calls. Each
public API call inside a callback creates a separate telemetry span.

## Streaming reads and ingestion

`LanceDB.Query.stream/2` returns a lazy Enumerable of row maps from a query
value. Use Elixir `Stream` transformations and `Enum` consumers:

```elixir
LanceDB.Query.stream(
  LanceDB.Query.new(
    table,
    :scan,
    columns: [:id, :text],
    filter: "id > 100"
  ),
  batch_size: 256
)
|> Stream.map(& &1["text"])
|> Enum.each(&IO.puts/1)

first_ten =
  LanceDB.Query.stream(LanceDB.Query.new(table, :scan, []), batch_size: 16) |> Enum.take(10)

matches =
  LanceDB.Query.stream(LanceDB.Query.new(table, {:vector, [0.9, 0.1, 0.2]}, limit: 100),
    batch_size: 20
  )

text_matches =
  LanceDB.Query.stream(
    LanceDB.Query.new(table, {:text, LanceDB.FTS.match("elixir", column: :text)}, []),
    batch_size: 1024
  )

hybrid_matches =
  LanceDB.Query.stream(LanceDB.Query.new(table, {:hybrid, [0.9, 0.1, 0.2], "elixir"}, limit: 50),
    batch_size: 1024
  )
```

Plain scans have a default limit of **100 rows** for immediate reads and
streams. Use `limit: nil` for an unlimited scan. Search streams keep the
search default of 10 rows. Supply `:limit` to set the maximum result count.
Other options follow the selected query or search API. These include
projection, supported ordering, offsets, filters, row IDs, and search
settings.

Streams use the same metadata and timestamp formats as immediate reads.

`:batch_size` defaults to 1024. It limits the rows returned by each native
batch request. The native cursor keeps one query execution and reads its
Arrow stream. It does not repeat queries with increasing offsets. Elixir
requests the next batch after it consumes the current batch. The SDK can
read data ahead of these requests.

Sorting and ranking can use more memory. Hybrid search collects both
candidate sets before it returns results. Its memory use also depends on
`limit + offset`. `Enum.to_list/1` collects all results in the caller. For
large scans, process each batch as you read it.

Stream creation checks options but does not access the database. Reading the
stream opens a new cursor. Each execution keeps its snapshot. Reading the
same stream again starts a new execution. That execution can read newer data
according to the handle's read-consistency settings. One process can create
a stream that another process reads.

Resume a suspended stream read in the process that started it.

Database errors raise `LanceDB.Error` when the caller reads the stream. A
cursor closes when its stream ends or stops early, for example with
`Enum.take/2` or `Enum.find/2`. It also closes on consumer exceptions,
consumer process exit, or resource garbage collection. Explicit stream
cleanup waits for native cleanup and read slot release. Process exit and
garbage collection request cancellation without waiting. `:timeout_ms`
limits the complete cursor lifetime from the start of the stream read.

It includes consumer pauses between batch requests. A timeout closes native
execution and raises an error on a later batch request. It does not
interrupt Elixir callbacks or remove rows already fetched into a batch.

For ingestion, `Table.add/3` reads a `Source.rows/2` enumerable in batches
with a size limit:

```elixir
{:ok, target} = LanceDB.Connection.create_empty_table(db, "imported", id: :int64, text: :string)
records = Stream.map(1..10000, &%{id: &1, text: "document #{&1}"})

{:ok, %LanceDB.WriteResult{inserted: 10000}} =
  LanceDB.Table.add(target, LanceDB.Source.rows(records, batch_size: 500), [])

rows = LanceDB.Query.stream(LanceDB.Query.new(target, :scan, filter: "id <= 100"), batch_size: 32)
{:ok, copy} = LanceDB.Connection.create_empty_table(db, "copy", id: :int64, text: :string)

{:ok, %LanceDB.WriteResult{inserted: 100}} =
  LanceDB.Table.add(copy, LanceDB.Source.rows(rows, batch_size: 25), [])
```

`Table.add/3` reads the source immediately and returns a `WriteResult`. It
uses the stored schema. It uses the same producer loop as
`Table.add(table,
Source.rows(rows))`. The default is one commit. Use
`commit: :batch` to keep confirmed earlier commits if a later batch fails.
`Error.details` then includes inserted rows, confirmed commits, and the last
confirmed version.

An empty append reports zero rows, zero commits, and a `nil` version. An
empty overwrite clears rows in one commit. See
[Ingestion controls and write
results](#ingestion-controls-and-write-results).

## Upserts and updates

`upsert/3` inserts new rows and replaces rows that match the `:on` columns.
It uses one merge operation. The default key is `:id`:

```elixir
{:ok, %{inserted: inserted, updated: updated}} =
  LanceDB.Table.upsert(table, [
    %{id: "1", text: "Updated Elixir document", category: "code", vector: [0.8, 0.2, 0.1]},
    %{id: "4", text: "New document", category: "code", vector: [0.7, 0.3, 0.2]}
  ], on: :id)
```

Use `on: [:tenant_id, :id]` for a composite key. Incoming keys must be
unique, non-null strings or integers. Supply every table column with
matching types. Upserts replace complete rows. Rows absent from the incoming
batch stay unchanged. Repeating a batch with the same keys adds no rows if
existing keys are unique.

Upsert does not add a uniqueness constraint. It does not remove existing
duplicate keys.

Use `update/3` for partial changes:

```elixir
{:ok, %{updated: updated_count, version: version}} =
  LanceDB.Table.update(table, %{text: "Elixir's concurrency model", category: "code"},
    filter: "id = '1'"
  )

# Explicit SQL expressions support calculations against the previous row values.
# For a table with a numeric `views` column:
{:ok, %{updated: updated_count, version: version}} =
  LanceDB.Table.update(table, %{views: {:sql, "views + 1"}}, filter: "category = 'code'")
```

Update results are `LanceDB.WriteResult` structs. They include the affected
row count and the version returned by the operation. This also applies when
no rows match.

Literal values can be strings, binaries, integers, floats, booleans, `nil`,
vectors, lists, struct maps, or `Calendar.ISO` date or date-time structs.
Timestamp literals use the precision and timezone rules below. The operation
escapes strings as SQL literals. It checks literal types and vector
dimensions against the table schema before writing.

Use `LanceDB.Expr` or `{:sql, expression}` for SQL expressions. These
expressions use the SDK's type conversion rules. Use `LanceDB.Expr` to
combine expression values. Do not insert untrusted text into SQL strings.

The current SDK cannot update rows in tables that contain blob columns. This
includes updates to other columns. These updates return an unsupported error
before execution. Use `upsert/3` or `merge/3` with complete records to
replace blob rows.

Without `:filter`, the operation updates **every row**. A filter with no
matches returns `{:ok, %LanceDB.WriteResult{updated: 0, version: version}}`. Invalid options or update value structures raise `ArgumentError`. Invalid
upsert records and database errors return `{:error, reason}`.

## Conditional merges

Use `Table.merge/3` for conditional updates and synchronization. Set each
action explicitly. `Table.merge/3` disables every action by default.
`upsert/3` enables insert and update:

```elixir
{:ok, result} =
  LanceDB.Table.merge(table, incoming_records,
    on: [:tenant, :id],
    when_matched_update_all: "source.revision > target.revision",
    when_not_matched_insert_all: true,
    timeout_ms: 30_000,
    use_index: true
  )
```

Enable at least one action:

- `:when_matched_update_all`: `true` updates all matched rows. A SQL string
updates only matches that meet the condition. Use `source.column` for incoming
values and `target.column` for stored values. If the condition is false, the row
stays unchanged. The row does not become an insert candidate.
- `:when_not_matched_insert_all`: `true` inserts source rows with no target
match.
- `:when_not_matched_by_source_delete`: `true` deletes target rows absent from
the source batch. A SQL string limits deletion. Use **unqualified target column
names**, such as `"tenant = 'a'"`.

For example, synchronize the complete incoming snapshot for one tenant:

```elixir
{:ok, result} =
  LanceDB.Table.merge(table, complete_tenant_snapshot,
    on: [:tenant, :id],
    when_matched_update_all: true,
    when_not_matched_insert_all: true,
    when_not_matched_by_source_delete: "tenant = 'a'"
  )
```

Deletion compares against the complete source in one merge. Pass a `Source`
to stream a snapshot. Separate merge calls on its chunks each apply deletion
independently. Every enabled action accepts an empty source. Unconditional
deletion with an empty source removes every target row. A deletion condition
does not limit updates or inserts.

Supply the source rows and update condition separately.

Incoming rows must include every table field. Keys must be unique, non-null
strings or integers. `:on` defaults to `:id` and also accepts composite
keys. Use unique keys in the target table. The merge does not add a
uniqueness constraint.

Results are `LanceDB.WriteResult` structs with `:inserted`, `:updated`,
`:deleted`, `:version`, and `:attempts` fields. `:timeout_ms` is a
positive integer in milliseconds. If you do not set a timeout, the SDK
default applies. `:use_index` defaults to true and controls the join-key
index. Do not insert untrusted text into SQL expressions. Invalid options
raise `ArgumentError`.

Invalid records and native failures return `{:error, reason}`.

## Ingestion controls and write results

Writes always return `%LanceDB.WriteResult{}` with the commit version and
applicable row counts. Unavailable fields are nil. Measured zero counts are
zero:

```elixir
{:ok, %{inserted: 2, version: version}} =
  LanceDB.Table.add(table, records,
    write_parallelism: 2,
    max_rows_per_file: 100_000,
    max_rows_per_group: 1024,
    max_bytes_per_file: 64_000_000
  )

{:ok, %{deleted: deleted, version: deletion_version}} =
  LanceDB.Table.delete(table, "id < 100")
```

File and row limits are positive uint32 values. Byte limits are targets, not
absolute limits. Omitted controls keep SDK defaults. `mode: :overwrite`
replaces rows through the existing handle with its stored schema. Adds,
deletes, updates, upserts, and conditional merges share the same result
type.

For one commit over an Enumerable, use `commit: :once`:

```elixir
{:ok, %{inserted: count, version: version}} =
  LanceDB.Table.add(
    table,
    LanceDB.Source.rows(record_stream,
      batch_size: 512
    ),
    commit: :once,
    write_parallelism: 1
  )
```

The default `commit: :once` commits the whole input. Use `commit: :batch` to
keep independently committed batches if a failure occurs. Single-commit
imports abort on input or validation failures without committing partial
rows. Producer exceptions return an error with kind `:source` after
cancellation. A producer interrupted during the final commit might not know
whether the commit succeeded. The native channel holds at most one pending
input batch, in addition to native writer buffers.

Aborted writes can leave unreferenced files for normal cleanup. Empty append
makes no changes and returns version `nil`. Empty overwrite clears rows.
Use `commit: :once` for overwrite. This prevents later batches from
overwriting earlier batches.

To observe native progress from another process, pass `progress: {pid, tag}`
to `Table.add/3`. The recipient receives
`{:lancedb_write_progress, tag,
progress}`. Progress contains cumulative
`output_rows`, `output_bytes`, `elapsed_ms`, nullable `total_rows`,
`active_tasks`, `total_tasks`, and `done`. The tag can be any Elixir
term, including a reference. Drain these messages regularly. `done` means
processing ended, including failure.

The API result determines whether a commit succeeded. Batch-mode counters
reset per batch.

## Streaming creation, merges, and column backfills

Define an explicit schema to create a table from a `Source`. It returns a
table after one commit. Creation from a plain row list can infer its schema:

```elixir
alias LanceDB.{Batch, Connection, Query, Schema, Table}
schema = Schema.new(id: :int64, text: :string)
rows = Stream.map(1..10000, &%{id: &1, text: "document #{&1}"})

{:ok, table} =
  LanceDB.Connection.create_table(
    db,
    "documents",
    LanceDB.Source.rows(rows, schema: schema, batch_size: 256),
    []
  )

batches = table |> Query.new(limit: nil) |> Query.stream_batches(batch_size: 256)

{:ok, copy} =
  LanceDB.Connection.create_table(
    db,
    "documents_copy",
    LanceDB.Source.batches(batches, schema: schema),
    []
  )

{:ok, result} =
  LanceDB.Table.upsert(
    copy,
    LanceDB.Source.rows([%{id: 1, text: "updated"}, %{id: 10001, text: "new"}], batch_size: 256),
    []
  )
```

`Connection.create_table/4` accepts `Source.rows/2` or `Source.batches/2`
with an explicit schema. Creation supports `:create`, `:overwrite`,
`:exist_ok`, schema metadata, and the creation controls described above.
Empty input creates an empty table. `:exist_ok` can return the existing
table without consuming the source.

`Table.merge/3` accepts either Source representation with conditions, join
keys, index control, and timeout. `Table.upsert/3` enables insert and update
together. Every operation uses one commit. The merge evaluates deletion
against the complete source. Merge keys must be non-null and unique across
the whole source. Native validation keeps a set of keys.

Memory use thus grows with distinct keys, although row batches use bounded
producer buffering. Streamed merges return `WriteResult`.

For computed columns, a callback receives each native batch from an
independent snapshot. It returns a native batch or a list of row maps for
new columns:

```elixir
{:ok, result} = LanceDB.SchemaEvolution.backfill_columns(table, [text_length: :int64], fn batch ->
  {:ok, rows} = Batch.to_rows(batch)
  Enum.map(rows, &%{text_length: String.length(&1["text"])})
end, read_columns: [:text], batch_size: 256)
```

The callback runs in the caller's BEAM process. It must keep row count and
order. `read_columns` limits its input fields. Output columns use the
supplied schema, including nested types and field metadata. Before it
starts, the operation checks that the target still matches the source
snapshot. It commits against that dataset version.

The SDK's commit-conflict rules apply to concurrent writes.

For precomputed values, use `SchemaEvolution.add_from_source/3`. Supply the
output schema on `Source.rows/2` or `Source.batches/2`. You can also supply
`expected_version`. Supply exactly one output row per visible table row in
physical scan order. Exclude deleted rows. The callback API handles this
alignment for you.

These operations return `WriteResult`.

A failed producer aborts input without committing a partial source.
Backfills check exact row counts and wait for successful EOF, even after
receiving enough rows. Failed writes can leave uncommitted storage files, as
with SDK writes. An error after the final commit starts can leave the commit
outcome unknown.
