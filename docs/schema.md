# Schemas and data types

[Back to README](../README.md#documentation).

These examples use `db` and `table` handles. See the [API guide](api.md) for setup.
Match the table schema to each example.

## Explicit schemas and empty tables

Define columns before loading data:

```elixir
schema = [
  %{name: :id, type: :string, nullable: false},
  {:text, :string},
  {:score, :float64},
  {:vector, {:vector, 3}}
]

{:ok, table} = LanceDB.Connection.create_empty_table(db, "typed_documents", schema)
{:ok, 0} = LanceDB.Table.count_rows(table)

{:ok, %LanceDB.WriteResult{inserted: 1}} =
  LanceDB.Table.add(table, [%{id: "1", text: nil, score: 1, vector: [1, 0, 0]}])

# The same schema can accompany initial records (including an empty list):
{:ok, other} = LanceDB.Connection.create_table(db, "other_documents", [], schema: schema)
```

Schemas support these primitive types:

- Strings and binary data: `:string`, `:large_string`, `:binary`, and `:large_binary`.
- Booleans and floats: `:boolean`, `:float16`, `:float32`, and `:float64`.
- Signed integers: `:int8`, `:int16`, `:int32`, and `:int64`.
- Unsigned integers: `:uint8`, `:uint16`, `:uint32`, and `:uint64`.
- Dates: `:date32` and `:date64`.

`{:vector, dimension}` defines a fixed-size float32 vector. Other vector
item types are also available. Schemas support timestamps, lists, structs,
decimals, times, durations, dictionaries, maps, JSON, and blobs. The
sections below describe these types.

Field names accept atoms or strings. Tuple fields are nullable by default.
Map field definitions accept `nullable: false`. Schemas must have at least
one field and unique field names. Both creation functions accept `:mode`.

Appends and upserts use the stored schema. They accept nulls in the first
row and integers for float columns. Nullable vector fields also accept
nulls. Every record must contain every field. Use `nil` explicitly for
nulls. Vector items must be numbers.

Vector dimensions must match the schema. Missing fields, extra fields,
incompatible values, and nulls in required fields cause an error before the
batch write. Empty appends change no rows. Empty upserts succeed with zero
affected rows but can still create a version.

### Creation controls

All creation functions accept `storage_options: %{...}` and an explicit
`location: "..."`. This includes empty tables, row streams, and Arrow
batches. Creation combines the connection's storage options with the
supplied overrides. It keeps these options for remote locations, including
when the connection is local. Reopen a custom location with
`Connection.open_table(db, name, location:
location)`. `mode: :exist_ok`
opens it without changing settings or consuming input.

Use `max_rows_per_file`, `max_rows_per_group`, and `max_bytes_per_file` to
set initial write sizes. These values must be positive uint32 integers. The
byte limit is a target, not an absolute maximum. `data_storage_version`
accepts `:stable`, `:next`, or `"legacy"`, `"2.0"`, `"2.1"`, `"2.2"`,
`"2.3"`. In the current SDK, stable is 2.1 and next is 2.3.
`enable_stable_row_ids` and `enable_v2_manifest_paths` accept explicit
booleans.

Omitted settings keep the SDK and connection defaults. Explicit typed
settings override the related storage option keys.

### Blobs

Declare `:blob` fields to store binary payloads with Lance blob v2. Small
values can stay inline. Larger values use packed or dedicated sidecar files.
Blob tables use file format 2.2 or later. They use stable row IDs by
default. Blob fields can be nested in lists and structs.

Regular queries return descriptors. To read a payload, use a fetch API.
Fetch APIs support top-level blobs and blobs nested in structs. The pinned
SDK cannot fetch individual list elements by row ID.

```elixir
alias LanceDB.{Blob, Connection, Query, Schema, Table}

schema = Schema.new(id: :int64, payload: :blob)
{:ok, assets} = Connection.create_table(db, "assets",
  [%{id: 1, payload: <<0, 255, 128, 42>>}], schema: schema)

{:ok, [row]} = Query.query(assets, filter: "id = 1", with_row_id: true)
row_id = row["_rowid"]
{:ok, [bytes]} = LanceDB.Blob.fetch(assets, :payload, [row_id])
{:ok, [prefix]} = LanceDB.Blob.fetch_ranges(assets, :payload, [{row_id, 0, 2}])
{:ok, [blob]} = LanceDB.Blob.open(assets, :payload, [row_id])

{:ok, prefix} = Blob.read(blob, 2)       # advances the cursor
{:ok, suffix} = Blob.read_range(blob, 2, 2) # leaves the cursor unchanged
{:ok, :ok} = Blob.seek(blob, 0)
chunks = Blob.stream(blob, chunk_size: 65_536) # lazy reads from byte zero
Enum.each(chunks, &IO.binwrite/1)
{:ok, :ok} = Blob.close(blob)
```

Fetches keep input order, duplicate row IDs, empty bytes, and nulls (`nil`).
Use `_rowid` from the same table version. This ID differs from an
application ID or a row offset. Specify byte ranges as `{offset, length}`.
Each range must fit within the blob. `Blob.read/1` reads from the cursor to
EOF.

`read/2` reads up to the specified length. Both functions return an empty
binary at EOF. `read_ranges/2` keeps request order. Use `Blob.size/1`,
`tell/1`, `info/1`, and `closed?/1` to inspect the handle.

Handles shared across processes share a synchronized cursor and close state.
Separate fetches have independent cursors. `Blob.stream/2` is repeatable and
leaves the cursor unchanged. The stream uses the handle without owning it.
If you stop enumeration early, the handle stays open. Errors raise during
enumeration.

Close the handle when you finish. Garbage collection also releases native
references. Keep the files for the underlying table version until reads
finish.

Rows, streams, and Arrow batches accept binary blob inputs and `nil`. Query
blob descriptors are read results. You cannot ingest these descriptors as
payloads. Use `:binary` for ordinary binary columns that return bytes in
query results. The package manages the blob Arrow extension marker. It keeps
other field metadata.

You cannot add blob types through a type cast.

`Blob.reference(uri)` supplies an external object instead of bytes. Creation
and `Table.add/3` accept `external_blob_mode: :reference` or `:ingest`.
Reference mode is the default and keeps the URI. Ingest mode copies bytes
into Lance storage. For reference URIs outside registered external bases,
set `allow_external_blob_outside_bases: true`. Referenced objects must stay
available.

You can remove ingested objects after the write succeeds. These options
apply to each write. The package does not save them as table defaults.
`blob_pack_file_size_threshold` sets the byte limit for packed sidecar
files. It must be a positive uint32 integer. The SDK default is 1 GiB.

### Dates, integers, and binary data

```elixir
{:ok, events} = LanceDB.Connection.create_empty_table(db, "events", [
  id: :uint64, count: :int32, day: :date32, archived_on: :date64, payload: :binary
])

{:ok, %LanceDB.WriteResult{inserted: 1}} = LanceDB.Table.add(events, [%{
  id: 18_446_744_073_709_551_615, count: -12,
  day: ~D[2024-02-29], archived_on: "2024-03-01", payload: <<0, 255, 128>>
}])
```

Signed integer fields accept `-2^(bits-1)..2^(bits-1)-1`. Unsigned integer
fields accept `0..2^bits-1`. The package rejects floats, numeric strings,
and integers outside these ranges. All integer widths support merge and
upsert keys. For extreme integers in SQL expressions, cast a quoted decimal
string to prevent SQL literal rounding. For example, use
`arrow_cast('18446744073709551615',
'UInt64')`.

Dates accept `Calendar.ISO` Date structs, `YYYY-MM-DD` strings, or epoch
integers. `:date32` uses days since 1970-01-01. `:date64` uses milliseconds
since 1970-01-01. Its values must be at midnight, so use a multiple of
86,400,000. Years must be 0001–9999. Both types return ISO date strings,
including through streaming.

The package rejects invalid calendar dates and fractional days. Use
timestamp fields for date-time structs.

`:binary` accepts ordinary Elixir binaries, including empty values, zero
bytes, and invalid UTF-8. Query results return the original bytes. Use only
UTF-8 values for `:string`. Binary values stay distinct from strings in
stored schemas, computed projections, direct retrieval, and streaming. These
primitive types also work in lists and structs. The same nullability rules
apply.

Define an explicit schema for these types. Schema inference uses signed
int64 for integer values and strings for binaries. You can write returned
rows directly to another table with a matching schema.

### Decimals, times, large values, dictionaries, and maps

Define an explicit schema for these types. They support row ingestion, Arrow
batches, IPC, streaming reads, appends, and merges:

| Schema type | Input and result values |
| --- | --- |
| `{:decimal128, precision, scale}` | Exact decimal strings. Precision is 1–38. |
| `{:decimal256, precision, scale}` | Exact decimal strings. Precision is 1–76. |
| `{:time32, :second \| :millisecond}` | ISO time strings, `Time` structs, or integer ticks since midnight. Reads return ISO time strings. |
| `{:time64, :microsecond \| :nanosecond}` | The same inputs as time32, with finer precision. Strings keep exact nanoseconds. |
| `{:duration, unit}` | Signed int64 ticks in the specified unit, including negative values. Reads return integers. |
| `{:fixed_size_binary, width}` | Exactly `width` bytes. Reads return an Elixir binary. |
| `:large_string`, `:large_binary` | Strings or binaries with Arrow's 64-bit offsets. |
| `{:large_list, item_type}` | Lists with 64-bit offsets. Supports `item_nullable: false` and custom item `Field`s. |
| `{:dictionary, key_type, value_type}` | Decoded scalar values. The package manages integer Arrow dictionary keys. |
| `{:map, key_type, value_type}` | Lists of `{key, value}` pairs. Reads return pairs and keep non-string keys. |

Decimal strings use plain notation without exponents. The package rejects
floats, overflow, and nonzero digits beyond the specified scale. Scale
ranges from -128 through precision. Negative scales represent multiples of
powers of ten. Arrow 58 does not support SQL literal updates of
negative-scale decimals. Use `Table.upsert/3` or `Table.merge/3` with
complete rows.

Other decimal literal updates are supported. Schema evolution supports
conversions between decimal types, subject to SDK casting rules.

Times must fit within one day without rounding. The package rejects offsets
and leap seconds. `Time` structs must use `Calendar.ISO`. Duration units
are `:second`, `:millisecond`, `:microsecond`, and `:nanosecond`.

Dictionary key types are the signed and unsigned integer widths. Values can
be strings, large strings, binaries, large binaries, integers, or
floating-point numbers. Dictionary encoding is a physical storage detail.
Callers write and read decoded values. The package manages dictionary
indices. Dictionaries and maps can be nested in lists and structs.

For maps, including nested maps, use `data_storage_version: "2.2"` or a
later format. Map keys must be non-null and unique after Arrow conversion.
For example, different input numbers can become the same float32 key after
rounding. The operation rejects these duplicate keys before it writes the
batch.

Input accepts Elixir maps with string keys. Atom keys become strings. Use
pairs for other key types or to keep entry order. Values are nullable by
default. Custom key and value `Field` definitions keep child names,
nullability, and metadata. `{:map, entries_field}` also keeps these
properties.

Lance 10 does not support sorted Arrow maps. SQL literal updates cannot
replace complete maps. Use upsert or merge to replace a map.

```elixir
schema = LanceDB.Schema.new(
  id: :int64,
  amount: {:decimal128, 12, 2},
  elapsed: {:duration, :microsecond},
  labels: {:map, :int64, :string}
)

{:ok, table} = LanceDB.Connection.create_table(db, "measurements", [
  %{id: 1, amount: "123.45", elapsed: 1500, labels: [{1, "ready"}]}
], schema: schema, data_storage_version: "2.2")
```

### Native JSON

Declare `:json` to store Lance's native JSONB extension. Write ordinary
maps, lists, text strings, numbers, or booleans. Text becomes a JSON string
value. The package does not parse it as a JSON document. To ingest a JSON
document from text, first use `JSON.decode!/1`.

```elixir
{:ok, table} = LanceDB.Connection.create_table(db, "documents", [
  %{id: 1, data: %{title: "Elixir", tags: ["beam", nil]}},
  %{id: 2, data: LanceDB.JSON.null()},
  %{id: 3, data: nil}
], schema: [id: :int64, data: :json])
```

`nil` represents SQL NULL. `LanceDB.JSON.null/0` represents the JSON value
null. Reads return ordinary Elixir values with string object keys. A
non-null JSON field containing JSON null returns `%LanceDB.JSON{value: nil}`. Nulls in JSON arrays and objects stay ordinary nil. `LanceDB.JSON.new/1`
validates and wraps a JSON value.

Strings must be UTF-8. Object key aliases must be unique. Integers must fit
int64 or uint64. JSON can be nested in lists, structs, and maps. SDK JSON
functions work in filters and projections. For example, use
`E.call(:json_get_string, [E.col(:data), "title"])`.

`Table.schema/1` reports logical `:json` fields. Physical `ArrowSchema`
inspection keeps the storage representation and extension metadata. Native
storage uses LargeBinary with `lance.json`. Query batches use UTF-8 with
`arrow.json`. `ArrowSchema.to_schema/1` converts either representation to
logical `:json`. Query batches can supply data for another JSON table.

The native adapter converts the extension representation without converting
batches to Elixir rows.

### Lists and structs

Use explicit schemas to store variable-length lists and nested records:

```elixir
schema = [
  id: :int64,
  tags: {:list, :string},
  profile: {:struct, [name: :string, scores: {:list, :float64}]},
  events: {:list, {:struct, [at: {:timestamp, :microsecond, "UTC"}, label: :string]}}
]

{:ok, table} = LanceDB.Connection.create_empty_table(db, "nested", schema)
{:ok, %LanceDB.WriteResult{inserted: 1}} = LanceDB.Table.add(table, [%{
  id: 1,
  tags: ["elixir", nil],
  profile: %{name: "Ada", scores: [1.0, 2.5]},
  events: [%{at: ~U[2026-09-10 12:00:00Z], label: "created"}]
}])

{:ok, rows} = LanceDB.Query.query(table,
  select: [name: "profile.name", events: "events"],
  filter: "array_contains(tags, 'elixir')"
)

# Replace the entire list or struct column using ordinary Elixir values.
{:ok, %{updated: 1}} = LanceDB.Table.update(table, %{
  tags: [], profile: %{name: "Ada", scores: [3.0]}
})
```

`{:list, type}` accepts any supported element type, including another list,
a struct, a timestamp, or a vector. Empty lists and nullable items are
supported. Use `{:list, type, item_nullable: false}` to reject null items.
The containing field's `nullable` option controls whether the list itself
can be nil.

`{:struct, fields}` uses the same field definitions as the table schema,
including nullability for each child. A struct must have at least one field.
Values use maps with string or atom keys. Every child field must be present,
including nullable children. The package rejects missing fields, extra
fields, duplicate atom and string aliases at any depth, and incompatible
values. Null structs are supported when their children are nullable.

Lance 10 cannot store a null struct with required children, including
through nested structs. The package rejects these literals before a write
starts. The pinned SQL planner cannot cast struct update literals to
required child fields. For these columns, use `upsert/3` or `Table.merge/3`
with complete records. Creation, appends, and merges support required
children. Use the same method for nested field names that Arrow's SQL type
parser cannot keep.

Examples include names with double quotes. Updates reject these cases before
execution to prevent changes to field names or replacement of values with
nulls.

Creation, appends, upserts, conditional merges, and streamed ingestion
validate nested values against the stored schema. Literal updates replace
whole top-level columns. The package does not expose partial updates through
nested paths. Nested timestamps follow the same precision and timezone rules
as top-level timestamps. Query results contain maps with string keys and
lists. This applies to vector, FTS, and hybrid search, computed projections,
direct retrieval, and streaming.

Schema inspection describes child fields recursively for structs and list
items.

SQL-added columns can produce these supported types. Examples include
`make_array(text)` and `named_struct('label', text)`. Use `make_array(...)`
for computed or empty arrays. The pinned SDK's bracket-array syntax only
handles non-empty literals reliably. Rename, drop, and nullability
operations accept nested field paths. See schema evolution below.

Without an explicit schema, numeric lists use vector inference. Define an
explicit schema for variable-length lists and structs. Lists of vectors
support `multivector_search/3` with cosine distance. See
[batch and multivector search](queries.md#batch-and-multivector-search).

### Timestamps

Define timestamp precision and, optionally, timezone metadata:

```elixir
schema = [
  id: :int64,
  occurred_at: {:timestamp, :microsecond, "UTC"},
  local_time: {:timestamp, :millisecond}
]

{:ok, events} = LanceDB.Connection.create_empty_table(db, "events", schema)

{:ok, %LanceDB.WriteResult{inserted: 1}} =
  LanceDB.Table.add(events, [
    %{id: 1, occurred_at: ~U[2026-09-10 12:30:00.123456Z], local_time: ~N[2026-09-10 14:30:00.123]}
  ])

{:ok, rows} = LanceDB.Query.query(events)
# Timestamp values are "2026-09-10T12:30:00.123456Z" and "2026-09-10T14:30:00.123".

{:ok, _} = LanceDB.Table.update(events, %{occurred_at: ~U[2026-09-10 12:31:00Z]}, filter: "id = 1")
{:ok, rows} = LanceDB.Query.query(events, filter: "occurred_at >= to_timestamp('2026-09-10T12:00:00Z')")
```

| Schema type | Meaning | Accepted date-time values |
| --- | --- | --- |
| `{:timestamp, unit}` or `{:timestamp, unit, nil}` | Wall-clock time without a timezone | `NaiveDateTime` or ISO strings without an offset |
| `{:timestamp, unit, timezone}` | An instant with Arrow timezone metadata | `DateTime` or ISO strings with `Z` or `±HH:MM` |

Units are `:second`, `:millisecond`, `:microsecond`, and `:nanosecond`.
Timezones are strings. Use `"UTC"`, IANA names such as `"Europe/Vienna"`,
or fixed offsets such as `"+02:00"`. A timezone applies to the whole
column. Each input's offset determines its instant. The SDK does not store
the original offset for each row.

Explicit offsets prevent ambiguity during daylight-saving transitions.

Both forms accept signed 64-bit integers in the field's unit since
`1970-01-01T00:00:00`. For instants, this origin is UTC. For timestamps
without a timezone, it is a nominal wall-clock origin. For example, `-1` in
a nanosecond column means one nanosecond before that origin. Strings must
use `YYYY-MM-DDTHH:MM:SS`. They can include 1–9 fractional digits and the
appropriate offset.

Elixir date-time structs must use `Calendar.ISO`. The package encodes them
as ISO strings, including when you write them to string columns.

Values must fit the column's precision exactly. A millisecond field rejects
a microsecond value with a nonzero sub-millisecond fraction. The package
rejects floats, invalid dates, timezone mismatches, leap seconds, overflow,
and dates outside years 0001–9999. Nanosecond storage has a narrower signed
64-bit range, approximately 1677–2262. `nil` follows the field's nullability
setting.

All query and search results return timestamp strings. Instants use UTC with
`Z`. Wall-clock timestamps have no offset. This keeps exact nanoseconds and
prevents rounding of historical timezone offsets. Stored timezone metadata
stays visible through `LanceDB.Table.schema/1`. You can write returned
strings back unchanged.

Use `DateTime.from_iso8601/1` or `NaiveDateTime.from_iso8601/1` when
Elixir's microsecond precision is sufficient. Keep strings or epoch integers
for nanoseconds.

Timestamp fields support explicit creation, empty tables, appends, upserts,
conditional merges, literal updates, and B-tree indexing. SQL can also add
timestamp columns. For example,
`LanceDB.SchemaEvolution.add_columns(table, at:
"to_timestamp_micros(0)")`
adds a microsecond timestamp without a timezone. SQL expressions use the
SDK's casting rules. Timestamp columns cannot be merge keys. Use strings or
integers for merge keys.

Schema inference keeps ISO strings as strings. To create a table with
date-time structs, define an explicit schema.

## Schema evolution

Add columns without recreating a table:

```elixir
{:ok, %LanceDB.WriteResult{version: version}} =
  LanceDB.SchemaEvolution.add_columns(table,
    text_length: "CAST(length(text) AS BIGINT)",
    status: "'new'",
    note: "CAST(NULL AS STRING)"
  )
```

`add_columns/2` accepts a map or list of `{name, expression}` pairs.
Expressions can be SQL strings or `LanceDB.Expr` values. The SDK evaluates
the expressions against existing columns and rows. They **set existing row
values once**. They do not create defaults or generated values for later
writes. New appends and upserts must supply the added fields.

Multiple expressions in one call refer to the schema before that call. Use
separate calls for additions that depend on each other.

The package infers types and nullability from the expressions. Use explicit
SQL casts when necessary, especially for nulls and numeric literals. Use
only result types that the package supports. This includes lists and
structs. The package rejects bare `NULL` and unsupported types before a
schema change. Adding an existing column also returns an error.

Callers supply SQL expressions. Quote string literals. Do not interpolate
untrusted text.

Rename columns, change nullability, or convert types:

```elixir
{:ok, %LanceDB.WriteResult{version: version}} =
  LanceDB.SchemaEvolution.alter_columns(table,
    text: [rename: :title],
    id: [nullable: false]
  )

# Both changes can be applied to one column:
{:ok, %LanceDB.WriteResult{version: version}} = LanceDB.SchemaEvolution.alter_columns(table, title: [rename: :body, nullable: true])
```

Each alteration must specify `:rename`, `:nullable`, or `:type`. Making a
column non-nullable fails if existing rows contain nulls. Rename targets
cannot reuse existing names, including in swaps. If necessary, use an
intermediate name in separate calls. Indexes continue to reference renamed
fields. Their index names stay unchanged.

Use the new column names in queries and later writes.

Drop columns that are no longer needed:

```elixir
{:ok, %LanceDB.WriteResult{version: version}} = LanceDB.SchemaEvolution.drop_columns(table, [:note, :status])
```

The SDK removes indexes on dropped columns. It keeps the remaining fields
and rows. Keep at least one column. Dropped data can still exist in kept
versions. Use compaction and old-version cleanup to reclaim physical
storage. Dropping a column does not immediately erase historical data.

All three operations return `{:ok, %LanceDB.WriteResult{}}` with the
committed table version. They also work on empty tables. `add_columns/2`
adds top-level names without dots. Rename, nullability, and drop operations
accept nested paths as strings or atoms. Use backticks to quote names
containing dots or backticks. For example, use `` "data.` child.with.dot`"
``.

Rename targets are leaf names without dots. Names must stay unique within
each parent. Different parents can use the same child name. The package
rejects duplicate paths, including quoted aliases, within one call. It also
rejects ancestor and descendant paths in the same call. Split overlapping
edits into separate operations.

```elixir
{:ok, %LanceDB.WriteResult{version: _}} = LanceDB.SchemaEvolution.alter_columns(table, [{"data.count", [rename: :total, nullable: false]}])
{:ok, %LanceDB.WriteResult{version: _}} = LanceDB.SchemaEvolution.drop_columns(table, ["data.obsolete"])
{:ok, %LanceDB.WriteResult{version: _}} = LanceDB.SchemaEvolution.alter_columns(table, count: [type: :int64])
{:ok, %LanceDB.WriteResult{version: _}} = LanceDB.SchemaEvolution.alter_columns(table, counts: [type: {:list, :int64}])
```

`:type` accepts the same type definitions as explicit schemas. The SDK
accepts compatible conversions within integer, floating-point, temporal,
string, binary, and list families. It rejects conversions between families,
such as integer to float. Conversions rewrite column data. Drop attached
indexes before casting. Recreate them after the conversion.

Invalid casts and integer overflow fail without committing a new version.
Floating-point narrowing and temporal conversions can lose precision. Select
the destination type carefully.

**Pinned SDK limitation:** Lance 10 panics when it rewrites an individual nested
field's type. The package rejects nested `:type` changes before calling that
code. Nested rename, drop, and nullability changes are supported. Top-level
conversions of whole lists are also supported. The SDK does not support casts of
whole structs.

Invalid argument shapes raise `ArgumentError`. Expression, schema, and SDK
errors return `{:error, reason}`. Reopen other table handles to get the
latest schema before writing with it. Separate evolution calls produce
separate commits. They do not form one migration transaction.

## Schema and field metadata

Attach metadata at creation, independently of row values:

```elixir
{:ok, table} = LanceDB.Connection.create_empty_table(db, "annotated", [
  %{name: :id, type: :int64, metadata: %{description: "Document ID"}},
  {:details, {:struct, [%{name: :label, type: :string, metadata: %{language: "en"}}]}}
], schema_metadata: %{owner: "search-team"})

{:ok, %{"owner" => "search-team"}} = LanceDB.Metadata.schema(table)
{:ok, %LanceDB.WriteResult{version: version}} = LanceDB.Metadata.update_schema(table, %{owner: "analytics", obsolete: nil})
{:ok, %LanceDB.WriteResult{version: version}} = LanceDB.Metadata.update_field(table, "details.label", %{language: "de"})
{:ok, %LanceDB.WriteResult{version: version}} = LanceDB.Metadata.update_field(table, :id, %{unit: "identifier"}, replace: true)
{:ok, %LanceDB.WriteResult{version: version}} = LanceDB.Metadata.update_schema(table, %{}, replace: true)
```

Metadata maps accept string or atom keys and UTF-8 string values. Updates
merge by default. Use `nil` to delete a key. Use `replace: true` to replace
schema metadata or user field metadata. An empty replacement clears that
metadata. Field replacement keeps extension markers that identify the
field's logical type.

An attempt to change these markers returns an unsupported error.

Updates create versions. Historical handles reject updates. Branches and
shallow clones keep their metadata. `Table.schema/1` returns a
`LanceDB.Schema` with `:metadata` and `:fields`. Each `LanceDB.Field` keeps
its `:metadata`, including nested fields. Empty metadata maps stay `%{}`.

`Metadata.schema/1` returns only schema metadata, including `%{}` when
empty.
