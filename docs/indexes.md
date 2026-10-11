# Indexes

[Back to README](../README.md#documentation).

These examples use `db` and `table` handles. See the [API guide](api.md) for setup.
Match the table schema to each example.

## Index management

Create indexes, inspect their coverage, and remove them by name:

```elixir
{:ok, :created} = LanceDB.Index.create(table, :id, type: :btree, name: "id_lookup")
{:ok, indices} = LanceDB.Index.list(table)
# Each entry is an Index.Description: index.name, index.columns, index.index_type.
{:ok, stats} = LanceDB.Index.stats(table, "id_lookup")
{:ok, :dropped} = LanceDB.Index.drop(table, "id_lookup")

# Once the table has enough vector data for training:
{:ok, :created} =
  LanceDB.Index.create(table, :vector,
    type: :ivf_flat,
    name: "vector_cosine",
    distance: :cosine,
    num_partitions: 1
  )

{:ok, matches} = LanceDB.Query.search(table, [1, 0, 0], distance: :cosine)
```

Each index covers one column. Supported types:

- `:auto` (default): lets LanceDB choose an index for the column.
- `:btree`: scalar equality and range filters.
- `:bitmap`: scalar columns with few distinct values.
- `:label_list`: list membership filters with `array_has_any` and `array_has_all`.
- `:fm`: case-sensitive substring filters with `contains`, including parts of words.
- `:ivf_flat`: partitioned vector search without product quantization.
- `:ivf_pq`: partitioned vector search with product quantization.
- `:ivf_sq`: partitioned vector search with 8-bit scalar quantization.
- `:ivf_rq`: partitioned vector search with RabitQ quantization.
- `:hnsw_flat`: HNSW graphs within IVF partitions, storing unquantized vectors.
- `:hnsw_pq`: HNSW graphs within IVF partitions, with product quantization.
- `:hnsw_sq`: HNSW graphs within IVF partitions, with 8-bit scalar quantization.
- `:fts`: tokenized full-text search on string columns.

Choose `:label_list` and `:fm` explicitly. `:auto` does not select them.
LabelList supports scalar list elements such as strings and integers. FM
supports the package's string columns. It does not tokenize or normalize
text. Both types support naming, replacement, statistics, prewarming,
optimization, and deletion APIs.

```elixir
# A table with tags: {:list, :string} and text: :string:
{:ok, :created} = LanceDB.Index.create(table, :tags, type: :label_list)
{:ok, rows} = LanceDB.Query.query(table, filter: "array_has_any(tags, ['elixir', 'rust'])")
{:ok, rows} = LanceDB.Query.query(table, filter: "array_has_all(tags, ['elixir', 'rust'])")

{:ok, :created} = LanceDB.Index.create(table, :text, type: :fm)
{:ok, rows} = LanceDB.Query.query(table, filter: "contains(text, 'ixir')")
```

LanceDB checks scalar type compatibility and vector training requirements.
All seven vector index types accept these options:

| Option | Meaning / default |
| --- | --- |
| `:distance` | `:l2` (default), `:cosine`, or `:dot` for floating vectors; `:hamming` for uint8 vectors. Match the search metric. |
| `:num_partitions` | Explicit number of IVF partitions. |
| `:target_partition_size` | Target rows per partition. The SDK chooses the partition count. Cannot be combined with `:num_partitions`. |
| `:sample_rate` | IVF training sample multiplier per partition. The default is 256. The SDK also uses it for SQ training. |
| `:max_iterations` | Maximum IVF k-means iterations. The default is 50. In this SDK, it also controls PQ training for `:ivf_pq`. |

To use SDK partition sizing, do not set either partition option. All counts
must be positive uint32 integers. Vector indexes use training rows. Add data
to an empty table before you create a vector index. Indexed search can be
approximate.

PQ types (`:ivf_pq`, `:hnsw_pq`) accept `:num_sub_vectors` and `:num_bits`. The bit count is **4 or 8**. The default is 8. The subvector count must
divide the vector dimension. It must be even for 4-bit PQ. If you do not set
this option, the SDK selects the subdivision.

Four-bit PQ needs at least 16 training vectors. Eight-bit PQ normally needs
at least 256. Larger tables can use more training data.

RabitQ (`:ivf_rq`) accepts `:num_bits` from **1 through 9**. The default is
1. Its vector dimension must be divisible by 8. SQ types always use 8 bits.
They do not accept `:num_bits`. HNSW flat stores unquantized vectors.

```elixir
# On a populated table whose vector dimension is divisible by 8:
{:ok, :created} = LanceDB.Index.create(table, :vector,
  type: :ivf_rq,
  num_bits: 4,
  target_partition_size: 1024,
  sample_rate: 256,
  max_iterations: 50
)

{:ok, matches} = LanceDB.Query.search(table, query_vector,
  approx_mode: :accurate, refine_factor: 5, limit: 10
)
```

`:approx_mode` accepts `:fast`, `:normal`, `:accurate`, or nil. Omission
or nil uses the SDK's normal mode. This option controls the tradeoff between
speed and recall for RabitQ. In Lance 10, fast mode also enables ACORN
traversal for prefiltered HNSW searches. Other index types ignore it.
Refinement is a separate option.

None of these modes guarantees exact nearest neighbors. Use
`bypass_vector_index: true` for an exhaustive comparison.

This query option supports single-vector, batch, multivector, streaming,
explain, analysis, and output-schema requests. It also supports the vector
channel of hybrid search. It does not apply to scans or full-text-only
queries. Training options apply only to `Index.create/3` and
`Index.create_async/3`.

Creation waits for the embedded SDK to finish. By default, `replace: false`
protects existing indexes. To rebuild an index, use the same name and
`replace:
true`. `Index.list/1` returns `Index.Description` structs. Each
struct has `:name`, `:index_type`, `:columns`, `:index_uuid`, and
`:size_bytes` fields. It also has `:num_indexed_rows` and
`:num_unindexed_rows` fields.

Unavailable values can be `nil`. `Index.stats/2` returns a map with string
keys. These include `"index_type"`, `"num_indexed_rows"`,
`"num_unindexed_rows"`, `"distance_type"`, and `"num_indices"`. For a
missing index, `Index.stats/2` returns `{:ok, nil}`.

New writes can leave unindexed rows. Queries still include these rows.
Rebuilding an index adds them to the index. Dropping an index stops its use
in queries. Physical files can stay until cleanup. Use the maintenance
operations below to manage index coverage and reclaim storage.

### HNSW indexes and search tuning

`:hnsw_flat`, `:hnsw_pq`, and `:hnsw_sq` build HNSW graphs within IVF
partitions. Flat stores original vectors. PQ uses product quantization. SQ
uses 8-bit scalar quantization.

```elixir
{:ok, :created} = LanceDB.Index.create(table, :vector,
  type: :hnsw_pq,
  name: "hnsw_vectors",
  distance: :l2,
  num_partitions: 4,
  num_sub_vectors: 1,
  m: 20,
  ef_construction: 300
)

{:ok, results} = LanceDB.Query.search(table, query_vector,
  limit: 10, nprobes: 4, ef: 100, refine_factor: 5
)
```

For `:hnsw_sq` and `:hnsw_flat`, do not set `num_sub_vectors`. Supply
training data for all HNSW types. PQ normally needs at least 256 training
vectors. Its subvector count must divide the vector dimension. Larger
datasets can need more partitions. Use the same distance metric when you
build and search an index.

`m` controls graph neighbors. Its SDK default is 20. `ef_construction`
controls build candidates. Its SDK default is 300. Larger settings increase
build work and can improve recall. Omit construction options to use SDK
defaults.

At query time, `ef` controls HNSW candidates. Larger values can improve
recall and increase latency. The SDK default is approximately 1.5 times the
result limit. Set `ef_construction` at least as large as the `ef` you plan
to use. `nprobes` separately controls how many IVF partitions the query
searches. `ef` also applies to the vector channel of `hybrid_search/4`.

It has no effect without an HNSW index or with `bypass_vector_index: true`.
Construction counts and `ef` must be positive uint32 integers.

Index statistics include a string-keyed `"loss"` field. This value is the
sum of IVF training losses across index segments. It is `nil` for non-vector
indexes or when any segment lacks loss metadata. This statistic measures
training loss. It does not measure search recall.

## Index readiness waiting

```elixir
{:ok, job} = LanceDB.Index.create_async(table, :id, type: :btree, name: "ids")
{:ok, :ready} = LanceDB.Index.wait(table, "ids", timeout_ms: 30_000)
{:ok, :ready} = LanceDB.Index.wait(table, ["ids", "text_idx"],
  timeout_ms: 60_000, poll_interval_ms: 500
)
```

`Index.wait/3` observes each name until the index exists with zero unindexed
rows. The default timeout is 60 seconds. The maximum is two hours. The
default polling interval is one second. A timeout returns
`Error.kind == :timeout` with `details.pending_indexes`. Other inspection
errors, including admission saturation, return immediately.

The caller waits in Elixir and holds no native read slot between polls. One
operation telemetry span covers the complete wait.

Waiting does not build or optimize an index. A timeout does not cancel jobs.
After appending rows, run `Maintenance.optimize_indices/2` when necessary.
Each named index must be observed ready once. Later writes or replacement
can invalidate that observation. The handle's read-consistency configuration
controls visibility of changes from other handles.

Use `Job.wait/2` to wait for a particular creation job.

## Index prewarming

Load an existing index into the cache before the first search:

```elixir
{:ok, :prewarmed} = LanceDB.Index.prewarm(table, "my_vector_index")
```

The function accepts an index name as a string or atom. It waits for
prewarming to finish. It supports scalar, full-text, and vector indexes,
including indexes on a checked-out snapshot. Missing indexes return an
error. Prewarming does not build an index or add unindexed rows. Cached
pages can later be evicted.

## Typed vectors and native index jobs

Use an explicit schema to select vector storage. `{:vector, 384}` uses
float32. `{:vector, 384, :float16}` and `{:vector, 384, :float64}` select
other floating-point representations. Binary vectors use
`{:vector, 48, :uint8}` for 48 bytes (384 bits). Values are lists of byte
integers. Use `distance: :hamming` for both the index and search.

Dimensions for uint8 vectors count bytes. Batch search supports these
representations. Use float32 and cosine distance for document multivector
search.

```elixir
schema = LanceDB.Schema.new(id: :int64, vector: {:vector, 2, :float64})
{:ok, table} = LanceDB.Connection.create_table(db, "typed_vectors", [
  %{id: 1, vector: [1.00000001, 0.0]},
  %{id: 2, vector: [1.00000002, 0.0]}
], schema: schema)

{:ok, job} = LanceDB.Index.create_async(table, :vector,
  type: :ivf_flat, num_partitions: 1)
{:ok, status} = LanceDB.Job.status(job)
{:ok, :created} = LanceDB.Job.wait(job, timeout_ms: 30_000)
```

You can share jobs between processes. They continue after caller exit or
garbage collection. They keep their completion result for repeated waits. A
wait timeout leaves the job running. `Job.cancel/1` requests cancellation.
`Job.wait/2` reports the terminal outcome.

`Job.wait/2` accepts `timeout_ms: nil` (the default, wait indefinitely), or a
non-negative uint32 millisecond count. Zero checks without waiting. Pending
waits run on Tokio and reply by message, retaining no BEAM scheduler. These
wait deadlines differ from `Index.wait/3`, which polls table index readiness.

Cancellation cannot undo an already completed index. The write admission
slot stays held until the native job finishes. Explicit scalar index types
also accept `train: false` for an initially empty index.
