# Implementation notes

[Back to README](../README.md#documentation).

The Rust software development kit (SDK) supplies the database engine. The
native library supplies Natively Implemented Functions (NIFs) to the Erlang
virtual machine (BEAM).

## Internal responsibilities

Module names and local comments describe the implementation. Use these
boundaries when you change the code:

| Module or component | Responsibility |
| --- | --- |
| Public Elixir modules | Check API arguments and create one operation span. |
| `Options` | Share argument checks for names, versions, and metadata. |
| `Execution` | Convert failures to `Error`. Call the operation's decoder for write results. |
| `Backend` and its codecs | Extract resource handles, encode requests, and wrap native resources. |
| `Query.Definition`, `Controls`, and `Request` | Keep semantic controls, check combinations, and create an execution plan. |
| `Query.Executor` | Use the same Arrow cursor for immediate and streaming reads. Keep callbacks in BEAM. |
| `Creation`, `Write`, `SchemaEvolution`, and `Ingestion` | Share source consumption, row and batch transport, and writer cleanup. |
| `Schema`, `ArrowSchema`, and native `StorageSchema` | Separate logical definitions, physical output, and checks for stored fields. |
| `TableCache` | Coordinate shared requests, supervised tasks, caller monitors, replies, telemetry, and rejection of stale task results. |
| `TableCache.Entries` | Define entry reservation, request accounting, recency, eviction selection, and idle expiry without process effects. |
| `TableCache.Options` and `TableCache.Worker` | Validate fixed instance settings and perform connection, open, and warming work outside the coordinator. |
| Native `outcome.rs` and `wire.rs` | Define engine results and encode them for BEAM. |
| Native compatibility modules | Keep SDK workarounds separate from the public API. |

Public builders reject unknown options. Native code checks record values
against the schema. Native query request decoding can ignore unknown fields.
Keep public option checks in the builders.

Row input uses direct BEAM terms. Native decoding creates owned values
before background work starts. Small schema and query descriptions use JSON.
`Batch.to_rows/1` converts Arrow output through JSON. Native codec tests
check JSON round trips.

A batch slice can keep its parent buffers. The `batch_size` option limits
returned rows. It does not limit memory use. Arrow IPC import and export
allocate memory. Native batch transfer avoids row conversion.

Each snapshot needs a separate SDK table and dataset view. An `Arc` or SDK
table clone shares its view. The shared Tokio runtime executes native
operations. Direct APIs create no process for each table and use no
connection pool. An optional application-supervised `TableCache` owns one
coordinator and a linked task supervisor. Its bounded tasks perform storage
work; returned handles go directly to the ordinary query APIs.

Each cache entry has a fresh generation reference. Task results apply only to
the matching generation, so invalidation and eviction cannot be undone by a
late open or warmer. Active work remains counted until it settles, even if its
entry is discarded. Caller monitors and fetch timeouts remove waiters without
cancelling a shared open. Pending fetches protect their entries from eviction.

Warming pins an independent snapshot sharing the retained handle's session.
The completion key combines the table version and the SDK's first index segment
UUID. A first-segment UUID alone misses appended index segments; the SDK's
`index_version` describes its storage format. Rechecking the live version after
warming rejects observed concurrent changes. Detection follows the configured
read-consistency interval. No completion record promises cache residency.

One-shot reads use monitored Tokio tasks. Admission happens before spawning;
operations with deadlines retain one budget across preparation and execution.
Reference-tagged replies preserve the synchronous Elixir API without retaining
a BEAM scheduler. Timeout, caller death, and caught task panics drop the read
future and release its permit before replying. SDK tasks spawned separately can
outlive that future.

All SDK waits run outside BEAM schedulers, including metadata inspection, query
planning and counts, blobs, index construction, maintenance, and writes.
Dirty CPU schedulers decode input and perform synchronous codecs. Blob results
are encoded in an owned environment after I/O; message transfer shares their
binary storage. The SDK's synchronous merge reader bridges to its input stream
only on background blocking threads.

Read cancellation and write completion have different guarantees. Explicit
cursor close waits for cleanup and read slot release. A writer abort sends
an input error and waits for SDK execution. Dropping a write future can
leave SDK tasks running separately. One-shot writes continue after caller death;
their permit stays held until SDK execution settles. Stream preparation transfers
its permit to the writer, and asynchronous index creation transfers it to the job
watcher. Push, finish, abort, and job commands need no additional admission slot.
Cancellation cannot guarantee rollback after final commit starts.

Backfills keep the final batch until successful source EOF. The SDK can stop
reading after it receives the expected row count. Checking EOF prevents a
late input failure from being lost. Empty backfills also check EOF before
committing.

Contract tests check snapshot isolation, schema conversion, source formats,
permit release, errors, and telemetry. Update these tests when an operation
contract changes.

## SDK compatibility

These adapters support LanceDB 0.37.1 and Lance 10. Review them when you
upgrade the SDK. Remove an adapter only after its regression tests pass
without it. Paths in this table are relative to `native/lancedb_nif/src/`,
except for Elixir builders.

| Adapter | SDK limitation and tests | Condition for removal |
| --- | --- | --- |
| `compat/storage.rs` | Null structs with required descendants are unsupported. Custom vector item properties are lost. Nested casts and escaped names can change field identity. Native schema and value tests, plus schema, evolution, and contract tests cover these cases. | The SDK keeps field properties and converts the selected types safely. |
| `compat/table.rs` | Merge and tag changes can bypass snapshot write protection. History, tag, and branch tests cover this guard. | Every SDK write operation rejects historical handles. |
| `compat/update.rs` | Row updates fail on blob tables, including updates to other columns. Storage contract tests check rejection before execution and unchanged data and version. | SDK row updates keep blob payloads and schema. |
| `compat/fts.rs` | JSON tokenization and indexing can panic on invalid input. Advanced FTS tests check invalid query triples and synchronous and asynchronous index creation. | The SDK returns typed errors for invalid input without a panic. |
| `compat/ipc.rs` | Arrow IPC schema conversion can panic on malformed metadata. IPC safety tests check missing schema fields, invalid integer widths, and panics during iteration after a valid batch. Import returns an invalid-data error for the entire stream. | Arrow returns typed errors for malformed IPC metadata without a panic. |
| `compat/statistics.rs` | Typed statistics do not include IVF training loss for individual segments. Native tests check complete, missing, and non-finite values. | Typed SDK statistics supply the same complete loss value. |
| `hybrid.rs` | Hybrid fusion can produce an incorrect schema when only text search returns candidates. Ranking and FTS tests check empty candidate sets. | Native hybrid queries keep matching projections for all empty candidate sets. |
| `projection.rs` | Ordering can need columns that projection hides. Constant expressions also need physical input. Projection and ranking tests cover these cases. | The SDK planner handles these projections directly. |
| `diagnostics.rs` | Query analysis can ignore result stream errors. Diagnostics tests check execution failures. | SDK analysis returns all result stream failures. |
| `fts.rs` and Elixir FTS builders | Multi-match serialization does not include the operator. Default column selection misses negative leaves. Fuzzy prefixes can split Unicode characters. FTS and tokenizer tests cover these cases. | SDK queries keep operators and select every leaf safely. |
| `value.rs` | Nested literals need array functions and explicit child casts. Struct literals cannot target required children. Nested update tests check exact values and unsupported errors. | Native literal updates keep types, names, nullability, and timestamps directly. |
| `cloning.rs` | The SDK loses storage overrides when opening a clone. S3 clone tests check saved connection options. | SDK cloning keeps the connection configuration. |
| `creation.rs` and `table_open.rs` | Local SDK connections discard inherited storage options for remote locations. S3 tests cover creation formats, empty tables, and overrides. | Local connections keep inherited options for all location overrides. |
| `query/vector_input.rs` | SDK builders can convert vectors to float32. Document multivector search supports only float32. Vector tests check float64 precision and Hamming input. | SDK builders keep vector types, including multivector types. |
| `ingestion/worker.rs` and bulk readers | Cancellation needs an input error and a wait for execution. Repeated input requests after EOF must still return EOF. Backfills must await source EOF. Native worker and ingestion tests cover these cases. | The SDK guarantees cancellation without separate commits and equivalent source termination. |

FTS boost subtraction, phrase positions, and explicit fuzzy distance follow
SDK behavior. Blob query results stay descriptors. Callers must fetch
payload bytes explicitly, even if an SDK upgrade changes descriptor storage.
