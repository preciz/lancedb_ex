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
| `FTS`, `FTS.Prepared`, and `FTS.Preparation` | Build ordinary query clauses, retain analyzed/source term associations, and validate explicit or saved preparation requests. |
| Native `fts/preparation.rs` | Pair simple-analyzer output with source byte offsets and stop early on distinct-term limits. Reject unsupported analyzers instead of guessing token alignment. |
| `Creation`, `Write`, `SchemaEvolution`, and `Ingestion` | Share source consumption, row and batch transport, and writer cleanup. |
| Native `batch/validation` and `ingestion/worker` | Compile value checks, validate destination fields and reachable Arrow values, and maintain one bounded producer/commit lifecycle. |
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

The native writer compiles a validation plan once from its destination schema.
All producer formats pass through that writer, which checks field compatibility
and validates reachable Arrow values before sending a batch to the SDK. Scalar
checks operate on physical buffers; recursive checks respect slices and null
parents. Calendar and time bounds share the row-normalization rules. Map-key
canonicalization is separate from recursive traversal. IPC import remains a
structural check, allowing inspection and export of external Arrow data.
Row conversion uses the same recursive traversal to guard JSONB payloads before
the SDK decoder reads them, including IPC imports and existing datasets.

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

Native dispatch functions with only resource handles and simple controls run on
normal schedulers. Connect/open JSON decoding, row and batch codecs, and bulk
term decoding such as schema metadata maps use dirty CPU schedulers.

Write dispatch supplies validation/not-started metadata for unmarked preparation
failures. Mutation calls and fallible processing after mutation explicitly mark
execution/unknown outcomes, which dispatch preserves. All table mutations check
snapshot mutability before planning or invoking a mutation.

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

These adapters support LanceDB 0.40.0 and Lance 13. Review them when you
upgrade the SDK. Remove an adapter only after its regression tests pass
without it. Paths in this table are relative to `native/lancedb_nif/src/`,
except for Elixir builders.

| Adapter | SDK limitation and tests | Condition for removal |
| --- | --- | --- |
| `compat/storage.rs` | Custom vector item properties are lost. Escaped names can change field identity in SQL literal casts. Native schema and value tests, plus schema, evolution, and contract tests cover these cases. | The SDK keeps field properties and converts the selected types safely. |
| `compat/table.rs` | Merge and tag changes can bypass snapshot write protection. History, tag, and branch tests cover this guard. | Every SDK write operation rejects historical handles. |
| `compat/update.rs` | Direct updates of blob columns or collections containing blobs are unsupported. Scalar updates preserve blob payloads. Storage contract tests check rejection before execution and unchanged data and version. | SDK updates accept logical blob write values. |
| `compat/fts.rs` and `analyzer.rs` | JSON indexing can panic on invalid input. SDK token inspection discards deferred tokenizer errors; use its fallible collector with explicit or saved settings. Advanced FTS and tokenizer tests check invalid input, persisted analyzers, and selector errors. | The SDK returns typed errors for invalid input without a panic or silent empty tokens. |
| `compat/ipc.rs` | Arrow IPC schema conversion can panic on malformed metadata. IPC safety tests check missing schema fields, invalid integer widths, and panics during iteration after a valid batch. Import returns an invalid-data error for the entire stream. | Arrow returns typed errors for malformed IPC metadata without a panic. |
| `compat/json.rs` | Lance's JSONB decoder assumes valid UTF-8 and numeric layouts and decodes physical child arrays beneath null parents or outside list/map offsets. Validate borrowed extension payloads before writes and mask unreachable child slots before SDK conversion. Arrow validation tests cover malformed imported payloads. | The SDK supplies a safe, fallible JSONB validator and respects parent validity and offsets during conversion. |
| `compat/statistics.rs` | Typed statistics do not include IVF training loss for individual segments. Native tests check complete, missing, and non-finite values. | Typed SDK statistics supply the same complete loss value. |
| `hybrid.rs` | Hybrid fusion can produce an incorrect schema when only text search returns candidates. Ranking and FTS tests check empty candidate sets. | Native hybrid queries keep matching projections for all empty candidate sets. |
| `projection.rs` | Ordering can need columns that projection hides. Constant expressions also need physical input. Projection and ranking tests cover these cases. | The SDK planner handles these projections directly. |
| `diagnostics.rs` | Query analysis can ignore result stream errors. Diagnostics tests check execution failures. | SDK analysis returns all result stream failures. |
| `fts.rs` and Elixir FTS builders | Multi-match serialization does not include the operator. Default column selection misses negative leaves. FTS and tokenizer tests cover these cases. | SDK queries keep operators and select every leaf safely. |
| `fts/preparation.rs` and Elixir prepared matches | Positive fuzzy distances use basic tokenization instead of index normalization. Retain original terms for exact clauses and analyzed terms for fuzzy alternatives. Prepared FTS tests cover case, accents, stemming, alignment, budgets, and Unicode policies. | Explicit fuzzy execution uses the saved analyzer consistently; the optional preparation API still provides reusable term inspection and composition. |
| `value.rs` | Nested literals need array functions and explicit child casts. Struct literals cannot target required children. Nested update tests check exact values and unsupported errors. | Native literal updates keep types, names, nullability, and timestamps directly. |
| `cloning.rs` | The SDK loses storage overrides when opening a clone, and Lance's `Dataset::shallow_clone` commits with the source dataset's object store instead of resolving the destination store. Native, branch, and S3 clone tests check cross-bucket target stores and saved connection options. | SDK cloning commits to the target store and keeps the connection configuration. |
| `creation.rs` and `table_open.rs` | Local SDK connections discard inherited storage options for remote locations. S3 tests cover creation formats, empty tables, and overrides. | Local connections keep inherited options for all location overrides. |
| `query/vector_input.rs` | SDK builders can convert vectors to float32. Document multivector search supports only float32. Vector tests check float64 precision and Hamming input. | SDK builders keep vector types, including multivector types. |
| `compat/vector_query.rs` | SDK 0.40 batch planning assumes float32 queries and panics for typed float16/float64 input. Plan these batches as independent typed searches; keep shared scans for float32. Vector tests cover precision, offsets, streams and diagnostics. | SDK batch planning supports each stored floating type without coercion or a panic. |
| `compat/ngram.rs` | A `contains` needle that produces no trigram can silently lose matches once an n-gram index exists. Use the equivalent null-preserving `strpos(...) > 0` for untokenizable literals and computed needles; retain useful literal index searches. SQL tokens preserve strings, quoted names and comments. N-gram tests cover Unicode, punctuation, computed needles, reads and filtered writes. | The SDK planner falls back to a full recheck for every needle that produces no usable tokens. |
| `compat/computed.rs` | SQL refresh zips evaluated and stored container arrays using exact child types. Nested metadata edits on computed containers or their inputs change those types and can make a later refresh fail. Reject those edits before committing; allow top-level user metadata. Computed-column tests cover struct expressions, identity projections, blocked edits, and subsequent input refreshes. | Refresh aligns evaluated child metadata with the stored type; allow the nested metadata edits and verify recomputation succeeds. |
| `ingestion/worker.rs` and bulk readers | Cancellation needs an input error and a wait for execution. Repeated input requests after EOF must still return EOF. Backfills must await source EOF. Native worker and ingestion tests cover these cases. | The SDK guarantees cancellation without separate commits and equivalent source termination. |

LanceDB 0.40.0's job implementation references `Error::Http` without guarding it
with the `remote` feature. The native crate enables that feature to compile the
published SDK; this does not add a supported Cloud API to the Elixir package.
Remove the feature after [upstream issue #4142](https://github.com/lancedb/lancedb/issues/4142)
is fixed and the embedded build passes without it.

FTS boost subtraction, phrase positions, and explicit fuzzy distance follow
SDK behavior. Blob query results stay descriptors. Callers must fetch
payload bytes explicitly, even if an SDK upgrade changes descriptor storage.
