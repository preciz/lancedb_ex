# Changelog

## Unreleased

- Add Apple Silicon and Intel macOS targets to native builds and anonymous
  production consumer validation. Require macOS 15 or later; public archives
  are pending a new verified four-platform native build.
- Upgrade source builds to LanceDB SDK 0.40.0 and Lance 13.0.0. Enable the SDK's
  `remote` feature to work around its upstream job compilation bug.
- Support nested struct field casts, Unicode fuzzy prefixes, and JSON fuzzy
  values while preserving exact JSON paths/types, removing the
  corresponding Lance 10 compatibility guards. New tables default to file format
  2.2; blob tables require explicit stable row IDs when IDs must survive compaction.
- Preserve tokenizer errors with the SDK's fallible collector for both explicit
  and saved analyzers. Query analysis now reports malformed JSON query input
  through SDK errors instead of tokenizer panics.
- Token positions start at the first retained term. N-gram analysis now disables
  stemming and stop-word removal by default, retaining explicit filter overrides.
- Phrase queries scan unindexed current rows unless `fast_search: true` is used.
  Blob reads at EOF retain positions beyond EOF instead of clamping them to the file size.
- Permit null parents with required struct children and scalar updates on blob
  tables. Continue rejecting direct blob-column updates before execution.
- Offset retrieval retains duplicate occurrences. Multivector cosine distance
  sums the closest-document cosine distance for each query vector.
- Preserve typed float16/float64 batch searches through independent query plans
  to avoid the SDK's float32 downcast panic. Float32 batches use its shared scan.
- Separate source CI from native artifact validation. Verify selected binaries
  on manifest updates and release tags, and require both tag workflows to pass
  before publishing a package.
- Give unnamed table caches distinct supervisor child IDs.
- Count pending requests separately from finished warming records, retain bounded
  status history, and evaluate admission after prospective eviction without
  discarding entries on rejection.
- Document that each stream cursor belongs to the process that begins enumeration.
- Complete message-based native waits for storage reads and writes, query
  planning and counts, blobs, index operations, maintenance, and writer
  backpressure and cleanup. Reads cancel on caller death; writes retain admission
  until SDK settlement. Public synchronous return values remain unchanged.

## 0.2.0

- Add opt-in, application-supervised `LanceDB.TableCache`: shared table opens,
  retained sessions, bounded background multi-index warming, LRU/idle eviction,
  explicit invalidation, status and telemetry. Track table version and index UUID
  so changed indexes can be warmed again; discard stale task results.
- Move blocking table/index inspection, index prewarming and deletion, and table
  listing/deletion to dirty I/O schedulers so storage waits leave dirty CPU
  schedulers available.
- Normalize native metrics recorder installation failures to `LanceDB.Error`,
  matching `Telemetry.enable_native_metrics/0`'s documented return type.
- Add typespecs for every public library function, checked by a regression test.
- Reject stale selected native builds in CI before compilation, requiring matching
  immutable build and checksum manifests for committed native changes.
- Extend anonymous package and production-release checks with supervised caching,
  background warming of two FTS indexes, and query reuse of warmed caches.
- Remove the obsolete pre-release migration guide.

This release selects new verified native binaries for Linux x86_64 and ARM64.
LanceDB SDK 0.37.1 and platform requirements are unchanged.

### Breaking changes

- Remove redundant `t/0` aliases from `LanceDB.Branch`, `LanceDB.History`,
  `LanceDB.Maintenance`, `LanceDB.Metadata`, and `LanceDB.SchemaEvolution`.
  Use `t:LanceDB.Table.t/0` for table handles.

## 0.1.1

- Add `LanceDB.Table.cache_stats/1` for the handle's actual session: index and
  metadata cache usage, entries, hits, and misses, without a global recorder.
  Capacity is unavailable in the pinned SDK and is reported as `nil`.
- Add optional `:timeout_ms` to connect and table opening. Native work runs on
  Tokio, yields BEAM schedulers, and releases read admission on cooperative
  timeout or caller death. One deadline covers the entire native operation.
- Accept `:index_cache_size_bytes` and `:metadata_cache_size_bytes` during
  initial connection setup, avoiding the extra SDK connection needed by
  per-open budgets. Separate connections have independent sessions; tables
  using one connection share its budgets. Existing per-open isolation remains.
- Document actual session sharing, cancellation limits, and reproducible
  local/MinIO connect and open benchmarks.

Requires the new native build selected in this release. Normal installation
downloads its verified Linux x86_64 or ARM64 binary without compiling Rust.
LanceDB SDK 0.37.1 and platform requirements are unchanged.

## 0.1.0

Initial public release of LanceDB for Elixir, using LanceDB Rust SDK 0.37.1.

- Local and S3-compatible storage, with explicit schemas and nested data types.
- Vector, batch vector, multivector, full-text, and hybrid queries, plus filters,
  projections, streaming, and direct row retrieval.
- Scalar, vector, and full-text indexes; streaming writes, updates, merges, and
  schema evolution.
- Table history, tags, branches, clones, metadata, blobs, and maintenance.
- Structured errors, execution deadlines, telemetry, and asynchronous native jobs.
- Precompiled Linux x86_64 and ARM64 NIFs with immutable build IDs and verified
  checksums. Normal installation does not require Rust.

Requires Elixir 1.20 or later in the 1.x series. Linux x86_64 requires glibc
2.35 or later; Linux ARM64 requires glibc 2.39 or later.
