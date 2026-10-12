# Changelog

## Unreleased

- Add opt-in, application-supervised `LanceDB.TableCache`: shared table opens,
  retained sessions, bounded background multi-index warming, LRU/idle eviction,
  explicit invalidation, status and telemetry. Track table version and index UUID
  so changed indexes can be warmed again; discard stale task results.
- Move blocking table/index inspection, index prewarming and deletion, and table
  listing/deletion to dirty I/O schedulers so storage waits leave dirty CPU
  schedulers available. Requires a new native build for precompiled installations.
- Normalize native metrics recorder installation failures to `LanceDB.Error`,
  matching `Telemetry.enable_native_metrics/0`'s documented return type.
- Add typespecs for every public library function, checked by a regression test.
- Reject stale selected native builds in CI before compilation, requiring matching
  immutable build and checksum manifests for committed native changes.
- Extend anonymous package and production-release checks with supervised caching,
  background warming of two FTS indexes, and query reuse of warmed caches.
- Remove the obsolete pre-release migration guide.

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
