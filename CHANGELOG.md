# Changelog

## Unreleased

- Move blocking table/index inspection, index prewarming and deletion, and table
  listing/deletion to dirty I/O schedulers so storage waits leave dirty CPU
  schedulers available. Requires a new native build for precompiled installations.

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
