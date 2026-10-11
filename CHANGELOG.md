# Changelog

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
