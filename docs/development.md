# Development and tests

[Back to README](../README.md#documentation).

Run all commands from the package directory.

## Development

Use rustup so `rust-toolchain.toml` selects Rust 1.99.0, rustfmt and Clippy.
Run these checks from the package directory. Set the source-build flag
before you get dependencies:

```sh
export LANCEDB_BUILD=true
mix deps.get
mix format --check-formatted
mix credo --strict
mix compile --warnings-as-errors
mix test --cover
cargo fmt --all --check
cargo clippy --locked --release --all-targets -p lancedb_nif -- -D warnings
cargo test --locked --release -p lancedb_nif
```

Each test directory has a specific purpose:

| Location | Purpose |
| --- | --- |
| `test/unit/` | Pure validation, recursive codecs, query composition, source ownership, archive integrity, and source builds. |
| `test/properties/` | Generated datasets and operation sequences checked against independent Elixir models using the real NIF. |
| `test/lancedb_*_test.exs` | Focused integration examples and feature-specific properties. |
| `test/support/` | Shared database lifecycle, bounded generators and query adapters. |
| `test/lancedb_s3_test.exs` | Optional object-storage integration, tagged `:s3`. |

Without `LANCEDB_BUILD=true`, tests use the published NIF. Set
`LANCEDB_BUILD=true` before dependency resolution to check changes to Rust.
The default suite does not need an S3 service or Phoenix application.
CI runs the S3 suite separately.

Integration tests check persistence, scalar and recursive schemas, searches,
indexes, writes, ingestion, and schema evolution. They also check history,
branches, metadata, blobs, maintenance, telemetry, resource ownership, and
failure contracts. `LanceDB.DataCase` allocates a unique path per test and
cleans it on exit. Tests that change process-global admission limits,
telemetry recorder state or environment variables run synchronously.

StreamData is a test-only dependency. Properties check recursive JSON and
typed values, exact integers and decimals, float32 rounding, schema
metadata, expression quoting, query composition and overflow limits.
Independent models check ordered pagination and nested sorting (including null ordering),
upsert and conditional merge sequences, metadata updates and branch
isolation. Vector properties verify both candidate completeness and
distances against arithmetic oracles.

Write-failure properties put incorrect rows, producer failures, and
finalizer failures at generated batch boundaries. They check stored rows,
versions, confirmed commits, and source cleanup. They also check the
difference between `commit: :once` and `commit: :batch`. If a finalizer
fails during early termination, the result includes
`Error.details.cleanup_error`. The result keeps the primary error and
confirmed progress.

Native safety tests reject excessively nested malformed records, including a
10,000-level input tested in a separate BEAM. Term decoding limits wire
nesting to 64 levels, including the outer row list and row map. Cursor and job
scheduler tests run a separate BEAM with one dirty I/O scheduler and hold storage
responses in a local read-only S3 fixture. Pending cursor reads, cleanup,
and native job waits use Tokio and reply by message. BEAM schedulers stay
available for other work. History and branch scheduler tests use one dirty CPU
scheduler and verify that held metadata I/O does not delay Arrow conversion.
Source-build regressions use tiny Cargo workspaces
and run only with `LANCEDB_BUILD=true`. Properties that change data create a fresh
database **inside** `check all` with `with_database/1`.

This helper removes the database after each generation and shrink attempt.
Only properties that read data share a fixture. Generators limit recursion
and dataset size so failures shrink quickly. They include empty values,
nulls, integer boundaries, Unicode, arbitrary bytes, and quoting characters.
When you correct a bug, add its useful counterexample as a regression test.

Use the StreamData `:property` tag to select **all** properties, including
properties in feature suites. A filename pattern can miss those properties.
To repeat a failure, use the seed that ExUnit prints:

```sh
mix test test/unit
mix test --only property
mix test --only property --seed 12345
mix test test/properties/lancedb_write_failure_test.exs --seed 12345
scripts/test_s3.sh
```

CI runs formatting, strict Elixir linting, Clippy with warnings denied for
all native targets, locked Cargo tests and the full Elixir
suite with coverage. It also replays every property with seeds `0` and
`12345`, in addition to the full suite's random seed. Elixir line coverage
must be at least 90%. Coverage excludes test support and the internal NIF module,
whose fallback stubs are replaced by Rust NIFs. Elixir coverage does not
measure Rust coverage.

See [Internal responsibilities](internals.md#internal-responsibilities) and [SDK compatibility](internals.md#sdk-compatibility) for development notes.

## S3 integration tests

GitHub CI has a separate MinIO integration job. It covers writes,
independent connections, vector, FTS, and hybrid search, version tags,
restore, cleanup, table deletion, and rejected credentials. Run the same job
locally with Elixir and curl. Curl must support AWS SigV4:

```sh
scripts/test_s3.sh
```

The script downloads the pinned MinIO release from its official GitHub
assets and verifies it against a committed SHA256 digest. It supports the
same two Linux platforms as the NIF and caches the verified binary under
`_build/minio`. The server binds to localhost on an ephemeral port, uses
temporary data and test credentials, and is stopped on exit. Failed runs
print its logs before cleanup. Docker, an AWS account, and GitHub secrets
are not necessary.

S3 tests are tagged `:s3`. Ordinary `mix test` runs exclude them. To use an
existing test endpoint and bucket, set these environment variables:

- `LANCEDB_TEST_S3_ENDPOINT`
- `LANCEDB_TEST_S3_URI` (an S3 bucket or prefix)
- `LANCEDB_TEST_S3_ACCESS_KEY`
- `LANCEDB_TEST_S3_SECRET_KEY`

Then run:

```sh
LANCEDB_BUILD=true mix test test/lancedb_s3_test.exs --include s3
```

Each test creates and removes tables under a unique prefix beneath the
supplied URI. These tests check S3 compatibility with MinIO. They do not
cover AWS IAM, STS, or AWS-specific service behavior.
