# Development and tests

[Back to README](../README.md#documentation).

Run all commands from the package directory.

## Development

Use rustup so `rust-toolchain.toml` selects Rust 1.99.0, rustfmt and Clippy.
Development source pins LanceDB 0.40.0 and Lance 13.0.0; Arrow remains on 58.
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

Tests tagged `:fts_preparation` run when the loaded NIF supports preparation.
The currently selected public build lacks that unreleased feature, so default
tests skip those checks. Source builds require it; packaged candidate builds
automatically run the checks when their NIF implements it. Consumer smoke
validation uses the same empty-analysis probe and keeps checking existing APIs
with older native builds. Unexpected preparation errors fail verification.

Tests tagged `:standard_optimization` follow the same capability gating. An invalid
resource probe checks registration before native admission or storage: Rustler
raises `:badarg` for the matching NIF, while an older stub raises `:nif_not_loaded`.
Source builds require support; unexpected errors fail verification. Consumer
validation exercises optimization on its temporary table when support is present.

Tests tagged `:native_value_validation` check stricter Arrow write validation that
the currently selected published NIF lacks. Source builds run these tests, and
the native packaging workflow explicitly includes them against every candidate
binary without compiling Rust in the consumer project. To verify a matching
precompiled build locally, run `mix test --include native_value_validation`.

Rust changes require source-build validation and a new immutable native build
before release. Passing tests with previously selected archives does not
validate unpublished native changes.

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
available for other work. Storage scheduler tests use one dirty CPU and one
dirty I/O scheduler. Held history, branch, table and index metadata, prewarming,
listing, deletion, counts, and query planning leave both Arrow conversion and
file I/O available. Read caller death releases admission before storage resumes;
write caller death retains admission until settlement. The table URI accessor
is checked to complete without a storage request.
Writer scheduler tests hold an upload and verify that finish and abort yield
both dirty scheduler pools while retaining write admission until SDK settlement.
Table-cache regressions gate background workers and S3 index reads to check
coalescing, bounded concurrency, caller cleanup, LRU/idle eviction, retry, and
invalidation during active work. They check index replacement and appended
segments, shared session counters, and fewer first-query S3 index reads after
warming than with an independent cold session.
Connect/open scheduler tests hold both S3 reads and local SDK region-resolution
requests. They check deadlines and caller death with one dirty CPU and one
dirty I/O scheduler, including admission release before storage is released.
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
all native targets, locked Cargo tests and the full Elixir suite with coverage.
Source checks and the dependent MinIO job build the current native sources with
`LANCEDB_BUILD=true`, independently of the selected precompiled build.
Both jobs force the package's Mix compilation after cache restoration. This
runs Rustler's copy step to recreate `priv/native` even when cached BEAM files
are already current; Cargo keeps using its incremental build cache. The MinIO
job compiles in `MIX_ENV=test` before starting the integration tests.

The separate `native-artifacts.yml` workflow runs when build/checksum manifests
change, on `v*` tag pushes, or by manual dispatch. It checks source freshness,
then verifies anonymous downloads and production releases on Linux x86_64,
Linux ARM64, and Apple Silicon macOS. Native source, Cargo files, toolchain,
or precompile workflow changes require a new verified native build before release; development source CI can
run while those artifacts are being prepared. Elixir-only changes can retain
the selected build.

Run the same check locally with `elixir scripts/check_native_build.exs` after
committing changes; it compares the selected revision with `HEAD` and requires
that revision in local Git history. It does not download or verify archives.

CI also replays every property with seeds `0` and
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
two Linux platforms in the native-build matrix and caches the verified binary under
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

## Connection and open benchmark

The benchmark compares per-open cache budgets with configuring those budgets
on the initial connection. It reports connect and open separately, plus the
combined per-sample time, as JSON with median and p95 values in microseconds.
Run local storage alone, or reuse the disposable MinIO server for both:

```sh
MIX_ENV=test LANCEDB_BUILD=true mix run benchmarks/connection_open.exs
MIX_ENV=test LANCEDB_BUILD=true scripts/test_s3.sh mix run benchmarks/connection_open.exs
```

Set `LANCEDB_BENCH_ITERATIONS` to change the default 100 samples per case.
`--legacy-only` runs against a checkout before connection budgets were added.
Five untimed opens prime SDK initialization and module loading. Cold cases
create a fresh connection/session for each sample; they do not flush OS or
storage caches. Warm cases retain a connection after one FTS index prewarm.
The existing per-open budget path continues to create a new session on each
open, whereas the configured connection reuses its session. The workload has
100 rows, a text index, 112 MiB index and 16 MiB metadata budgets, and a zero
read-consistency interval. Queries and index prewarming are outside the timed
open samples. Setup time for each warm case is reported separately.

On 2026-10-11, Linux x86_64 / Ryzen 7 8845HS, Rust 1.99.0 release builds,
Elixir 1.20.4 / OTP 29, and localhost MinIO RELEASE.2025-09-07T16-13-09Z:

| Cold case | Median connect (ms) | Median open (ms) | Median combined (ms) |
| --- | ---: | ---: | ---: |
| Local, before patch | 0.112 | 0.161 | 0.276 |
| Local, existing path after patch | 0.106 | 0.155 | 0.263 |
| Local, configured connection | 0.103 | 0.062 | 0.167 |
| MinIO, before patch | 5.892 | 6.948 | 12.852 |
| MinIO, existing path after patch | 5.839 | 6.881 | 12.770 |
| MinIO, configured connection | 5.609 | 0.951 | 6.656 |

| Warm case | Before patch median open (ms) | Existing path median/p95 (ms) | Configured median/p95 (ms) |
| --- | ---: | ---: | ---: |
| Local | 0.169 | 0.166 / 0.238 | 0.021 / 0.030 |
| MinIO | 6.839 | 7.711 / 24.405 | 0.295 / 0.353 |

Combined medians are computed per sample, so they need not equal the sum of
the phase medians. The paired comparison shows about 48% lower cold combined
time and 96% lower warm open time on MinIO. This sequential microbenchmark
does not predict production S3 latency or FTS query performance; host load,
SDK cache reuse, and namespace setup affect its results. The before-patch
baseline was captured on commit `1f45a1f`; the comparison runs both paths in
the patched build. Recorded phase summaries are in
[`benchmarks/results/connection_open_2026-10-11.json`](https://github.com/preciz/lancedb_ex/blob/main/benchmarks/results/connection_open_2026-10-11.json).
