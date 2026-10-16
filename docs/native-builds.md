# Native builds

[Back to README](../README.md#documentation).

Run all commands from the package directory.

## Explicit maintainer source builds

To build from source, set `LANCEDB_BUILD=true` before you get dependencies.
Mix then includes the Rustler compiler. Install Rust through rustup,
a C/C++ build toolchain, and `protoc`. `rust-toolchain.toml` pins Rust and Cargo
to 1.99.0; CI and native artifact builds install the same version.
Rustler and RustlerPrecompiled are dependencies for compilation. An application
release does not start them.
The first source build can take a long time and use much disk space.

Use the committed Cargo lockfile and the SDK version `=0.40.0`.

```sh
export LANCEDB_BUILD=true
mix deps.get
cargo build --locked --release -p lancedb_nif
mix compile --force --warnings-as-errors
mix test --cover
```

Set `LANCEDB_BUILD=true` to enable a source build. A global Rustler
force-build setting does not enable source builds for this package. Normal
installations reject incorrect build IDs, development versions, and source
revision mismatches before Rustler can compile Rust.

Source compilation runs `cargo metadata --locked` before Rustler.
Compilation fails if the lockfile does not match the dependencies. It does
not change the lockfile. Changes to the workspace `Cargo.toml` or
`Cargo.lock` cause a new native compilation. If you change dependencies,
update the lockfile and commit it with the source changes.

## Building precompiled artifacts

Commit and push the native source and `Cargo.lock` before you start the
workflow. Use a new source revision for every new build. This rule also
applies when you change the toolchain or workflow. Do not reuse a build ID
or replace release assets. You do not need a public package release or a new
Elixir package version.

Run the [Build precompiled NIFs](https://github.com/preciz/lancedb_ex/actions/workflows/precompile.yml) workflow with the full source SHA:

```sh
native_source=$(git rev-parse HEAD)
gh workflow run precompile.yml --ref main -f source_revision="$native_source"
gh run list --workflow precompile.yml
```

The workflow checks out that source revision separately from the workflow
code. It builds with `cargo build --locked` and runs tests with
`cargo test --locked`. It checks Rustler Cargo features for NIF ABI 2.15.
It also checks that the build did not change `Cargo.lock`. All three targets
must pass the package tests with their packaged NIF before publication.
Linux x86_64 uses Ubuntu 22.04.

Linux ARM64 uses Ubuntu 24.04.

Apple Silicon uses `macos-15` and builds natively
with `MACOSX_DEPLOYMENT_TARGET=15.0`, requiring macOS 15 or later. Install
`protoc` with Homebrew on macOS. Cargo produces `liblancedb_nif.dylib`; the
packager renames it to the `.so` archive member expected by BEAM and
RustlerPrecompiled on macOS. All platforms use `.so.tar.gz` archive names.

The publication job checks all archives and combines their trusted
checksums. It publishes the archives to the public GitHub release
`native-<full-source-revision>`. The release also contains these files:

- `native-build.exs` and the checksum manifest.
- Cargo feature reports for each target.
- `provenance.txt` with the native source revision, workflow revision, SDK version, NIF ABI, and workflow run.

The initial check and the publication job reject an existing build tag. If
publication fails, find the cause before you try again. Never replace
release assets or publish a different archive with the same build ID.

After publication, the workflow checks a fresh consumer on each supported
target. Each consumer starts with an empty NIF cache and no URL overrides.
Its `PATH` contains neither GitHub CLI nor Rust tools. The consumer has no
GitHub credentials. It downloads from the default URL, checks the checksum, and
loads the NIF. It then connects with initial cache budgets and a deadline,
writes a small table, reopens it with a deadline, inspects cache statistics,
and executes a vector query. It creates two FTS indexes and starts an
application supervisor with a named `TableCache` child. Background warming
must complete for both indexes within ten seconds after fetch; failures stop
the check immediately. Repeated fetching must return the retained handle, and
the first multi-column FTS query must reuse its warmed session cache, verified
by resident entries and increased index hits. The supervisor is stopped before
the temporary database is removed.

The checks include unsupported targets, unavailable downloads, incorrect
checksums, and missing manifests. These failures must not start Rust. The
workflow also builds a production release and repeats the same API checks.
That release runs without `gh`, GitHub credentials, Rust, or Cargo.

Use verification mode to repeat consumer checks for a published build. This
mode downloads the existing trusted manifests and archives. It does not
build, publish, or replace archives:

```sh
gh workflow run precompile.yml --ref main -f source_revision="$native_source" -f verify_only=true
```

Wait for all workflow jobs, including consumer checks, to succeed. If GitHub
cannot start the jobs, resolve the reported GitHub Actions error first.
Download the trusted bundle from the successful publication run. Set
`native_run` below to that run ID. Update the build status in the README and
[Current native build](installation.md#current-native-build) before you commit the selected build.
Run the consumer checks.

Then commit both matching manifests and the updated documentation together:

```sh
native_run="<successful-publication-run-id>"
gh run view "$native_run"
gh run download "$native_run" --name lancedb-precompiled --dir /tmp/lancedb-native-bundle
cp /tmp/lancedb-native-bundle/native-build.exs .
cp /tmp/lancedb-native-bundle/checksum-Elixir.LanceDB.Native.exs .
scripts/validate_consumer.sh
git add native-build.exs checksum-Elixir.LanceDB.Native.exs README.md docs/installation.md
git commit -m "build: select verified immutable native build"
```

Commit the build ID, source revision, SDK version, tag, and default URL in
`native-build.exs`. Commit the matching trusted checksum manifest in the
same change. Consumers must use a checkout that contains both manifests. The
local [validation script](https://github.com/preciz/lancedb_ex/blob/main/scripts/validate_consumer.sh) repeats the anonymous consumer checks.
Its consumer environment has no GitHub CLI or Rust toolchain. Verification
mode does not create a new `lancedb-precompiled` bundle.

For that mode, use the trusted manifests from the existing release.

For a maintainer-only local archive, use a clean, committed source revision:

```sh
cargo build --locked --release -p lancedb_nif
native_version="0.0.0+$(git rev-parse HEAD)"
elixir scripts/package_nif.exs target/release/liblancedb_nif.so x86_64-unknown-linux-gnu "$native_version" /tmp/lancedb-local-artifacts
elixir scripts/checksums.exs /tmp/lancedb-local-artifacts /tmp/lancedb-local-checksums.exs
```

On macOS, pass `target/release/liblancedb_nif.dylib` and the matching
`aarch64-apple-darwin` target. A local archive does
not substitute for the three-target workflow or its trusted combined
manifest. The local packager refuses to replace an existing archive or
checksum file. Build distributed binaries on their intended minimum OS version.

## Preparing a package release

Use the public repository `preciz/lancedb_ex`. First publish and verify the
Linux and macOS native archives for all three targets. Commit their matching
build and checksum manifests.
Then update the release status and installation examples in the README and
installation guide. Remove the changelog's unreleased marker and pending-build
note only after the new native build passes verification.

Commit the release candidate changes and require a clean working tree.
The selected native build must include every native change in the package.
Before publication, compare the selected source to the committed release
candidate. This command must produce no differences:

```sh
elixir scripts/check_native_build.exs
```

The separate `native-artifacts.yml` workflow runs this check with full Git
history before its anonymous consumer and production-release checks on all three
Linux and macOS targets. It runs on build/checksum manifest changes, `v*` tag
pushes, or manual dispatch. Ordinary source CI builds and tests the current Rust sources
independently, so development can proceed while new artifacts are being prepared.
The check compares committed inputs with `HEAD`; commit local changes before
running it. Elixir-only and documentation changes do not require a native rebuild.

If it reports changes, build and verify a new immutable native release before
selecting its manifests. Passing tests with `LANCEDB_BUILD=true` does not
validate previously published binaries. Run the full Elixir suite with the
selected binaries in a fresh build directory as well:

```sh
env -u LANCEDB_BUILD MIX_BUILD_PATH=_build/precompiled-release mix test --cover --warnings-as-errors
```

Check the package and its documentation before publication:

```sh
export LANCEDB_BUILD=true
mix deps.get
mix format --check-formatted
mix credo --strict
mix compile --warnings-as-errors
mix test --cover --warnings-as-errors
cargo fmt --all --check
cargo clippy --locked --release --all-targets -p lancedb_nif -- -D warnings
cargo test --locked --release -p lancedb_nif
scripts/test_s3.sh
mix docs --warnings-as-errors
env -u LANCEDB_BUILD mix hex.build --unpack --output /tmp/lancedb-hex-package
scripts/validate_consumer.sh /tmp/lancedb-hex-package
env -u LANCEDB_BUILD mix hex.publish --dry-run --yes
```

Inspect the unpacked package. It must contain the license, native source,
Cargo lockfile, public build URL, and matching trusted checksum manifest.
It must not contain local build outputs or credentials.
The consumer check above installs the actual unpacked Hex package, downloads
its selected binary anonymously, and verifies a production application release.
Review `CHANGELOG.md` and the generated documentation too.

After all checks pass, tag and push the package source, then run the release
script from that checkout:

```sh
git tag -a v0.5.0 -m "Release v0.5.0"
git push origin main v0.5.0
elixir scripts/release.exs
```

The script requires Elixir, Mix with Hex, Git, `curl`, and an authenticated
GitHub CLI (`gh auth login`). Install the development dependencies during the
checks above so Hex can generate the documentation. Hex authentication and any
password or OTP prompts remain interactive in your terminal.

The script requires a clean working tree and matching local and GitHub tags at
`HEAD`. It checks native freshness and requires both tag workflows, `ci.yml` and
`native-artifacts.yml`, to pass at that exact revision before publication. It
waits for running checks. If either tag workflow has not appeared yet, wait and
rerun the script.

It checks that every file in the built package matches the Git tag, including
rejecting ignored files accidentally selected for packaging. It publishes the
package and docs to Hex, fetches the published package, and compares every source
file with that local package.
It verifies the documentation's version and source links and runs the fresh
consumer and production-release checks against the published package. Finally,
it creates a stable GitHub Release with the version's changelog, links to Hex and
HexDocs, and breaking changes. It marks the release latest only if Hex identifies
that version as its latest stable release.

Rerun the same command after an interrupted release. It verifies an existing Hex
package instead of replacing it, publishes missing docs separately, and leaves
an existing GitHub Release intact. Source mismatches, failed CI, retired Hex
versions, stale docs, and draft or prerelease GitHub entries stop the script.
It reports success only after Hex, HexDocs, consumer checks, and the public
GitHub Release are confirmed.

To verify an already completed release without publishing anything:

```sh
elixir scripts/release.exs --verify-only
```

This still builds and downloads packages and runs consumer checks in temporary
directories. It fails if any publication is missing or inconsistent.

Require a clean working tree before tagging. The version tag must identify
the final package commit, including the selected native manifests. If that
tag already exists, check its local and remote revisions before continuing.
Never move a tag for a published Hex version or publish a different commit
under the same version. Use a new package version for changes to a published
release.

The package tag identifies the Elixir release. The separate native tag
identifies the compiled wrapper. Building and publishing a native archive
does not require a Hex release or an Elixir package version change.

A published Hex version contains its selected build and trusted checksums.
To change the default for Hex users, publish a new package version with the
new matching manifests. Git checkouts can select a new native build while
the Elixir package version remains unchanged.

Clear `LANCEDB_BUILD` when you build or publish the Hex package. This keeps
Rustler optional in the published dependency metadata. Source tests and
documentation generation can use the explicit maintainer build flag.
