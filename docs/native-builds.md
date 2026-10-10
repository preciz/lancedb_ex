# Native builds

[Back to README](../README.md#documentation).

Run all commands from the package directory.

## Explicit maintainer source builds

To build from source, set `LANCEDB_BUILD=true` before you get dependencies.
Mix then includes the Rustler compiler. Install Rust 1.94.1 or later, Cargo,
a C/C++ build toolchain, and `protoc`. Rustler and RustlerPrecompiled are
dependencies for compilation. An application release does not start them.
The first source build can take a long time and use much disk space.

Use the committed Cargo lockfile and the SDK version `=0.37.1`.

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
It also checks that the build did not change `Cargo.lock`. All four targets
must pass the package tests with their packaged NIF before publication.
Linux x86_64 uses Ubuntu 22.04.

Linux ARM64 uses Ubuntu 24.04. Both macOS targets use macOS 15 runners.

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
loads the NIF. It then writes and queries a small LanceDB table.

The checks include unsupported targets, unavailable downloads, incorrect
checksums, and missing manifests. These failures must not start Rust. The
workflow also builds a production release and repeats the write and query.
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

Use the matching target and `.dylib` input on macOS. A local archive does
not substitute for the four-target workflow or its trusted combined
manifest. Build distributed binaries on their intended minimum OS version.

## First public package release

Use the public repository `preciz/lancedb_ex`. First publish and verify the
four native archives. Commit their matching build and checksum manifests.
Then update the release status in the README and installation guide.

Check the package and its documentation before publication:

```sh
export LANCEDB_BUILD=true
mix deps.get
mix format --check-formatted
mix credo --strict
mix test --cover --warnings-as-errors
mix docs --warnings-as-errors
env -u LANCEDB_BUILD mix hex.build --unpack --output /tmp/lancedb-hex-package
scripts/validate_consumer.sh
```

Inspect the unpacked package. It must contain the license, native source,
Cargo lockfile, public build URL, and matching trusted checksum manifest.
It must not contain local build outputs or credentials.

After all checks pass, tag the package source and publish to Hex:

```sh
git tag v0.1.0
git push origin main v0.1.0
env -u LANCEDB_BUILD mix hex.publish
```

The package tag identifies the Elixir release. The separate native tag
identifies the compiled wrapper. Building and publishing a native archive
does not require a Hex release or an Elixir package version change.

A published Hex version contains its selected build and trusted checksums.
To change the default for Hex users, publish a new package version with the
new matching manifests. Git checkouts can select a new native build while
the Elixir package version remains `0.1.0`.

Clear `LANCEDB_BUILD` when you build or publish the Hex package. This keeps
Rustler optional in the published dependency metadata. Source tests and
documentation generation can use the explicit maintainer build flag.
