# Installation and storage

[Back to README](../README.md#documentation).

## Requirements

- Elixir 1.20 or a later 1.x version. CI uses Elixir 1.20 and OTP 29.

The package downloads native binaries with `rustler_precompiled` 0.10. It
checks each archive against the committed checksum manifest. The binaries
use NIF ABI 2.15. The package supplies binaries for these two Linux targets:

| Target | Build platform |
| --- | --- |
| `x86_64-unknown-linux-gnu` | Ubuntu 22.04; glibc 2.35 or later |
| `aarch64-unknown-linux-gnu` | Ubuntu 24.04; glibc 2.39 or later |

Use the listed Linux glibc version or a later version. Normal installation
rejects other targets, including macOS, without compiling Rust.

Native archives use public releases in
[preciz/lancedb_ex](https://github.com/preciz/lancedb_ex). Downloads use the
release URL directly. GitHub CLI, credentials, Rust, and Cargo are not
necessary. Consumer validation runs without them.

Compilation fails if build configuration or checksums are missing. It also
fails for unsupported platforms, download errors, or
incorrect checksums. Normal installation does not create checksums or
compile Rust.

## Installation

Add the package to `mix.exs`:

```elixir
defp deps do
  [
    {:lancedb_ex, "~> 0.1.0"}
  ]
end
```

Get the dependencies and compile the application:

```sh
mix deps.get
mix compile
```

A URL override and a supervision tree entry are not necessary.
The Hex package and OTP application use the name `lancedb_ex`.
[native-build.exs](https://github.com/preciz/lancedb_ex/blob/main/native-build.exs) records the native build ID, exact source revision, and
default artifact URL. The Elixir package version is `0.1.0`. The package
pins the SDK version to `=0.37.1`. These versions do not identify the
wrapper binary. Each native build has the SemVer version
`0.0.0+<full-source-revision>`.

Its internal release tag is `native-<full-source-revision>`. Archive,
library, and cache filenames contain the full source revision. A new native
build does not need a new Elixir package version or a public package
release.

### Current native build

The selected native build is
[`0.0.0+c922f11b2a7465c8a96d7863479d21974f0e9405`](https://github.com/preciz/lancedb_ex/releases/tag/native-c922f11b2a7465c8a96d7863479d21974f0e9405).
Its exact source revision is
[`c922f11b2a7465c8a96d7863479d21974f0e9405`](https://github.com/preciz/lancedb_ex/commit/c922f11b2a7465c8a96d7863479d21974f0e9405).
It uses SDK `0.37.1` and NIF ABI `2.15`.

Both Linux targets passed their Rust and Elixir tests. The
[publication workflow](https://github.com/preciz/lancedb_ex/actions/runs/38132611907)
also checked fresh consumers on both targets. Each consumer downloaded the
archive from the public default URL, checked its checksum, loaded the NIF,
and completed a write and query without Rust or credentials.

To install from the public repository, use this Git dependency:

```elixir
{:lancedb_ex, github: "preciz/lancedb_ex", branch: "main"}
```

Then run `mix deps.get` and `mix compile`. This installation uses the
published native archives. It does not compile Rust.

See [Building precompiled artifacts](native-builds.md#building-precompiled-artifacts) for the publication procedure.

### Artifact URL overrides

To use an existing mirror, set its artifact URL in `config/config.exs`:

```elixir
config :lancedb_ex, :nif_base_url, "https://your-artifact-host/lancedb/native-<source-revision>/"
```

At compile time, `LANCEDB_NIF_BASE_URL` overrides the application setting. A
URL override changes the download location. Use `{url, headers}` as
`:nif_base_url` if your mirror uses authentication. The
mirror must supply the committed archives with their exact filenames. The
package still checks the committed checksums. After you change the build
mode or URL, run `mix deps.compile lancedb_ex --force`.

Do not create checksums during installation.

### Deployment and offline builds

Mix downloads the archive when it compiles the dependency. `mix release`
includes the verified library from `priv/native`. GitHub credentials, `gh`, Rust, and Cargo are not necessary when the release runs. Build the release
for its deployment platform. Use the platform versions in [Requirements](#requirements).

For an offline build, put the exact committed archive in a writable cache.
Set `RUSTLER_PRECOMPILED_GLOBAL_CACHE_PATH` to that directory before
compilation. The package checks the committed checksum before it extracts
the cached archive. The archive filename identifies its native build and
target. A different cache directory does not bypass checksum checks.

## S3 and MinIO

Pass storage options explicitly. The package supplies no credentials,
endpoint overrides, or application-specific environment defaults.

```elixir
{:ok, db} =
  LanceDB.connect("s3://my-bucket/vectors", %{
    "aws_region" => "us-east-1",
    "aws_access_key_id" => System.fetch_env!("AWS_ACCESS_KEY_ID"),
    "aws_secret_access_key" => System.fetch_env!("AWS_SECRET_ACCESS_KEY")
  })
```

For a local MinIO instance, also pass `"aws_endpoint"`,
`"allow_http" =>
"true"`, and
`"aws_virtual_hosted_style_request" => "false"`. The package changes
storage-option keys and values to strings.
