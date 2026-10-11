# LanceDB for Elixir

[![Tests](https://github.com/preciz/lancedb_ex/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/preciz/lancedb_ex/actions/workflows/ci.yml)

This package embeds [LanceDB](https://lancedb.com/) in Elixir through Rustler.
Use local files or S3-compatible object storage. A database server is not
necessary. Supply your own vector embeddings.

The published Elixir package version is `0.1.0`; this checkout prepares `0.1.1`.
The Rust software development kit (SDK) is pinned to `0.37.1`. Each native build
has a separate immutable ID. The [selected native build](docs/installation.md#current-native-build)
was verified for `0.1.0`. The new native binaries for `0.1.1` are pending;
use an [explicit source build](docs/native-builds.md#explicit-maintainer-source-builds)
to run this checkout. The guides describe the current source API.

## Requirements

- Elixir 1.20 or a later 1.x version. CI uses OTP 29.
- Linux x86_64 with glibc 2.35 or later, or Linux ARM64 with glibc 2.39 or later.

Native archives use public GitHub releases. GitHub CLI, credentials, Rust,
and Cargo are not necessary for normal installation.

## Installation

Add the package to `mix.exs`:

```elixir
defp deps do
  [
    {:lancedb_ex, "~> 0.1.0"}
  ]
end
```

Run these commands from your application directory:

```sh
mix deps.get
mix compile
```

Mix downloads the native archive and checks the committed checksum. The
package supplies a default artifact URL. A URL override and a supervision
tree entry are not necessary.

Normal installation does not compile Rust or create checksums. Missing
configuration, unsupported platforms, download failures, and incorrect
checksums stop compilation.

See the [installation guide](docs/installation.md) for URL overrides,
offline builds, application releases, and S3 storage options.

## Quick start

Use a new data directory for this example:

```elixir
alias LanceDB.{Connection, Query, Table}

{:ok, db} = LanceDB.connect("./data/vectors")

{:ok, table} =
  Connection.create_table(db, "documents", [
    %{id: "1", text: "Elixir and OTP", vector: [1.0, 0.0, 0.2]},
    %{id: "2", text: "Sourdough bread", vector: [0.0, 1.0, 0.8]}
  ])

{:ok, matches} =
  Query.search(table, [0.9, 0.1, 0.2], limit: 5, distance: :cosine)

IO.inspect(matches)

{:ok, %LanceDB.WriteResult{inserted: 1}} =
  Table.add(table, [
    %{id: "3", text: "Phoenix LiveView", vector: [0.9, 0.1, 0.3]}
  ])

{:ok, 3} = Table.count_rows(table)
```

Result rows have string keys. Vector search adds a numeric `"_distance"`
field. Operations return `{:ok, value}` or `{:error, %LanceDB.Error{}}`.
Incorrect API arguments raise `ArgumentError`.

The current source also supports connection and table-open deadlines, cache
budgets on the initial connection, and `Table.cache_stats/1`. See
[table-open controls and cache statistics](docs/tables.md#table-open-controls-and-listing)
for session sharing, option ranges, and examples.

## Documentation

| Guide | Contents |
| --- | --- |
| [API guide](docs/api.md) | Handles, query defaults, errors, records, and telemetry. |
| [Queries](docs/queries.md) | Filters, expressions, projections, direct retrieval, timeouts, diagnostics, and vector search. |
| [Full-text and hybrid search](docs/search.md) | Text queries, analyzers, ranking callbacks, and result ordering. |
| [Writes and streaming](docs/writes.md) | Input sources, appends, updates, merges, streams, progress, and backfills. |
| [Schemas and data types](docs/schema.md) | Explicit schemas, nested values, blobs, timestamps, schema changes, and metadata. |
| [Indexes](docs/indexes.md) | Index types, training, search tuning, readiness, and native jobs. |
| [Table operations](docs/tables.md) | Statistics, history, tags, branches, clones, table options, and maintenance. |
| [Installation and storage](docs/installation.md) | Native artifacts, authentication, deployment, offline builds, and S3 configuration. |
| [Development and tests](docs/development.md) | Local checks, StreamData properties, coverage, and MinIO tests. |
| [Native builds](docs/native-builds.md) | Explicit source builds, immutable artifacts, publication, and consumer checks. |
| [Implementation notes](docs/internals.md) | Module responsibilities and SDK compatibility adapters. |
| [Migration guide](docs/migration.md) | API changes from earlier checkouts. |
| [Changelog](CHANGELOG.md) | Public release notes. |

The source repository is [preciz/lancedb_ex](https://github.com/preciz/lancedb_ex).
This package uses the [MIT license](https://github.com/preciz/lancedb_ex/blob/main/LICENSE).
