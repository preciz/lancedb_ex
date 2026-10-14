# LanceDB for Elixir

[![Tests](https://github.com/preciz/lancedb_ex/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/preciz/lancedb_ex/actions/workflows/ci.yml)

This package embeds [LanceDB](https://lancedb.com/) in Elixir through Rustler.
Use local files or S3-compatible object storage. A database server is not
necessary. Supply your own vector embeddings.

The Elixir package version is `0.3.0`. The Rust software development kit
(SDK) is pinned to `0.40.0`. Each native build has a separate immutable ID.
The [selected native build](docs/installation.md#current-native-build) passed
public download and production release checks on Linux x86_64, Linux ARM64,
and Apple Silicon macOS, including supervised table caching, background FTS
warming, and shared cache reuse.

Unreleased FTS query preparation, standard table optimization, and stricter
Arrow batch write validation require `LANCEDB_BUILD=true` until a new matching
native build is published and selected. Version `0.3.0` does not include these
additions. The unreleased table-cache connection
accessor works with the currently selected native binaries.

## Requirements

- Elixir 1.20 or a later 1.x version. CI uses OTP 29.
- Linux x86_64 with glibc 2.35 or later, or Linux ARM64 with glibc 2.39 or later.
- Apple Silicon Macs with macOS 15 or later.

Native archives use public GitHub releases. GitHub CLI, credentials, Rust,
and Cargo are not necessary for normal installation.

## Installation

Add the package to `mix.exs`:

```elixir
defp deps do
  [
    {:lancedb_ex, "~> 0.3.0"}
  ]
end
```

Run these commands from your application directory:

```sh
mix deps.get
mix compile
```

Mix downloads the native archive and checks the committed checksum. The
package supplies a default artifact URL. Direct database operations need no
supervision tree entry. The optional table cache is application-supervised.

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
field. Database operations return `{:ok, value}` or `{:error, %LanceDB.Error{}}`.
Builders and in-memory inspection helpers return their values directly.
Incorrect API arguments raise `ArgumentError`.

Connections and table opens support deadlines, cache budgets on the initial
connection, and `Table.cache_stats/1`. See
[table-open controls and cache statistics](docs/tables.md#table-open-controls-and-listing)
for session sharing, option ranges, and examples.

## Supervised table caching

The optional table cache supports applications that retain
handles and warm indexes in the background. Add it to your supervision tree:

```elixir
{LanceDB.TableCache,
 name: MyApp.TableCache,
 uri: "./data/vectors",
 max_tables: 64,
 index_cache_size_bytes: 112 * 1024 * 1024,
 metadata_cache_size_bytes: 16 * 1024 * 1024,
 read_consistency_interval_ms: 0}
```

```elixir
{:ok, :scheduled} =
  LanceDB.TableCache.prewarm(MyApp.TableCache, "documents", indexes: ["text_idx"])

{:ok, table} = LanceDB.TableCache.fetch(MyApp.TableCache, "documents")
{:ok, rows} = LanceDB.Query.full_text_search(table, "Elixir")
```

The application creates tables and indexes and decides when to warm them.
The cache shares concurrent opens and index warming, retains one session,
and limits handles and background work. Fetches and queries can proceed during
warming. See [table caching](docs/table-cache.md) for status, invalidation,
capacity controls and consistency guarantees.

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
| [Table caching](docs/table-cache.md) | Optional supervision, shared handles, background warming, limits, and invalidation. |
| [Installation and storage](docs/installation.md) | Native artifacts, authentication, deployment, offline builds, and S3 configuration. |
| [Development and tests](docs/development.md) | Local checks, StreamData properties, coverage, and MinIO tests. |
| [Native builds](docs/native-builds.md) | Explicit source builds, immutable artifacts, publication, and consumer checks. |
| [Implementation notes](docs/internals.md) | Module responsibilities and SDK compatibility adapters. |
| [Changelog](CHANGELOG.md) | Public release notes. |

The source repository is [preciz/lancedb_ex](https://github.com/preciz/lancedb_ex).
This package uses the [MIT license](https://github.com/preciz/lancedb_ex/blob/main/LICENSE).
