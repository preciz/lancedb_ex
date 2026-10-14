# Table operations

[Back to README](../README.md#documentation).

These examples use `db` and `table` handles. See the [API guide](api.md) for setup.
Match the table schema to each example.

## Table statistics

```elixir
{:ok, stats} = LanceDB.Maintenance.stats(table)
stats["num_rows"]
stats["num_indices"]
stats["total_bytes"]
stats["fragment_stats"]["num_fragments"]
stats["fragment_stats"]["num_small_fragments"]
stats["fragment_stats"]["lengths"]
```

Fragment lengths report physical row counts as `min`, `max`, integer
`mean`, `p25`, `p50`, `p75`, and `p99`. These counts can include
deleted rows. `num_rows` counts live rows. In LanceDB 0.40.0, a small
fragment has fewer than 100,000 physical rows. Empty tables return zero
counts and distributions. Statistics use the table handle's version and
read-consistency settings.

This includes historical checkout.

`total_bytes` adds recorded data, index, and overlay file sizes for the
selected version. It excludes manifests, deletion files, historical
versions, and files without recorded sizes. The value describes recorded
storage for the current table. It is not a complete measurement of
filesystem or bucket usage.

## Version history and restore

Inspect kept versions and read an older snapshot:

```elixir
{:ok, saved_version} = LanceDB.History.version(table)
{:ok, history} = LanceDB.History.list_versions(table)
# Each History.Version has :version, an RFC3339 :timestamp, and string-keyed :metadata.

{:ok, snapshot} = LanceDB.History.snapshot(table, saved_version)
{:ok, historical_rows} = LanceDB.Query.query(snapshot)
{:ok, historical_schema} = LanceDB.Table.schema(snapshot)

# Resume normal reads and writes at the latest version:
{:ok, refreshed} = LanceDB.History.latest(table)
```

Checkout returns an independent read-only handle. You can safely share the
original handle between processes. Selecting a snapshot does not change
another caller's view. Branch checkout and numeric or tag checkout follow
the same ownership rule. `History.latest/1` returns a new live handle on the
same branch.

Restore selects an explicit version on a live target:

```elixir
{:ok, %LanceDB.WriteResult{version: new_version}} = LanceDB.History.restore(table, saved_version)
```

Restore creates a new version containing the historical data, schema, and
indexes. Existing snapshots stay pinned. Other live handles observe changes
according to their read-consistency policy. Kept history stays available
until cleanup.

### Version tags

Tags give kept versions durable names. Supply an explicit version when
creating or moving a tag:

```elixir
{:ok, saved_version} = LanceDB.History.version(table)
{:ok, :created} = LanceDB.History.create_tag(table, "before_import", saved_version)
{:ok, saved_version} = LanceDB.History.tag_version(table, "before_import")
{:ok, tags} = LanceDB.History.list_tags(table)
# %{"before_import" => %{"version" => ..., "manifest_size" => ..., "branch" => nil}}

{:ok, snapshot} = LanceDB.History.snapshot_tag(table, "before_import")
# Read the saved snapshot, then either return to latest or restore it:
{:ok, refreshed} = LanceDB.History.latest(table)

{:ok, current_version} = LanceDB.History.version(table)
{:ok, :updated} = LanceDB.History.update_tag(table, "before_import", current_version)
{:ok, :deleted} = LanceDB.History.delete_tag(table, "before_import")
```

To restore a tag, first get its version with `tag_version/2`. Then pass
that version to `restore/2` on a live target. Moving or deleting the tag
does not change an already-opened snapshot.

Tag names must be non-empty UTF-8 strings. LanceDB validates additional
naming constraints. Creating a duplicate tag returns an error. Resolving,
deleting, or updating a missing tag also returns an error. The same applies
when the target version is missing. Use a live handle to change tags.

Deleting a tag does not delete its version.

Cleanup fails if its retention cutoff would remove a tagged version. Keep
the tag to protect that snapshot. Alternatively, move or delete the tag
before cleanup.

Pruned versions cannot be checked out or restored. Select cleanup retention
to cover your recovery needs and the lifetime of historical readers. An
invalid or missing version returns an error without changing the visible
snapshot.

## Branches and shallow clones

Branches provide independent table histories. Creating or checking out a
branch returns a new handle and leaves the source handle unchanged:

```elixir
{:ok, experiment} = LanceDB.Branch.create(table, "experiment")
{:ok, "main"} = LanceDB.Branch.current(table)
{:ok, "experiment"} = LanceDB.Branch.current(experiment)
{:ok, branches} = LanceDB.Branch.list(table)

{:ok, version} = LanceDB.History.version(experiment)
{:ok, snapshot} = LanceDB.Branch.open(table, "experiment", version: version)
{:ok, main} = LanceDB.Branch.open(experiment, "main")
```

`Branch.create/3` accepts `from: version`, `from: {:tag, "saved"}`,
`from:
{:branch, "experiment"}`, or
`from: {:branch, "experiment", version}`. The default `:current` creates a
branch from the source handle's visible version. A plain version number
refers to that handle's branch. Pinned handles are read-only. They cannot
create or delete branches. `History.latest/1` returns a live handle on the
snapshot's own branch and enables writes.

`Branch.delete/2` removes a branch subject to the SDK's reference
restrictions. Branch listings contain `parent_branch`, `parent_version`,
`created_at` (Unix seconds), `manifest_size`, and `metadata`. The parent
branch is `nil` for main.

Shallow clones create a separate table with its own manifests. They share
data and index files with a source table:

```elixir
{:ok, source_uri} = LanceDB.Table.uri(table)
{:ok, copy} = LanceDB.Connection.clone_table(db, "copy", source_uri)
{:ok, old_copy} = LanceDB.Connection.clone_table(db, "old_copy", source_uri, source_version: 1)
{:ok, saved_copy} = LanceDB.Connection.clone_table(db, "saved_copy", source_uri, source_tag: "saved")
```

`source_version` selects a version on main. A source tag can identify a
branch snapshot. You cannot combine these selectors. The default is main's
latest version. The connection storage options must give access to both
locations. Clone writes do not update the source.

The clone still depends on shared source files. **Keep those files while
clones reference them.** Dropping or pruning the source can invalidate a
clone. A shallow clone cannot serve as an independent backup. The package
does not expose deep cloning or branch diff and merge.

## Table-open controls and listing

Connect/open deadlines, initial connection cache budgets, and cache statistics
are additions in `0.1.1`. See
[installation](installation.md#current-native-build) for the required native build.

Both connection and table opens accept an optional `timeout_ms`:

```elixir
{:ok, conn} = LanceDB.connect(uri, storage_options, timeout_ms: 5_000)
{:ok, table} = LanceDB.Connection.open_table(conn, "docs", timeout_ms: 2_000)
```

The value is a positive uint32 millisecond count; omitted or `nil` adds no
wrapper deadline. A timeout returns `{:error, %LanceDB.Error{kind: :timeout}}`.
The open budget covers the whole operation, including the extra SDK connection
when per-open cache budgets select a dedicated session. These APIs wait for a
result in the caller's mailbox while native work runs on Tokio. Storage waits
retain no BEAM scheduler. Caller death cancels the operation and releases its
read admission slot. Cancellation is cooperative: it cannot preempt synchronous
SDK work or guarantee that separately spawned SDK storage tasks have stopped.
A `GenServer.call` timeout only stops waiting; use `timeout_ms` to bound native
work, or terminate the calling process to cancel it.

Existing `open_table/2` calls keep their behavior. The optional third
argument can select a branch or version. It can also override storage
options or location and configure native cache budgets:

```elixir
{:ok, snapshot} = LanceDB.Connection.open_table(conn, "docs", branch: "experiment", version: 2)
{:ok, table} = LanceDB.Connection.open_table(conn, "docs",
  index_cache_size_bytes: 128 * 1024 * 1024,
  metadata_cache_size_bytes: 32 * 1024 * 1024,
  storage_options: %{aws_region: "us-east-1"}
)
{:ok, located} = LanceDB.Connection.open_table(conn, "external", location: source_uri)

{:ok, first_page} = LanceDB.Connection.table_names(conn, limit: 20)
{:ok, next_page} = LanceDB.Connection.table_names(conn, limit: 20, start_after: List.last(first_page))
```

The SDK returns names in lexicographic order. `start_after` is exclusive.
Request a next page only when the previous page is non-empty. Omitting
listing options returns all tables. A version pins a read-only snapshot of
the selected branch. The default branch is main.

Opening a snapshot does not change other handles.

Storage options override the connection's options for the opened table.
Either cache limit creates a dedicated native session for that handle.
Budgets must be integer byte counts from 0 through 9,223,372,036,854,775,807;
omit an option to use its default rather than passing `nil`.
Snapshots and branch checkouts preserve it; branch creation uses a new SDK
session. The dedicated session keeps connection storage and
read-consistency settings. The other cache keeps its SDK default. Zero
disables the selected cache. These budgets do not limit total query memory.

If you do not set either limit, opened tables use the shared connection
session. These options do not represent arbitrary Rust read callbacks,
credential providers, or namespace clients.

To configure the session on the initial SDK connection, give the cache
budgets to `connect/3` and omit them from `open_table/3`:

```elixir
{:ok, conn} = LanceDB.connect(uri, storage_options,
  index_cache_size_bytes: 112 * 1024 * 1024,
  metadata_cache_size_bytes: 16 * 1024 * 1024,
  read_consistency_interval_ms: 0,
  timeout_ms: 5_000
)
{:ok, table} = LanceDB.Connection.open_table(conn, "docs", timeout_ms: 2_000)
```

This creates the configured session once, without first creating a default
SDK connection and then replacing it during open. Each connection owns an
independent session. Tables created or opened without per-open budgets on
that connection share its cache capacity. Use one such connection per dataset
when you need independent budgets. The connection itself does not retain a
table handle or implement application cache eviction.

Either connection budget selects a configured session; the other cache keeps
its SDK default, and zero disables a cache. Existing per-open budgets still
create a separate session, even on a configured connection. Their omitted
cache keeps its SDK default rather than inheriting the connection's budget.
All budgets apply per session and do not limit total native memory or RSS.

`LanceDB.Table.cache_stats/1` inspects the handle's actual native session
without installing the process-global metrics recorder:

```elixir
{:ok, %{"scope" => "session", "index" => index, "metadata" => metadata}} =
  LanceDB.Table.cache_stats(table)
index["size_bytes"]
metadata["num_entries"]
```

Each cache reports `size_bytes` (SDK-accounted entry bytes), `num_entries`,
and cumulative `hits` and `misses`. `capacity_bytes` is `nil`: the pinned SDK
does not expose capacity. These figures describe cache entries, not total
native memory or process RSS. Counters are not reset by inspection; concurrent
work can change individual fields while the snapshot is read. Shared handles
report session-wide statistics; explicit per-open cache budgets isolate them.
Snapshots and branch checkouts share their source session; creating a branch
uses a new SDK session. Resolving the
session follows read consistency and may check storage. The result contains
no URI, credentials, or storage options.

These cache statistics differ from `Maintenance.stats/1`, which describes
stored table files and rows, and `Telemetry.snapshot/0`, which reads opt-in
process-global native metrics.

## Table maintenance

Run maintenance after batches of writes, according to your application's
workload:

```elixir
# Standard SDK pass: compact, clean up history older than seven days, then optimize indexes.
{:ok, %{"compaction" => compaction, "cleanup" => cleanup}} =
  LanceDB.Maintenance.optimize(table)

# Or select individual operations and customize their settings:
# Merge small fragments while preserving current data and schema.
{:ok, compaction} = LanceDB.Maintenance.compact_files(table)
# Optional target size (rows, not bytes):
{:ok, compaction} = LanceDB.Maintenance.compact_files(table, target_rows_per_fragment: 100_000)

# Add unindexed rows to existing scalar and vector indexes.
{:ok, :optimized} = LanceDB.Maintenance.optimize_indices(table)
{:ok, stats} = LanceDB.Index.stats(table, "vector_cosine")

# Permanently remove versions older than seven days and unreferenced files.
{:ok, cleanup} = LanceDB.Maintenance.cleanup_old_versions(table)
# Or keep thirty days of history:
{:ok, cleanup} = LanceDB.Maintenance.cleanup_old_versions(table, older_than_seconds: 30 * 86_400)
```

These operations return synchronously to the Elixir caller while native work
runs on Tokio and replies by message. Storage waits retain no BEAM scheduler.
Schedule maintenance in your application; the package does not schedule it.

`optimize/1` wraps LanceDB 0.40.0's `OptimizeAction::All` under one native write
admission permit for the entire pass. Its fixed order is file compaction with SDK
defaults, cleanup of versions older than seven days, and optimization of existing
indexes with SDK defaults. It preserves current rows and schema, creates no missing
indexes, and does not request vector retraining. Use the individual operations
below for different settings or retention.

The result is a string-keyed map. `"compaction"` contains `"fragments_removed"`,
`"fragments_added"`, `"files_removed"`, and `"files_added"`; `"cleanup"` contains
the same removal statistics as `cleanup_old_versions/2`. Compaction counts describe
rewrites, not necessarily files physically deleted. Successful cleanup may remove
nothing when every version is recent.

The cleanup defaults match `cleanup_old_versions/2`: tagged old versions cause an
error and recent unverified files retain the SDK's seven-day protection window.
Cleanup is irreversible; seven days is suitable only when it exceeds the lifetime
of readers and writers using old versions. Coordinate maintenance and writes in
your application. The pass is not one transaction and has no rollback: compaction
can complete before cleanup or index optimization fails. Failures return a
`LanceDB.Error` without a structured failed phase or statistics for completed steps.
Caller death does not cancel an admitted write; native admission remains held
until the SDK operation settles.

This unreleased operation requires `LANCEDB_BUILD=true` until matching native
binaries are published and selected. It is absent from Hex version `0.3.0`.

`compact_files/2` defaults to 1,048,576 target rows per fragment. It uses
the SDK's other compaction defaults, including index remapping. Its result
contains `Maintenance.Compaction` fields `:fragments_removed`,
`:fragments_added`, `:files_removed`, and `:files_added`. These counts
describe rewritten data. Old files can stay referenced by kept versions.
Compaction alone does not necessarily reduce disk usage.

`optimize_indices/2` adds unindexed data to indexes. Restrict it with
`index_names: ["text_fts"]`. `num_indices_to_merge: 0` only indexes new
rows. A positive value controls how many deltas are merged. Omission or nil
uses SDK defaults. `retrain: true` retrains vector models.

You cannot combine it with a merge count. Inspect `"num_unindexed_rows"`
with `LanceDB.Index.stats/2`. Compaction and index optimization do not
prune old versions.

Compaction exposes writer sizes through `max_rows_per_group` and
`max_bytes_per_file`. Other controls are `num_threads`, `batch_size`,
`io_buffer_size`, `binary_copy_read_batch_bytes`, and
`max_source_fragments`. These values must be positive uint32 integers. Set
`materialize_deletions` and `materialize_deletions_threshold` to control
deletion materialization. The threshold must be between 0 and 1.
`compaction_mode` accepts `:reencode`, `:try_binary_copy`, or
`:force_binary_copy`.

Forced copying fails for incompatible fragments. `defer_index_remap` is
boolean. `index_remap_mode` accepts `:direct` or `:compact`.
`max_overlays_per_fragment` accepts nil or a non-negative uint32 integer.
Omitted options keep SDK defaults.

`cleanup_old_versions/2` defaults to 604,800 seconds of retention. Cleanup
is **irreversible**. Pruned versions and their unreferenced data are no
longer readable. Select retention longer than the lifetime of readers and
writers that can use older versions. Zero is supported for idle tables with
no readers using old snapshots. Cleanup keeps the latest version.

Tagged old versions cause an error. The SDK's seven-day safety window
protects recent unverified files. `error_if_tagged_old_versions: false`
keeps tagged versions while pruning other eligible versions.
`delete_unverified: true` bypasses the file protection window. Use it only
when no active writers can own those unreferenced files.

Cleanup returns `"bytes_removed"`, `"old_versions"`,
`"data_files_removed"`, `"transaction_files_removed"`,
`"index_files_removed"`, and `"deletion_files_removed"`. A successful call
can remove nothing when all versions are within retention. Invalid options
raise `ArgumentError`. SDK failures return `{:error, reason}`. Operations
are independent. Successful earlier steps stay applied if a later
maintenance call fails.

`LanceDB.Maintenance.uses_v2_manifest_paths/1` inspects manifest naming.
`LanceDB.Maintenance.migrate_manifest_paths_v2/1` performs an idempotent
migration to V2 paths. Stop other table operations before migration.
Historical handles cannot migrate.
