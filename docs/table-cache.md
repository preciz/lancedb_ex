# Supervised table caching

[Back to README](../README.md#documentation).

`LanceDB.TableCache` is optional. Applications that repeatedly open the same
tables can retain handles and warm several indexes in the background. The
application owns credentials, schemas, index creation, table names, and when
to request warming. This component handles sharing, capacity and lifecycle.

## Supervision and configuration

Add a child to your application's supervision tree:

```elixir
children = [
  {LanceDB.TableCache,
   name: MyApp.TableCache,
   uri: "s3://my-bucket/database",
   storage_options: storage_options,
   max_tables: 64,
   idle_timeout_ms: 3_600_000,
   index_cache_size_bytes: 112 * 1024 * 1024,
   metadata_cache_size_bytes: 16 * 1024 * 1024,
   read_consistency_interval_ms: 0}
]

Supervisor.start_link(children, strategy: :one_for_one)
```

Storage opens lazily on the first fetch or warming request. Every instance owns
one connection and SDK session with fixed storage, cache and consistency settings.
Use separate named instances for incompatible settings. Each BEAM node owns its
own instance. There is no distributed coordination or automatic index creation.

| Option | Default | Meaning |
| --- | --- | --- |
| `uri` | Required | Database location. |
| `name` | Unregistered | GenServer name; public calls also accept its PID. |
| `storage_options` | `%{}` | Same normalized options as `Connection.connect/3`. |
| `index_cache_size_bytes`, `metadata_cache_size_bytes` | SDK defaults | Shared session budgets; zero disables that cache. |
| `read_consistency_interval_ms` | SDK default | Zero checks for external changes on each read. |
| `connect_timeout_ms`, `open_timeout_ms` | `5_000` each | Native deadlines; `nil` disables a deadline. |
| `max_tables` | `64` | Retained entries, including pending opens. |
| `idle_timeout_ms` | `3_600_000` | Expire entries idle since fetch/prewarm; `nil` disables expiry. |
| `max_opens` | `4` | Concurrent table opens. |
| `max_warmers` | `2` | Concurrent table warming tasks. |
| `max_requests` | `256` | Waiting fetch callers plus remembered per-index warming records. |

Limits and non-nil idle intervals must be positive uint32 integers. Cache budgets,
consistency intervals and native deadlines have the same ranges as
[connection/open controls](tables.md#table-open-controls-and-listing).
Unknown options raise `ArgumentError` before storage work begins.

## Fetching and warming

```elixir
alias LanceDB.{Query, TableCache}

{:ok, :scheduled} =
  TableCache.prewarm(MyApp.TableCache, "documents", indexes: ["names_idx", "text_idx"])

{:ok, table} = TableCache.fetch(MyApp.TableCache, "documents", timeout_ms: 15_000)
{:ok, rows} = Query.full_text_search(table, "Elixir", text_column: :text)
{:ok, statuses} = TableCache.status(MyApp.TableCache, "documents")
```

`fetch/3` returns the same retained handle to concurrent callers. Opens run outside
the coordinator, so a slow open need not block other tables. The caller timeout
covers queueing, connection and opening; `nil` waits indefinitely. Caller death
or timeout removes that waiter. The shared open continues for the cache and other
callers. Failed opens can be retried by another fetch.

`prewarm/3` returns after scheduling, without waiting for storage. Supply a non-empty
list of distinct string or atom index names. Requests for queued or running indexes
share work. Indexes for one table warm sequentially; different tables can warm in
parallel up to `max_warmers`. New indexes requested during an active batch wait for
that batch to finish. Fetching retained handles and querying them can proceed while
warming is active, subject to ordinary native admission and storage limits.

`status/2` returns a string-keyed map of `LanceDB.TableCache.Warming` structs:

```elixir
%{
  "text_idx" => %LanceDB.TableCache.Warming{
    status: :completed,
    version: 3,
    index_uuid: "...",
    duration_ms: 12,
    error: nil
  }
}
```

States are `:queued`, `:running`, `:completed`, and `:failed`. An unwarmed retained
table returns an empty map; an absent/evicted table returns `:not_found`. Failures
contain a structured `LanceDB.Error`, including missing indexes, worker crashes,
native admission `:busy` and observed concurrent table changes (`:conflict`). Another
prewarm request retries failed indexes. There are no automatic retries.

Completed attempts are revalidated against table version and index UUID before
skipping native prewarming. Table version catches added index segments that keep
the same first-segment UUID. Warming pins an independent snapshot sharing the
fetched session and checks the live version again afterward. Changes observed
during the attempt fail it for retry. Queued/running records can retain the last
completed identity until the attempt finishes; failed records clear it.

The handle's read-consistency policy governs visibility of external changes.
Use `read_consistency_interval_ms: 0` when warming should check them every time.
The SDK default does not refresh automatically after another handle writes.
Cached handles are live main-branch tables. Use direct APIs for custom locations,
branches, per-open storage overrides and historical checkouts.

## Eviction and invalidation

```elixir
:ok = LanceDB.TableCache.invalidate(MyApp.TableCache, "documents")
```

Invalidate after dropping/recreating a table, or to discard retained warming
records and force a fresh attempt. It succeeds even if the entry is absent.
Pending fetches receive `:cancelled`. Late open/warming results cannot recreate
an entry or mark a replacement complete. Invalidation does not revoke handles
already returned to callers.

At `max_tables`, admission evicts the entry least recently fetched/prewarmed.
Pending fetch callers protect an entry until they finish, die or time out.
Idle expiry runs periodically and on fetch, prewarm and status calls. Queries on
returned handles do not refresh cache activity. Status inspection also does not
keep an entry alive. Warming alone does not protect an entry from eviction.

`max_requests` includes completed and failed index records, so both pending work
and remembered status stay bounded. Invalidation and eviction discard records.
Saturation returns `:busy`. Already-retained handles remain fetchable without
adding a waiting request. Invalidated/evicted workers count toward concurrency
limits until they settle; repeated invalidation cannot create unbounded work.

Native prewarming has no deadline and cannot be forcibly interrupted by killing
an Elixir task. An abandoned attempt can still populate its shared session.
Eviction releases this component's references, while caller-held handles and
session caches can outlive the entry. `max_tables` bounds managed entries;
session byte budgets separately bound index/metadata caches. Neither bounds all
native memory. A successful warming record does not guarantee page residency;
scans and other queries can evict warmed pages later.

## Telemetry

Cache calls and background workers emit the usual operation spans described in
the [API guide](api.md#telemetry). Worker metadata includes a per-instance
`cache_id`, table name, entry generation and requested index names. Names can
contain application data; credentials and handles are not added to successful
events.

| Event | Measurements | Metadata |
| --- | --- | --- |
| `[:lancedb, :table_cache, :warm]` | `duration_ms` | `cache_id`, `table_name`, `index_name`, `status`, `version`, `index_uuid`, `error_kind` |
| `[:lancedb, :table_cache, :evict]` | `count: 1` | `cache_id`, `table_name`, `reason` (`:capacity`, `:idle`, `:invalidated`) |

Warming events describe each applied index result, including failures and skipped
unchanged work. Batch operation success means the batch was inspected; individual
index failures are reported in per-index events and status. Stale discarded results
have worker spans but do not emit applied index events. Connection/open failures
appear in their worker spans and mark queued warming records failed. Duration is
in milliseconds for cache warming events; operation spans use native time units.

Use `Table.cache_stats/1` on a fetched handle to inspect actual shared session
usage and hit/miss counters. These counters span all retained tables in the instance.
