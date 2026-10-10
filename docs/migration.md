# Migration guide

[Back to README](../README.md#documentation).

## API migration

This package has no public package release. Earlier checkouts used separate
entry points for query sources and input formats. Use the current operations
below when you update callers.

| Previous entry point | Current entry point |
| --- | --- |
| `Query.rows(table, vector: vector, ...)` | Build a query with `Query.new`, `nearest_to`, and `options`. Then call `Query.stream`. |
| `Query.explain_plan`, `analyze_plan`, `output_schema` | Build a Query, then call `explain`, `analyze`, or `schema`. |
| `Connection.create_stream`, `create_batches` | Call `Connection.create_table` with `Source.rows` or `Source.batches`. |
| `Table.add_stream`, `add_batches` | Call `Table.add` with a Source. |
| `Table.merge_stream`, `merge_batches`, `upsert_stream`, `upsert_batches` | Call `Table.merge` or `Table.upsert` with a Source. |
| Table version, snapshot, restore, and tag methods | Use the related methods on `History`. |
| `Table.create_branch`, `open_branch`, `list_branches`, `current_branch`, `delete_branch` | Use `Branch.create`, `open`, `list`, `current`, and `delete`. |
| Table maintenance and statistics methods | Use the related methods on `Maintenance`. |
| `Table.add_columns`, `alter_columns`, `drop_columns`, `backfill_columns` | Use the related methods on `SchemaEvolution`. |
| Column sources with a separate schema argument | Use `SchemaEvolution.add_from_source` with the schema on the Source. |
| Table schema metadata methods | Use `Metadata.schema`, `update_schema`, and `update_field`. |
| Table blob column, fetch, range, and file methods | Use `Blob.columns`, `fetch`, `fetch_ranges`, and `open`. |

For example, build a vector stream with separate query and stream options:

```elixir
query =
  table
  |> LanceDB.Query.new()
  |> LanceDB.Query.nearest_to(vector)
  |> LanceDB.Query.options(query_opts)

rows = LanceDB.Query.stream(query, stream_opts)
```

Simple row lists stay valid inputs for creation and writes. Creation sources
need a schema. The operation accepts equal source and operation schemas.
Conflicting schemas raise before the operation reads input. Existing-table
writes check supplied source fields against the destination. Source schema
metadata does not change destination metadata.

A source change keeps explicit query controls. Omitted limits use the new
source's default: 100 for scans and 10 for searches. `limit: nil` is valid
only for scans. Query values contain semantic controls. There is no public
native request map.

`Table.schema/1` returns logical `Schema`. `Query.schema/1` and
`Batch.schema/1` return physical `ArrowSchema`. Use
`ArrowSchema.to_schema/1` to check storage compatibility before creating a
destination. Blob descriptors need explicit payload fetching and a `:blob`
destination. `Blob.reference/1` returns `Blob.Reference`.

`History.list_versions/1`, `Index.list/1`, and
`Maintenance.compact_files/2` return `History.Version`, `Index.Description`, and `Maintenance.Compaction` structs. Use atom fields, such as
`version.version`, `index.name`, and `compaction.fragments_removed`. User
metadata, raw statistics, and native metric attributes keep string keys.

Convenience functions enter the public operation once. Each operation owns
one telemetry span. Query open, next, and close events share an
`execution_id` for one stream read. Immediate query events also provide
`parent_operation_id`. Each new stream read and callback API call gets
separate IDs.
