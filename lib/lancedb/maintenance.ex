defmodule LanceDB.Maintenance do
  @moduledoc "Table statistics, compaction, index optimization and version cleanup."
  alias LanceDB.Backend
  alias LanceDB.Operations.Maintenance
  @type table :: LanceDB.Table.t()
  @type result(value) :: LanceDB.result(value)
  @doc """
  Returns table statistics as a map with string keys.

  Includes `num_rows`, `num_indices`, `total_bytes`, and `fragment_stats` with
  fragment counts and physical row-count distributions (`min`, `max`, integer
  `mean`, `p25`, `p50`, `p75`, `p99`). In LanceDB 0.40.0, small fragments have
  fewer than 100,000 physical rows. Physical counts can include deleted rows.

  `total_bytes` sums recorded data, index, and overlay file sizes for this table
  version. It excludes manifests, deletion files, historical versions, and files
  whose sizes were not recorded. It is not a complete directory or bucket size.
  Uses the table handle's version and read-consistency configuration.
  """
  @spec stats(table()) :: result(map())
  def stats(table) do
    LanceDB.Execution.run(:table_stats, fn -> Maintenance.table_stats(table) end)
  end

  @doc """
  Adds unindexed rows to existing indexes, returning `{:ok, :optimized}`.
  `:index_names` restricts work to named indexes (a non-empty list).
  `:num_indices_to_merge` controls index deltas to merge; zero only indexes new rows,
  nil/omitted uses the SDK default. `:retrain` rebuilds vector models (default false)
  and cannot be combined with `:num_indices_to_merge`.
  Missing indexes are not created. Use `LanceDB.Index.stats/2` to inspect coverage.
  """
  @spec optimize_indices(table(), keyword()) :: result(:optimized)
  def optimize_indices(table, opts \\ []) do
    LanceDB.Execution.run(:optimize_indices, fn -> Maintenance.optimize_indices(table, opts) end)
  end

  @doc """
  Merges small data fragments and returns a Maintenance.Compaction struct.
  `:target_rows_per_fragment` defaults to 1,048,576. Positive uint32 sizes also include
  `:max_rows_per_group`, `:max_bytes_per_file`, `:num_threads`, `:batch_size`,
  `:io_buffer_size`, `:binary_copy_read_batch_bytes`, and `:max_source_fragments`.
  Boolean controls are `:materialize_deletions` and `:defer_index_remap`;
  `:materialize_deletions_threshold` is between 0 and 1.
  `:compaction_mode` is `:reencode`, `:try_binary_copy`, or `:force_binary_copy`;
  forced binary copying fails for incompatible fragments. `:index_remap_mode` is
  `:direct` or `:compact`. `:max_overlays_per_fragment` is nil or a non-negative uint32.
  Omitted options preserve SDK defaults. Current rows and schema are preserved;
  old files remain until version cleanup.
  """
  @spec compact_files(table(), keyword()) :: result(LanceDB.Maintenance.Compaction.t())
  def compact_files(table, opts \\ []) do
    LanceDB.Execution.run(:compact_files, fn -> Maintenance.compact_files(table, opts) end)
  end

  @doc """
  Permanently removes old versions and their unreferenced files.

  `:older_than_seconds` sets retention (default 604,800: seven days). Zero is
  allowed for an idle table with no readers using old versions. Choose retention
  longer than the lifetime of readers and writers; pruned versions cannot be read
  again. The latest version is retained. `:error_if_tagged_old_versions` defaults
  to true; false skips tagged versions while pruning other eligible versions.
  `:delete_unverified` defaults to false. True also deletes unreferenced files
  within the SDK's seven-day protection window; use only with no active writers.
  Returns removal statistics with string keys.
  """
  @spec cleanup_old_versions(table(), keyword()) :: result(map())
  def cleanup_old_versions(table, opts \\ []) do
    LanceDB.Execution.run(:cleanup_old_versions, fn ->
      Maintenance.cleanup_old_versions(table, opts)
    end)
  end

  @doc "Returns whether the table uses V2 manifest paths."
  @spec uses_v2_manifest_paths(table()) :: result(boolean())
  def uses_v2_manifest_paths(table),
    do:
      LanceDB.Execution.run(:uses_v2_manifest_paths, fn ->
        Backend.uses_v2_manifest_paths(table)
      end)

  @doc "Migrates manifest paths to V2. Run only while all other table operations are stopped; idempotent."
  @spec migrate_manifest_paths_v2(table()) :: result(:migrated)
  def migrate_manifest_paths_v2(table),
    do:
      LanceDB.Execution.run(:migrate_manifest_paths_v2, fn ->
        Backend.migrate_manifest_paths_v2(table)
      end)
end
