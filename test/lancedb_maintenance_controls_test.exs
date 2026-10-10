defmodule LanceDB.MaintenanceControlsTest do
  use LanceDB.DataCase, async: true
  alias LanceDB.{Connection, Index, Query, Table}

  setup %{path: path} do
    {:ok, conn} = LanceDB.connect(path)
    %{conn: conn}
  end

  test("index selection leaves unrelated indexes untouched", %{conn: conn}) do
    rows =
      for id <- 1..32 do
        %{id: id, vector: [id / 32, 1.0]}
      end

    {:ok, table} = Connection.create_table(conn, "indexed", rows)
    assert {:ok, :created} = Index.create(table, :id, type: :btree, name: "ids")

    assert {:ok, :created} =
             Index.create(table, :vector, type: :ivf_flat, name: "vectors", num_partitions: 1)

    assert {:ok, _} = Table.add(table, [%{id: 33, vector: [2.0, 1.0]}])

    assert {:ok, :optimized} =
             LanceDB.Maintenance.optimize_indices(table,
               index_names: ["ids"],
               num_indices_to_merge: 0
             )

    assert {:ok, %{"num_unindexed_rows" => 0}} = Index.stats(table, "ids")
    assert {:ok, %{"num_unindexed_rows" => 1}} = Index.stats(table, "vectors")

    assert {:ok, :optimized} =
             LanceDB.Maintenance.optimize_indices(table, index_names: [:vectors], retrain: true)

    assert {:ok, %{"num_unindexed_rows" => 0}} = Index.stats(table, "vectors")
  end

  test("compaction controls preserve surviving rows", %{conn: conn}) do
    {:ok, table} = Connection.create_empty_table(conn, "compact", id: :int64)

    for id <- 1..6 do
      assert {:ok, _} = Table.add(table, [%{id: id}])
    end

    assert {:ok, _} = Table.delete(table, "id = 1")
    assert {:ok, before} = Query.query(table, order_by: [asc: :id])

    assert {:ok, %LanceDB.Maintenance.Compaction{fragments_removed: removed}} =
             LanceDB.Maintenance.compact_files(table,
               target_rows_per_fragment: 100,
               max_rows_per_group: 16,
               max_bytes_per_file: 1_048_576,
               num_threads: 1,
               batch_size: 2,
               io_buffer_size: 1_048_576,
               materialize_deletions: true,
               materialize_deletions_threshold: 0.0,
               defer_index_remap: false,
               compaction_mode: :reencode,
               index_remap_mode: :direct,
               max_overlays_per_fragment: nil,
               binary_copy_read_batch_bytes: 1_048_576,
               max_source_fragments: 16
             )

    assert removed > 1
    assert {:ok, ^before} = Query.query(table, order_by: [asc: :id])

    assert {:ok, _} =
             LanceDB.Maintenance.compact_files(table,
               compaction_mode: :try_binary_copy,
               index_remap_mode: :compact
             )
  end

  test("pruning can retain tagged versions and remove other old versions", %{conn: conn}) do
    {:ok, table} = Connection.create_table(conn, "prune", [%{id: 1}])
    assert {:ok, :created} = LanceDB.History.create_tag(table, "keep", 1)
    assert {:ok, _} = Table.add(table, [%{id: 2}])
    assert {:ok, _} = Table.add(table, [%{id: 3}])
    assert {:error, _} = LanceDB.Maintenance.cleanup_old_versions(table, older_than_seconds: 0)

    assert {:ok, %{"old_versions" => removed}} =
             LanceDB.Maintenance.cleanup_old_versions(table,
               older_than_seconds: 0,
               error_if_tagged_old_versions: false,
               delete_unverified: true
             )

    assert removed >= 1
    assert {:ok, tagged} = LanceDB.History.snapshot_tag(table, "keep")
    assert {:ok, [%{"id" => 1}]} = Query.query(tagged)
    assert {:ok, 3} = Table.count_rows(table)
  end

  test "invalid maintenance controls are rejected before execution" do
    for opts <- [
          [index_names: []],
          [num_indices_to_merge: -1],
          [retrain: nil],
          [retrain: true, num_indices_to_merge: 1],
          [unknown: true]
        ] do
      assert_raise ArgumentError, fn -> LanceDB.Maintenance.optimize_indices(:unused, opts) end
    end

    for opts <- [
          [compaction_mode: :invalid],
          [index_remap_mode: :invalid],
          [num_threads: 0],
          [materialize_deletions_threshold: -0.1],
          [materialize_deletions_threshold: 1.1],
          [materialize_deletions: nil],
          [defer_index_remap: 1],
          [max_overlays_per_fragment: -1]
        ] do
      assert_raise ArgumentError, fn -> LanceDB.Maintenance.compact_files(:unused, opts) end
    end

    assert_raise ArgumentError, fn ->
      LanceDB.Maintenance.cleanup_old_versions(:unused, error_if_tagged_old_versions: nil)
    end
  end
end
