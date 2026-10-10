defmodule LanceDB.TableStatsTest do
  use LanceDB.DataCase, async: true

  setup %{path: path} do
    {:ok, db} = LanceDB.connect(path)
    {:ok, table} = LanceDB.Connection.create_empty_table(db, "items", id: :int64)
    %{table: table}
  end

  test "empty tables expose zero counts and distributions", %{table: table} do
    assert {:ok, stats} = LanceDB.Maintenance.stats(table)
    assert stats["total_bytes"] == 0
    assert stats["num_rows"] == 0
    assert stats["num_indices"] == 0
    assert stats["fragment_stats"]["num_fragments"] == 0
    assert stats["fragment_stats"]["num_small_fragments"] == 0

    assert stats["fragment_stats"]["lengths"] == %{
             "min" => 0,
             "max" => 0,
             "mean" => 0,
             "p25" => 0,
             "p50" => 0,
             "p75" => 0,
             "p99" => 0
           }
  end

  test "statistics track appends, indexes, deletions, compaction and historical versions", %{
    table: table
  } do
    {:ok, %{inserted: 3}} = LanceDB.Table.add(table, [%{id: 1}, %{id: 2}, %{id: 3}])
    {:ok, %{inserted: 2}} = LanceDB.Table.add(table, [%{id: 4}, %{id: 5}])
    {:ok, before} = LanceDB.Maintenance.stats(table)
    {:ok, version} = LanceDB.History.version(table)
    assert before["num_rows"] == 5
    assert before["total_bytes"] > 0
    assert before["num_indices"] == 0
    assert before["fragment_stats"]["num_fragments"] == 2
    assert before["fragment_stats"]["num_small_fragments"] == 2

    assert before["fragment_stats"]["lengths"] == %{
             "min" => 2,
             "max" => 3,
             "mean" => 2,
             "p25" => 2,
             "p50" => 3,
             "p75" => 3,
             "p99" => 3
           }

    {:ok, :created} = LanceDB.Index.create(table, :id, type: :btree)
    {:ok, indexed} = LanceDB.Maintenance.stats(table)
    assert indexed["num_indices"] == 1
    assert indexed["total_bytes"] > before["total_bytes"]
    {:ok, %{operation: :delete}} = LanceDB.Table.delete(table, "id = 1")
    {:ok, deleted} = LanceDB.Maintenance.stats(table)
    assert deleted["num_rows"] == 4
    assert deleted["fragment_stats"]["lengths"]["max"] == 3
    {:ok, _} = LanceDB.Maintenance.compact_files(table, target_rows_per_fragment: 100)
    {:ok, compacted} = LanceDB.Maintenance.stats(table)
    assert compacted["num_rows"] == 4
    assert compacted["fragment_stats"]["num_fragments"] == 1

    {:ok, table} = LanceDB.History.snapshot(table, version)
    assert {:ok, ^before} = LanceDB.Maintenance.stats(table)
    {:ok, table} = LanceDB.History.latest(table)
    assert {:ok, ^compacted} = LanceDB.Maintenance.stats(table)
  end
end
