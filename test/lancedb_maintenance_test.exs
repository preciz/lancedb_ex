defmodule LanceDB.MaintenanceTest do
  use LanceDB.DataCase, async: true

  setup %{path: path} do
    {:ok, conn} = LanceDB.connect(path)
    %{conn: conn, path: path}
  end

  test("index optimization incorporates appended scalar and vector rows", %{conn: conn}) do
    rows =
      for id <- 1..32 do
        %{id: id, vector: [id / 32, 1.0]}
      end

    {:ok, table} = LanceDB.Connection.create_table(conn, "indexed", rows)
    assert {:ok, :created} = LanceDB.Index.create(table, :id, type: :btree, name: "ids")

    assert {:ok, :created} =
             LanceDB.Index.create(table, :vector,
               type: :ivf_flat,
               name: "vectors",
               num_partitions: 1
             )

    assert {:ok, %{inserted: 1}} = LanceDB.Table.add(table, [%{id: 33, vector: [2.0, 1.0]}])

    for name <- ["ids", "vectors"] do
      assert {:ok, %{"num_unindexed_rows" => 1}} = LanceDB.Index.stats(table, name)
    end

    assert {:ok, :optimized} = LanceDB.Maintenance.optimize_indices(table)

    for name <- ["ids", "vectors"] do
      assert {:ok, %{"num_indexed_rows" => 33, "num_unindexed_rows" => 0}} =
               LanceDB.Index.stats(table, name)
    end

    assert {:ok, [%{"id" => 33}]} = LanceDB.Query.query(table, filter: "id = 33")
    assert {:ok, [%{"id" => 33}]} = LanceDB.Query.search(table, [2, 1], limit: 1)
    assert {:ok, :optimized} = LanceDB.Maintenance.optimize_indices(table)
  end

  test("compaction preserves updated rows, schema and indexes across reopen", %{
    conn: conn,
    path: path
  }) do
    {:ok, table} =
      LanceDB.Connection.create_empty_table(conn, "compact",
        id: :int64,
        text: :string,
        vector: {:vector, 2}
      )

    for id <- 1..8 do
      assert {:ok, %{inserted: 1}} =
               LanceDB.Table.add(table, [%{id: id, text: nil, vector: [id, 1]}])
    end

    assert {:ok, :created} = LanceDB.Index.create(table, :id, type: :btree)

    assert {:ok, :created} =
             LanceDB.Index.create(table, :vector, type: :ivf_flat, num_partitions: 1)

    assert {:ok, %{operation: :delete}} = LanceDB.Table.delete(table, "id = 1")

    assert {:ok, %{updated: 1}} =
             LanceDB.Table.update(table, %{text: "updated"}, filter: "id = 2")

    assert {:ok, before} = LanceDB.Query.query(table, order_by: [asc: :id])
    assert {:ok, schema} = LanceDB.Table.schema(table)
    assert {:ok, stats} = LanceDB.Maintenance.compact_files(table, target_rows_per_fragment: 100)
    assert stats.fragments_removed > stats.fragments_added
    assert stats.files_added > 0
    assert stats.files_removed > 0
    assert {:ok, ^before} = LanceDB.Query.query(table, order_by: [asc: :id])
    assert {:ok, ^schema} = LanceDB.Table.schema(table)
    assert {:ok, [%{"text" => "updated"}]} = LanceDB.Query.query(table, filter: "id = 2")
    assert {:ok, [%{"id" => 8}]} = LanceDB.Query.search(table, [8, 1], limit: 1)
    assert {:ok, other} = LanceDB.connect(path)
    assert {:ok, reopened} = LanceDB.Connection.open_table(other, "compact")
    assert {:ok, ^before} = LanceDB.Query.query(reopened, order_by: [asc: :id])

    assert {:ok, %LanceDB.Maintenance.Compaction{fragments_removed: 0}} =
             LanceDB.Maintenance.compact_files(table)
  end

  test("cleanup retains recent history by default and prunes obsolete versions explicitly", %{
    conn: conn,
    path: path
  }) do
    {:ok, table} = LanceDB.Connection.create_table(conn, "cleanup", [%{id: 1}])
    assert {:ok, %{inserted: 1}} = LanceDB.Table.add(table, [%{id: 2}])
    assert {:ok, %{operation: :delete}} = LanceDB.Table.delete(table, "id = 1")
    assert {:ok, _} = LanceDB.Maintenance.compact_files(table)

    assert {:ok, %{"old_versions" => 0, "bytes_removed" => 0}} =
             LanceDB.Maintenance.cleanup_old_versions(table)

    assert {:ok, stats} = LanceDB.Maintenance.cleanup_old_versions(table, older_than_seconds: 0)
    assert stats["old_versions"] >= 2
    assert stats["bytes_removed"] > 0
    assert stats["data_files_removed"] > 0
    assert is_integer(stats["transaction_files_removed"])
    assert is_integer(stats["index_files_removed"])
    assert is_integer(stats["deletion_files_removed"])
    assert {:ok, [%{"id" => 2}]} = LanceDB.Query.query(table)
    assert {:ok, other} = LanceDB.connect(path)
    assert {:ok, reopened} = LanceDB.Connection.open_table(other, "cleanup")
    assert {:ok, [%{"id" => 2}]} = LanceDB.Query.query(reopened)

    assert {:ok, %{"old_versions" => 0}} =
             LanceDB.Maintenance.cleanup_old_versions(table, older_than_seconds: 0)

    assert {:ok, %{inserted: 1}} = LanceDB.Table.add(table, [%{id: 3}])
    assert {:ok, 2} = LanceDB.Table.count_rows(table)
  end

  test("maintenance handles empty and unindexed tables", %{conn: conn}) do
    {:ok, table} = LanceDB.Connection.create_empty_table(conn, "empty", id: :int64)
    assert {:ok, :optimized} = LanceDB.Maintenance.optimize_indices(table)
    assert {:ok, []} = LanceDB.Index.list(table)

    assert {:ok, %LanceDB.Maintenance.Compaction{fragments_removed: 0}} =
             LanceDB.Maintenance.compact_files(table)

    assert {:ok, %{"old_versions" => 0}} = LanceDB.Maintenance.cleanup_old_versions(table)
    assert {:ok, []} = LanceDB.Query.query(table)
  end

  test "invalid maintenance options fail before native execution" do
    for value <- [0, -1, 1.5, nil, "100", 4_294_967_296] do
      assert_raise ArgumentError, fn ->
        LanceDB.Maintenance.compact_files(:unused, target_rows_per_fragment: value)
      end
    end

    for value <- [-1, 0.5, nil, "7d", 31_536_000_001] do
      assert_raise ArgumentError, fn ->
        LanceDB.Maintenance.cleanup_old_versions(:unused, older_than_seconds: value)
      end
    end

    assert_raise ArgumentError, fn -> LanceDB.Maintenance.compact_files(:unused, typo: 1) end

    assert_raise ArgumentError, fn ->
      LanceDB.Maintenance.cleanup_old_versions(:unused, delete_unverified: :invalid)
    end
  end
end
