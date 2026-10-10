defmodule LanceDB.OpenTest do
  use LanceDB.DataCase, async: true

  setup %{path: path} do
    {:ok, conn} = LanceDB.connect(path, %{}, read_consistency_interval_ms: 0)
    {:ok, table} = LanceDB.Connection.create_table(conn, "docs", [%{id: 1}])
    %{conn: conn, table: table}
  end

  test "opens main and branch snapshots without changing other handles", %{
    conn: conn,
    table: table
  } do
    {:ok, initial} = LanceDB.History.version(table)
    {:ok, %{inserted: 1}} = LanceDB.Table.add(table, [%{id: 2}])
    assert {:ok, snapshot} = LanceDB.Connection.open_table(conn, "docs", version: initial)
    assert {:ok, 1} = LanceDB.Table.count_rows(snapshot)
    assert {:ok, 2} = LanceDB.Table.count_rows(table)
    assert {:error, _} = LanceDB.Table.add(snapshot, [%{id: 3}])
    {:ok, branch} = LanceDB.Branch.create(table, "branch")
    {:ok, version} = LanceDB.History.version(branch)
    {:ok, %{inserted: 1}} = LanceDB.Table.add(branch, [%{id: 3}])

    assert {:ok, branch_snapshot} =
             LanceDB.Connection.open_table(conn, "docs", branch: "branch", version: version)

    assert {:ok, 2} = LanceDB.Table.count_rows(branch_snapshot)
    assert {:ok, branch_snapshot} = LanceDB.History.latest(branch_snapshot)
    assert {:ok, 3} = LanceDB.Table.count_rows(branch_snapshot)
    assert {:ok, "branch"} = LanceDB.Branch.current(branch_snapshot)
    assert {:ok, 2} = LanceDB.Table.count_rows(table)
    assert {:error, _} = LanceDB.Connection.open_table(conn, "docs", branch: "missing")
    assert {:error, _} = LanceDB.Connection.open_table(conn, "docs", version: 999_999)
    assert {:error, _} = LanceDB.Connection.open_table(conn, "../invalid")
  end

  test "custom cache sessions preserve connection read consistency and location", %{
    conn: conn,
    table: table
  } do
    {:ok, uri} = LanceDB.Table.uri(table)

    for opts <- [
          [index_cache_size_bytes: 0],
          [metadata_cache_size_bytes: 0],
          [index_cache_size_bytes: 1024, metadata_cache_size_bytes: 2048]
        ] do
      assert {:ok, opened} =
               LanceDB.Connection.open_table(
                 conn,
                 "alias",
                 opts ++ [location: uri, storage_options: %{}]
               )

      assert {:ok, 1} = LanceDB.Table.count_rows(opened)
      assert {:ok, %{updated: 1}} = LanceDB.Table.update(table, %{id: 2})
      assert {:ok, [%{"id" => 2}]} = LanceDB.Query.query(opened)
      assert {:ok, %{updated: 1}} = LanceDB.Table.update(table, %{id: 1})
    end

    assert {:ok, opened} = LanceDB.Connection.open_table(conn, "docs", branch: "main")
    assert {:ok, "main"} = LanceDB.Branch.current(opened)
  end

  test "table names have exclusive lexicographic pagination", %{conn: conn} do
    for name <- ["a", "z", "m"] do
      {:ok, _} = LanceDB.Connection.create_empty_table(conn, name, id: :int64)
    end

    assert {:ok, ["a", "docs", "m", "z"]} = LanceDB.Connection.table_names(conn)
    assert {:ok, ["a", "docs"]} = LanceDB.Connection.table_names(conn, limit: 2)
    assert {:ok, ["m", "z"]} = LanceDB.Connection.table_names(conn, start_after: "docs", limit: 2)
    assert {:ok, []} = LanceDB.Connection.table_names(conn, start_after: "z")
  end

  test "invalid open and listing options fail before native calls" do
    for opts <- [
          [branch: ""],
          [version: nil],
          [version: 0],
          [location: <<255>>],
          [index_cache_size_bytes: -1],
          [metadata_cache_size_bytes: 1.5],
          [metadata_cache_size_bytes: nil],
          [storage_options: []],
          [unknown: true]
        ] do
      assert_raise ArgumentError, fn -> LanceDB.Connection.open_table(:unused, "docs", opts) end
    end

    for opts <- [[limit: nil], [limit: 0], [limit: -1], [start_after: ""], [unknown: true]] do
      assert_raise ArgumentError, fn -> LanceDB.Connection.table_names(:unused, opts) end
    end
  end
end
