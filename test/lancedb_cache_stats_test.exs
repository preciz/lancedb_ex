defmodule LanceDB.CacheStatsTest do
  use LanceDB.DataCase, async: true

  setup %{path: path} do
    {:ok, conn} = LanceDB.connect(path)
    {:ok, table} = LanceDB.Connection.create_table(conn, "docs", for(id <- 1..20, do: %{id: id}))
    {:ok, :created} = LanceDB.Index.create(table, :id, type: :btree, name: "ids")
    %{conn: conn, table: table}
  end

  test "reports shared session usage without resetting counters", %{conn: conn, table: table} do
    {:ok, other} = LanceDB.Connection.create_table(conn, "other", [%{id: 1}])
    {:ok, reopened} = LanceDB.Connection.open_table(conn, "docs")
    {:ok, :prewarmed} = LanceDB.Index.prewarm(table, "ids")
    {:ok, [_]} = LanceDB.Query.query(table, filter: "id = 1")

    assert {:ok, stats} = LanceDB.Table.cache_stats(table)
    assert stats["scope"] == "session"
    assert Enum.sort(Map.keys(stats)) == ["index", "metadata", "scope"]

    for name <- ["index", "metadata"] do
      cache = stats[name]

      assert Enum.sort(Map.keys(cache)) ==
               ["capacity_bytes", "hits", "misses", "num_entries", "size_bytes"]

      assert cache["capacity_bytes"] == nil

      for field <- ["hits", "misses", "num_entries", "size_bytes"] do
        assert is_integer(cache[field]) and cache[field] >= 0
      end
    end

    assert stats["index"]["num_entries"] > 0
    assert stats["index"]["size_bytes"] > 0
    assert stats["index"]["hits"] > 0
    assert stats["index"]["misses"] > 0
    assert {:ok, ^stats} = LanceDB.Table.cache_stats(table)
    assert {:ok, ^stats} = LanceDB.Table.cache_stats(reopened)
    assert {:ok, ^stats} = LanceDB.Table.cache_stats(other)
  end

  test "reports actual sessions for isolated opens, snapshots and branches", %{conn: conn} do
    opts = [index_cache_size_bytes: 16 * 1024 * 1024, metadata_cache_size_bytes: 1024 * 1024]
    {:ok, first} = LanceDB.Connection.open_table(conn, "docs", opts)
    {:ok, second} = LanceDB.Connection.open_table(conn, "docs", opts)
    {:ok, second_before} = LanceDB.Table.cache_stats(second)
    {:ok, :prewarmed} = LanceDB.Index.prewarm(first, "ids")
    assert {:ok, first_stats} = LanceDB.Table.cache_stats(first)
    assert first_stats["index"]["num_entries"] > 0
    assert {:ok, ^second_before} = LanceDB.Table.cache_stats(second)

    {:ok, snapshot} = LanceDB.History.snapshot(first, 1)
    {:ok, branch} = LanceDB.Branch.create(first, "experiment")
    {:ok, checkout} = LanceDB.Branch.open(first, "experiment")
    {:ok, stats} = LanceDB.Table.cache_stats(first)
    assert {:ok, ^stats} = LanceDB.Table.cache_stats(snapshot)
    assert {:ok, ^stats} = LanceDB.Table.cache_stats(checkout)
    assert {:ok, branch_stats} = LanceDB.Table.cache_stats(branch)
    assert branch_stats["index"]["num_entries"] == 0
  end

  test "zero-budget sessions retain no entries or bytes", %{conn: conn} do
    {:ok, table} =
      LanceDB.Connection.open_table(conn, "docs",
        index_cache_size_bytes: 0,
        metadata_cache_size_bytes: 0
      )

    {:ok, :prewarmed} = LanceDB.Index.prewarm(table, "ids")
    assert {:ok, [%{"id" => 1}]} = LanceDB.Query.query(table, filter: "id = 1")
    assert {:ok, stats} = LanceDB.Table.cache_stats(table)

    for name <- ["index", "metadata"] do
      assert stats[name]["num_entries"] == 0
      assert stats[name]["size_bytes"] == 0
    end
  end
end
