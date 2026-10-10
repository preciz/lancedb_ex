defmodule LanceDB.ConsistencyTest do
  use LanceDB.DataCase, async: true

  setup %{path: path} do
    {:ok, conn} = LanceDB.connect(path)
    {:ok, writer} = LanceDB.Connection.create_table(conn, "docs", [%{id: 1}])
    %{path: path, writer: writer}
  end

  test "default and nil consistency retain the reader snapshot until explicitly refreshed", %{
    path: path,
    writer: writer
  } do
    for opts <- [[], [read_consistency_interval_ms: nil]] do
      {:ok, conn} = LanceDB.connect(path, %{}, opts)
      {:ok, reader} = LanceDB.Connection.open_table(conn, "docs")
      {:ok, before} = LanceDB.Table.count_rows(reader)
      assert {:ok, %{inserted: 1}} = LanceDB.Table.add(writer, [%{id: before + 1}])
      assert {:ok, ^before} = LanceDB.Table.count_rows(reader)
      assert {:ok, reader} = LanceDB.History.latest(reader)
      assert {:ok, after_refresh} = LanceDB.Table.count_rows(reader)
      assert after_refresh == before + 1
    end
  end

  test "zero consistency refreshes independent readers but preserves checked-out snapshots", %{
    path: path,
    writer: writer
  } do
    {:ok, conn} = LanceDB.connect(path, %{}, read_consistency_interval_ms: 0)
    {:ok, reader} = LanceDB.Connection.open_table(conn, "docs")
    {:ok, first} = LanceDB.History.version(reader)
    assert {:ok, %{inserted: 1}} = LanceDB.Table.add(writer, [%{id: 2}])
    assert {:ok, [%{"id" => 2}]} = LanceDB.Query.query(reader, filter: "id = 2")
    assert {:ok, reader} = LanceDB.History.snapshot(reader, first)
    assert {:ok, %{inserted: 1}} = LanceDB.Table.add(writer, [%{id: 3}])
    assert {:ok, [%{"id" => 1}]} = LanceDB.Query.query(reader)
    assert {:error, _} = LanceDB.Table.add(reader, [%{id: 4}])
    assert {:ok, reader} = LanceDB.History.latest(reader)
    assert {:ok, 3} = LanceDB.Table.count_rows(reader)
  end

  test "newly created tables inherit the connection's consistency setting", %{path: path} do
    {:ok, conn} = LanceDB.connect(path, %{}, read_consistency_interval_ms: 0)
    {:ok, reader} = LanceDB.Connection.create_table(conn, "created", [%{id: 1}])
    {:ok, other} = LanceDB.connect(path)
    {:ok, writer} = LanceDB.Connection.open_table(other, "created")
    assert {:ok, %{inserted: 1}} = LanceDB.Table.add(writer, [%{id: 2}])
    assert {:ok, 2} = LanceDB.Table.count_rows(reader)
  end

  test "positive intervals refresh on a later read", %{path: path, writer: writer} do
    {:ok, conn} = LanceDB.connect(path, %{}, read_consistency_interval_ms: 20)
    {:ok, reader} = LanceDB.Connection.open_table(conn, "docs")
    assert {:ok, 1} = LanceDB.Table.count_rows(reader)
    assert {:ok, %{inserted: 1}} = LanceDB.Table.add(writer, [%{id: 2}])
    assert_eventually(fn -> LanceDB.Table.count_rows(reader) == {:ok, 2} end)
  end

  test "rejects invalid consistency options before connecting", %{path: path} do
    for interval <- [-1, 1.5, false, "100", :infinity, 4_294_967_296] do
      assert_raise ArgumentError, ~r/read_consistency_interval_ms/, fn ->
        LanceDB.connect(path, %{}, read_consistency_interval_ms: interval)
      end
    end

    assert_raise ArgumentError, fn -> LanceDB.connect(path, %{}, unknown: true) end
  end
end
