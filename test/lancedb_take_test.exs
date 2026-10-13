defmodule LanceDB.TakeTest do
  use LanceDB.DataCase, async: true
  use ExUnitProperties

  setup %{path: path} do
    {:ok, db} = LanceDB.connect(path)

    {:ok, table} =
      LanceDB.Connection.create_empty_table(db, "items",
        id: :int64,
        at: {:timestamp, :nanosecond, "UTC"}
      )

    {:ok, %{inserted: 125}} =
      LanceDB.Table.add(
        table,
        for id <- 1..125 do
          %{id: id, at: id}
        end
      )

    %{table: table}
  end

  test("retrieves full records using search/scan row IDs with no default limit", %{table: table}) do
    {:ok, rows} = LanceDB.Query.query(table, columns: [:id], with_row_id: true, limit: 200)
    ids = Enum.map(rows, & &1["_rowid"])
    assert {:ok, all} = LanceDB.Query.take_row_ids(table, ids, with_row_id: true)
    assert length(all) == 125
    assert Enum.all?(all, &is_binary(&1["at"]))
    wanted = Enum.take(ids, 3)

    assert {:ok, selected} =
             LanceDB.Query.take_row_ids(table, wanted ++ wanted ++ [18_446_744_073_709_551_615],
               columns: [:id]
             )

    assert length(selected) == 3
    assert Enum.all?(selected, &(Map.keys(&1) == ["id"]))
    assert {:ok, []} = LanceDB.Query.take_row_ids(table, [])
    assert {:ok, []} = LanceDB.Query.take_offsets(table, [])
    assert {:ok, []} = LanceDB.Query.take_offsets(table, [999])
    assert {:ok, _} = LanceDB.TestSupport.explain_plan(table, row_ids: ids, columns: [:id])

    assert Enum.to_list(
             LanceDB.TestSupport.rows(table, row_ids: ids, with_row_id: true, batch_size: 7)
           ) == all
  end

  test("offsets reflect deletions and historical checkout", %{table: table}) do
    {:ok, version} = LanceDB.History.version(table)
    {:ok, [%{"_rowid" => row_id}]} = LanceDB.Query.take_offsets(table, [0], with_row_id: true)
    assert {:ok, [%{"id" => 1}]} = LanceDB.Query.take_offsets(table, [0], columns: [:id])
    assert {:ok, %{operation: :delete}} = LanceDB.Table.delete(table, "id = 1")
    assert {:ok, []} = LanceDB.Query.take_row_ids(table, [row_id])
    assert {:ok, [%{"id" => 2}]} = LanceDB.Query.take_offsets(table, [0], columns: [:id])
    assert {:ok, table} = LanceDB.History.snapshot(table, version)
    assert {:ok, [%{"id" => 1}]} = LanceDB.Query.take_row_ids(table, [row_id], columns: [:id])
    assert {:ok, [%{"id" => 1}]} = LanceDB.Query.take_offsets(table, [0], columns: [:id])
  end

  test("take supports computed projection, streams, metadata and errors", %{table: table}) do
    offsets = [8, 2, 5, 2]
    opts = [select: [item: "id", total: "id * 2", position: "_rowoffset"], with_row_id: true]
    assert {:ok, rows} = LanceDB.Query.take_offsets(table, offsets, opts)
    assert Enum.sort(Enum.map(rows, & &1["item"])) == [3, 3, 6, 9]
    assert Enum.all?(rows, &(&1["total"] == 2 * &1["item"] and &1["position"] == &1["item"] - 1))

    assert Enum.to_list(
             LanceDB.TestSupport.rows(
               table,
               [row_offsets: offsets, batch_size: 1] ++ opts
             )
           ) == rows

    assert {:ok, _} = LanceDB.TestSupport.explain_plan(table, [row_offsets: offsets] ++ opts)
    assert {:error, _} = LanceDB.Query.take_offsets(table, [0], columns: [:missing])

    assert_raise LanceDB.Error, fn ->
      LanceDB.TestSupport.rows(table, row_offsets: [0], columns: [:missing]) |> Enum.to_list()
    end
  end

  test "retrieval validates identifiers, options and selector combinations" do
    for ids <- [nil, 1, [-1], [1.0], ["1"], [18_446_744_073_709_551_616]] do
      assert_raise ArgumentError, fn ->
        LanceDB.Query.take_offsets(%LanceDB.Table{resource: make_ref()}, ids)
      end

      assert_raise ArgumentError, fn ->
        LanceDB.Query.take_row_ids(%LanceDB.Table{resource: make_ref()}, ids)
      end
    end

    for opts <- [[filter: "id > 0"], [limit: 10], [with_row_id: nil], [timeout_ms: 0]] do
      assert_raise ArgumentError, fn ->
        LanceDB.Query.take_offsets(%LanceDB.Table{resource: make_ref()}, [0], opts)
      end
    end

    for opts <- [
          [row_ids: [], row_offsets: []],
          [row_ids: [], vector: [1, 0]],
          [row_offsets: [], text: "x"]
        ] do
      assert_raise ArgumentError, fn -> LanceDB.TestSupport.rows(:unused, opts) end
      assert_raise ArgumentError, fn -> LanceDB.TestSupport.explain_plan(:unused, opts) end
    end
  end

  property(
    "offset retrieval preserves the requested occurrence counts",
    %{table: table}
  ) do
    check(all(offsets <- list_of(integer(0..124), max_length: 30), max_runs: 20)) do
      assert {:ok, rows} = LanceDB.Query.take_offsets(table, offsets, columns: [:id])

      assert Enum.sort(Enum.map(rows, & &1["id"])) ==
               offsets |> Enum.map(&(&1 + 1)) |> Enum.sort()
    end
  end
end
