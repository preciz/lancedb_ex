defmodule LanceDB.ProjectionTest do
  use LanceDB.DataCase, async: true
  use ExUnitProperties

  setup %{path: path} do
    {:ok, db} = LanceDB.connect(path)

    {:ok, table} =
      LanceDB.Connection.create_empty_table(db, "items",
        id: :int64,
        price: :float64,
        quantity: :int64,
        text: :string,
        at: {:timestamp, :nanosecond, "UTC"},
        vector: {:vector, 2}
      )

    records =
      for id <- 1..125 do
        %{
          id: id,
          price: id / 2,
          quantity: 3,
          text:
            if id == 2 do
              nil
            else
              "elixir"
            end,
          at: id,
          vector: [id, 1]
        }
      end

    {:ok, %{inserted: 125}} = LanceDB.Table.add(table, records)
    %{table: table, db: db}
  end

  test("computed projections retain filters, ordering, timestamps and nulls", %{table: table}) do
    opts = [
      select: [id: "-id", total: "price * quantity", label: "upper(text)", time: "at"],
      order_by: [asc: :id],
      offset: 1,
      limit: 2,
      filter: "quantity = 3",
      with_row_id: true
    ]

    assert {:ok, [first, second] = eager} = LanceDB.Query.query(table, opts)
    assert first["id"] == -2
    assert first["total"] == 3.0
    assert first["label"] == nil
    assert first["time"] == "1970-01-01T00:00:00.000000002Z"
    assert second["id"] == -3
    assert second["label"] == "ELIXIR"
    assert Map.keys(first) |> Enum.sort() == ["_rowid", "id", "label", "time", "total"]

    assert Enum.to_list(LanceDB.TestSupport.rows(table, [batch_size: 1] ++ opts)) ==
             eager

    assert {:ok, plan} = LanceDB.TestSupport.explain_plan(table, opts)
    assert is_binary(plan)

    assert {:ok, [%{"value" => 42}, %{"value" => 42}]} =
             LanceDB.Query.query(table, select: %{value: "42"}, limit: 2)

    assert {:ok, []} =
             LanceDB.Query.query(table, select: [total: "price * quantity"], filter: "id < 0")

    assert {:error, _} = LanceDB.Query.query(table, select: [bad: "unknown_column + 1"])

    assert_raise LanceDB.Error, fn ->
      LanceDB.TestSupport.rows(table, select: [bad: "unknown_column"])
      |> Enum.to_list()
    end
  end

  test("computed search results preserve ranking metadata and hybrid alignment", %{table: table}) do
    {:ok, :created} = LanceDB.Index.create(table, :text, type: :fts)
    opts = [select: [item: "id", total: "price * quantity"], limit: 3, with_row_id: true]

    for {selectors, query, metadata} <- [
          {[vector: [3, 1]], fn -> LanceDB.Query.search(table, [3, 1], opts) end, "_distance"},
          {[text: "elixir"], fn -> LanceDB.Query.full_text_search(table, "elixir", opts) end,
           "_score"},
          {[vector: [3, 1], text: "elixir"],
           fn -> LanceDB.Query.hybrid_search(table, [3, 1], "elixir", opts) end,
           "_relevance_score"}
        ] do
      assert {:ok, rows} = query.()
      assert length(rows) == 3
      assert Enum.all?(rows, &(&1["total"] == &1["item"] * 1.5))

      assert Enum.all?(
               rows,
               &(Enum.sort(Map.keys(&1)) == Enum.sort(["item", "total", "_rowid", metadata]))
             )

      assert Enum.to_list(LanceDB.TestSupport.rows(table, selectors ++ [batch_size: 1] ++ opts)) ==
               rows

      assert {:ok, _} = LanceDB.TestSupport.explain_plan(table, selectors ++ opts)
    end

    assert {:ok, [_ | _]} =
             LanceDB.Query.hybrid_search(table, [1, 1], "elixir",
               select: [item: "id"],
               distance_range: {100_000, nil},
               limit: 3
             )

    assert {:ok, [_ | _]} =
             LanceDB.Query.hybrid_search(table, [1, 1], "absent", select: [item: "id"])
  end

  test("empty tables, constant search projections and quoted column names", %{
    db: db,
    table: table
  }) do
    {:ok, empty} =
      LanceDB.Connection.create_empty_table(db, "empty",
        id: :int64,
        text: :string,
        vector: {:vector, 2}
      )

    assert {:ok, []} = LanceDB.Query.query(empty, select: [constant: "1"])
    assert {:ok, []} = LanceDB.Query.search(empty, [1, 0], select: [constant: "1"])
    assert {:ok, []} = LanceDB.Query.take_offsets(empty, [0], select: [constant: "1"])

    assert Enum.to_list(LanceDB.TestSupport.rows(empty, select: [constant: "1"])) ==
             []

    {:ok, quoted} =
      LanceDB.Connection.create_empty_table(db, "quoted", [
        {"a b", :int64},
        {"a-b", :int64},
        {"__lancedb_select_0", :int64}
      ])

    {:ok, %{inserted: 1}} =
      LanceDB.Table.add(quoted, [%{"a b" => 3, "a-b" => 4, "__lancedb_select_0" => 5}])

    assert {:ok, [%{"sum" => 7}]} =
             LanceDB.Query.query(quoted, select: [{"sum", "`a b` + `a-b`"}])

    {:ok, :created} = LanceDB.Index.create(table, :text, type: :fts)

    assert {:ok, [%{"constant" => 42, "_distance" => _}]} =
             LanceDB.Query.search(table, [1, 1], select: [constant: "42"], limit: 1)

    assert {:ok, [%{"constant" => 42, "_score" => _}]} =
             LanceDB.Query.full_text_search(table, "elixir", select: [constant: "42"], limit: 1)

    assert {:ok, [%{"constant" => 42, "_relevance_score" => _} = result]} =
             LanceDB.Query.hybrid_search(table, [1, 1], "elixir",
               select: [constant: "42"],
               limit: 1
             )

    refute Map.has_key?(result, "_rowid")

    assert {:ok, [result]} =
             LanceDB.Query.hybrid_search(table, [1, 1], "elixir",
               columns: [:id, :_rowid],
               limit: 1
             )

    refute Map.has_key?(result, "_rowid")
  end

  test("select validates before execution", %{table: table}) do
    for select <- [
          [],
          %{},
          [id: 1],
          [nil: "id"],
          [id: ""],
          [id: <<255>>],
          [id: "id", id: "quantity"],
          [{"id", "id"}, id: "quantity"],
          [:_bad],
          [nil],
          :id,
          ~D[2026-01-01],
          [:_rowid | []]
        ] do
      assert_raise ArgumentError, fn -> LanceDB.Query.query(table, select: select) end
    end

    for name <- ~w(_rowid _rowoffset _rowaddr _distance _score _relevance_score query_index) do
      assert_raise ArgumentError, fn -> LanceDB.Query.query(table, select: [{name, "id"}]) end
    end

    assert_raise ArgumentError, fn ->
      LanceDB.Query.query(table, select: [item: "id"], columns: [:id])
    end

    assert {:ok, [_]} = LanceDB.Query.query(table, select: nil, columns: [:id], limit: 1)
  end

  property("computed values and batching match Elixir arithmetic", %{table: table}) do
    check(all(factor <- integer(-10..10), batch <- integer(1..20), max_runs: 15)) do
      opts = [select: [result: "id * #{factor}"], filter: "id <= 20", order_by: [asc: :id]]

      expected =
        for id <- 1..20 do
          %{"result" => id * factor}
        end

      assert {:ok, ^expected} = LanceDB.Query.query(table, opts)

      assert Enum.to_list(LanceDB.TestSupport.rows(table, [batch_size: batch] ++ opts)) ==
               expected
    end
  end
end
