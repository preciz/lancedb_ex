defmodule LanceDB.DiagnosticsTest do
  use LanceDB.DataCase, async: true

  setup %{path: path} do
    {:ok, db} = LanceDB.connect(path)

    {:ok, table} =
      LanceDB.Connection.create_empty_table(db, "docs",
        id: :int64,
        text: :string,
        vector: {:vector, 2},
        at: {:timestamp, :nanosecond, "UTC"}
      )

    {:ok, %{inserted: 3}} =
      LanceDB.Table.add(table, [
        %{id: 1, text: "elixir concurrency", vector: [1, 0], at: 1},
        %{id: 2, text: "elixir processes", vector: [0, 1], at: 2},
        %{id: 3, text: "rust memory", vector: [1, 1], at: 3}
      ])

    {:ok, :created} = LanceDB.Index.create(table, :text, type: :fts)
    %{db: db, table: table}
  end

  test("analysis executes every query type with runtime metrics and final counts", %{table: table}) do
    {:ok, [%{"_rowid" => row_id}]} = LanceDB.Query.take_offsets(table, [0], with_row_id: true)

    for selectors <- [
          [],
          [vector: [1, 0]],
          [text: "elixir"],
          [row_ids: [row_id]],
          [row_offsets: [0, 2]]
        ] do
      opts = selectors ++ [select: [computed: "id * 2"], timeout_ms: 10_000]
      assert {:ok, analysis} = LanceDB.TestSupport.analyze_plan(table, opts)
      assert analysis =~ "metrics=["
      assert analysis =~ "Execution: elapsed_ms="
      assert analysis =~ "Plan creation: elapsed_ms="
      expected = LanceDB.TestSupport.rows(table, opts) |> Enum.count()
      assert analysis =~ "Output: rows=#{expected}
"
    end

    assert {:ok, analysis} =
             LanceDB.TestSupport.analyze_plan(table,
               limit: 1,
               offset: 1,
               select: [id: "-id"],
               order_by: [asc: :id]
             )

    assert analysis =~ "Output: rows=1\n"
    assert {:ok, version} = LanceDB.History.version(table)
    assert {:ok, _} = LanceDB.TestSupport.analyze_plan(table)
    assert {:ok, ^version} = LanceDB.History.version(table)
  end

  test("hybrid analysis runs channels and fusion, including one or two empty channels", %{
    table: table
  }) do
    for opts <- [
          [limit: 2, offset: 1],
          [text: "missing"],
          [distance_range: {1_000_000, nil}],
          [filter: "id < 0"]
        ] do
      selectors = Keyword.merge([vector: [1, 0], text: "elixir", select: [item: "id"]], opts)
      expected = LanceDB.TestSupport.rows(table, selectors) |> Enum.count()
      assert {:ok, analysis} = LanceDB.TestSupport.analyze_plan(table, selectors)
      assert analysis =~ "Vector channel:"
      assert analysis =~ "Full-text channel:"
      assert analysis =~ "Fusion and projection: elapsed_ms="
      assert String.ends_with?(analysis, "Output: rows=#{expected}
")
    end
  end

  test(
    "schema inspection does not execute row expressions; analysis propagates execution failures",
    %{table: table}
  ) do
    for selectors <- [
          [],
          [vector: [1, 0]],
          [text: "elixir"],
          [vector: [1, 0], text: "elixir"],
          [row_offsets: [0]]
        ] do
      opts = selectors ++ [select: [number: "CAST(text AS BIGINT)"]]

      assert {:ok, %LanceDB.ArrowSchema{fields: fields}} =
               LanceDB.TestSupport.output_schema(table, opts)

      assert %LanceDB.Field{name: "number", type: :int64, nullable: true} in fields
      assert {:error, reason} = LanceDB.TestSupport.analyze_plan(table, opts)
      assert is_struct(reason, LanceDB.Error)
    end

    assert {:error, _} = LanceDB.TestSupport.output_schema(table, select: [bad: "missing_column"])
    assert {:error, _} = LanceDB.TestSupport.analyze_plan(table, filter: "id =")
  end

  test("schemas describe public aliases and metadata for all query types", %{table: table}) do
    for {selectors, metadata} <- [
          {[], []},
          {[vector: [1, 0]], ["_distance"]},
          {[text: "elixir"], ["_score"]},
          {[vector: [1, 0], text: "elixir"], ["_relevance_score"]},
          {[row_ids: []], []},
          {[row_offsets: [0]], []}
        ] do
      for with_row_id <- [false, true] do
        opts = selectors ++ [select: [item: "id", moment: "at"], with_row_id: with_row_id]

        assert {:ok, %LanceDB.ArrowSchema{fields: fields}} =
                 LanceDB.TestSupport.output_schema(table, opts)

        names = Enum.map(fields, & &1.name)

        expected =
          ["item", "moment"] ++
            metadata ++
            if with_row_id do
              ["_rowid"]
            else
              []
            end

        assert names == expected
        moment = Enum.find(fields, &(&1.name == "moment"))
        assert moment.type == {:timestamp, :nanosecond, "UTC"}

        for row <- Enum.to_list(LanceDB.TestSupport.rows(table, opts)) do
          assert Enum.sort(Map.keys(row)) == Enum.sort(names)
        end
      end
    end

    assert {:ok, %LanceDB.ArrowSchema{fields: [%LanceDB.Field{name: "total", type: :int64}]}} =
             LanceDB.TestSupport.output_schema(table,
               select: [total: "id * 2"],
               order_by: [desc: :text]
             )

    assert {:ok, schema} = LanceDB.Table.schema(table)

    assert {:ok, ^schema} =
             LanceDB.TestSupport.logical_schema(LanceDB.TestSupport.output_schema(table))
  end

  test("schemas remain available for empty tables and analysis returns zero rows", %{db: db}) do
    {:ok, empty} =
      LanceDB.Connection.create_empty_table(db, "empty",
        id: :int64,
        text: :string,
        vector: {:vector, 2}
      )

    {:ok, :created} = LanceDB.Index.create(empty, :text, type: :fts)

    for selectors <- [
          [],
          [vector: [1, 0]],
          [text: "elixir"],
          [vector: [1, 0], text: "elixir"],
          [row_offsets: []]
        ] do
      opts = selectors ++ [select: [item: "id"]]

      assert {:ok, %LanceDB.ArrowSchema{fields: [_ | _]}} =
               LanceDB.TestSupport.output_schema(empty, opts)

      assert {:ok, analysis} = LanceDB.TestSupport.analyze_plan(empty, opts)
      assert String.ends_with?(analysis, "Output: rows=0\n")
    end
  end

  test "diagnostics share validation and reject unsupported options" do
    for run <- [&LanceDB.TestSupport.analyze_plan/2, &LanceDB.TestSupport.output_schema/2],
        opts <- [
          [verbose: true],
          [timeout_ms: 0],
          [columns: []],
          [select: []],
          [vector: []],
          [limit: 0],
          [row_offsets: [-1]],
          [row_ids: [], text: "elixir"],
          [row_ids: [], row_offsets: []],
          [batch_size: 1],
          [typo: true]
        ] do
      assert_raise ArgumentError, fn -> run.(:unused, opts) end
    end
  end
end
