defmodule LanceDB.ExecutionTest do
  use LanceDB.DataCase, async: true

  setup %{path: path} do
    {:ok, conn} = LanceDB.connect(path)

    {:ok, table} =
      LanceDB.Connection.create_table(conn, "docs", [
        %{id: 1, text: "elixir concurrency", vector: [1, 0]},
        %{id: 2, text: "rust memory", vector: [0, 1]}
      ])

    %{table: table}
  end

  test("all query types preserve results with optional deadlines", %{table: table}) do
    for run <- query_functions(table) do
      assert {:ok, expected} = run.([])

      for timeout <- [nil, 10_000, 4_294_967_295] do
        assert {:ok, ^expected} = run.(timeout_ms: timeout)
      end

      assert {:error, reason} = run.(filter: "id =", timeout_ms: 10_000)
      assert is_struct(reason, LanceDB.Error)
    end
  end

  test("rejects invalid deadlines consistently for execution and planning", %{table: table}) do
    for run <- query_functions(table) ++ plan_functions(table),
        timeout <- [0, -1, 1.0, true, "100", :infinity, 4_294_967_296] do
      assert_raise ArgumentError, ~r/timeout_ms/, fn -> run.(timeout_ms: timeout) end
    end
  end

  test("ordinary plans use filters, ordering, offset and projection", %{table: table}) do
    opts = [filter: "id > 0", columns: [:text], order_by: [desc: :id], limit: 1, offset: 1]
    assert {:ok, plan} = LanceDB.TestSupport.explain_plan(table, opts)
    assert plan =~ "Sort"
    assert plan =~ "id"
    assert {:ok, [%{"text" => "elixir concurrency"}]} = LanceDB.Query.query(table, opts)
    assert {:ok, default_plan} = LanceDB.TestSupport.explain_plan(table)
    assert is_binary(default_plan)
    assert String.trim(default_plan) != ""
  end

  test("vector plans distinguish indexed and exhaustive searches", %{table: table}) do
    assert {:ok, :created} =
             LanceDB.Index.create(table, :vector, type: :ivf_flat, num_partitions: 1)

    opts = [vector: [1, 0], vector_column: :vector, columns: [:id], nprobes: 1, limit: 1]
    assert {:ok, indexed} = LanceDB.TestSupport.explain_plan(table, opts)
    assert indexed =~ ~r/ANN|Ivf/i

    assert {:ok, exhaustive} =
             LanceDB.TestSupport.explain_plan(
               table,
               opts ++ [bypass_vector_index: true]
             )

    refute exhaustive =~ ~r/ANN|Ivf/i
    assert exhaustive != indexed
  end

  test("text and hybrid plans describe actual channels and fusion", %{table: table}) do
    assert {:ok, :created} = LanceDB.Index.create(table, :text, type: :fts)
    opts = [text: "elixir", text_column: :text, columns: [:id], limit: 1]
    assert {:ok, text} = LanceDB.TestSupport.explain_plan(table, opts)
    assert text =~ ~r/fts|full.?text|match/i

    assert {:ok, hybrid} =
             LanceDB.TestSupport.explain_plan(
               table,
               opts ++ [vector: [1, 0], rrf_k: 10]
             )

    assert hybrid =~ "Vector channel:"
    assert hybrid =~ "Full-text channel:"
    assert hybrid =~ "Fusion: reciprocal rank fusion (rrf_k=10, limit=1)"
    assert hybrid =~ "_relevance_score"
  end

  test("verbose plans and deadlines work for each query type and preserve SDK errors", %{
    table: table
  }) do
    for run <- plan_functions(table) do
      assert {:ok, plan} = run.(verbose: true, timeout_ms: 10_000)
      assert is_binary(plan)
      assert String.trim(plan) != ""
      assert {:error, reason} = run.(filter: "id =")
      assert is_struct(reason, LanceDB.Error)
    end
  end

  test("explaining does not evaluate expressions against result rows", %{table: table}) do
    opts = [filter: "CAST(text AS BIGINT) > 0"]

    for run <- plan_functions(table) do
      assert {:ok, plan} = run.(opts)
      assert is_binary(plan)
    end

    for run <- query_functions(table) do
      assert {:error, _} = run.(opts)
    end
  end

  test("planning shares query validation and rejects invalid selectors", %{table: table}) do
    for opts <- [
          [verbose: nil],
          [vector: nil],
          [vector: []],
          [vector: "invalid"],
          [text: nil],
          [text: ""],
          [vector: [1, 0], offset: -1],
          [text: "elixir", distance: :l2],
          [unknown: true]
        ] do
      assert_raise ArgumentError, fn ->
        LanceDB.TestSupport.explain_plan(table, opts)
      end
    end
  end

  defp query_functions(table) do
    [
      &LanceDB.Query.query(table, &1),
      &LanceDB.Query.search(table, [1, 0], &1),
      &LanceDB.Query.full_text_search(table, "elixir", [text_column: :text] ++ &1),
      &LanceDB.Query.hybrid_search(table, [1, 0], "elixir", [text_column: :text] ++ &1)
    ]
  end

  defp plan_functions(table) do
    for selectors <- [
          [],
          [vector: [1, 0]],
          [text: "elixir", text_column: :text],
          [vector: [1, 0], text: "elixir", text_column: :text]
        ] do
      &LanceDB.TestSupport.explain_plan(table, selectors ++ &1)
    end
  end
end
