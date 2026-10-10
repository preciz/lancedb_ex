defmodule LanceDB.RemainingSearchControlsTest do
  use LanceDB.DataCase, async: true

  setup %{path: path} do
    {:ok, conn} = LanceDB.connect(path)

    rows =
      for id <- 1..8 do
        %{
          id: id,
          vector: [id, 0],
          text: String.duplicate("needle ", 10 - id) <> String.duplicate("other ", id)
        }
      end

    {:ok, table} = LanceDB.Connection.create_table(conn, "docs", rows)
    %{table: table}
  end

  test("postfiltering can return fewer matches than prefiltering", %{table: table}) do
    assert {:ok, [%{"id" => 3}]} =
             LanceDB.Query.search(table, [0, 0], limit: 1, filter: "id > 2", prefilter: true)

    assert {:ok, []} =
             LanceDB.Query.search(table, [0, 0], limit: 1, filter: "id > 2", prefilter: false)

    assert {:ok, [%{"id" => 3, "_relevance_score" => score}]} =
             LanceDB.Query.hybrid_search(table, [0, 0], "needle",
               text_column: :text,
               limit: 1,
               filter: "id > 2",
               prefilter: false
             )

    assert_in_delta score, 1 / 60, 1.0e-7
  end

  test("distance ranges use inclusive lower and exclusive upper float32 bounds", %{table: table}) do
    for indexed <- [false, true] do
      if indexed do
        assert {:ok, :created} =
                 LanceDB.Index.create(table, :vector, type: :ivf_flat, num_partitions: 1)
      end

      assert {:ok, [%{"id" => 1, "_distance" => 1.0}]} =
               LanceDB.Query.search(table, [0, 0], distance_range: {1, 4})

      assert {:ok, [%{"id" => 1}]} = LanceDB.Query.search(table, [0, 0], distance_range: {nil, 4})

      assert {:ok, [%{"id" => 8}]} =
               LanceDB.Query.search(table, [0, 0], distance_range: {64, nil})

      assert {:ok, []} = LanceDB.Query.search(table, [0, 0], distance_range: {65, nil})
      assert {:ok, rows} = LanceDB.Query.search(table, [0, 0], distance_range: {nil, nil})
      assert length(rows) == 8
    end
  end

  test("distance bounds apply to the hybrid vector channel while text can still match", %{
    table: table
  }) do
    assert {:ok, rows} =
             LanceDB.Query.hybrid_search(table, [0, 0], "needle",
               text_column: :text,
               distance_range: {65, nil},
               columns: [:id],
               limit: 8
             )

    assert Enum.map(rows, & &1["id"]) == Enum.to_list(1..8)
  end

  test("adaptive probes accept small, large and unbounded caps", %{table: table}) do
    assert {:ok, :created} =
             LanceDB.Index.create(table, :vector, type: :ivf_flat, num_partitions: 1)

    for opts <- [
          [minimum_nprobes: 1, maximum_nprobes: 2],
          [minimum_nprobes: 25, maximum_nprobes: 30],
          [minimum_nprobes: 1, maximum_nprobes: nil],
          [minimum_nprobes: 1],
          [maximum_nprobes: nil],
          [maximum_nprobes: 30]
        ] do
      assert {:ok, [%{"id" => 1}]} = LanceDB.Query.search(table, [0, 0], opts ++ [limit: 1])

      assert {:ok, plan} =
               LanceDB.TestSupport.explain_plan(
                 table,
                 opts ++ [vector: [0, 0], limit: 1]
               )

      assert plan =~ ~r/ANN|Ivf/i
    end
  end

  test("adaptive probing expands the search when initial partitions have no filtered matches", %{
    table: table
  }) do
    rows =
      for id <- 9..136 do
        %{id: id, vector: [1000 + id / 1000, 0], text: "far"}
      end

    assert {:ok, %{inserted: 128}} = LanceDB.Table.add(table, rows)

    assert {:ok, :created} =
             LanceDB.Index.create(table, :vector, type: :ivf_flat, num_partitions: 2)

    opts = [filter: "id >= 9", minimum_nprobes: 1, limit: 1]
    assert {:ok, []} = LanceDB.Query.search(table, [0, 0], opts ++ [maximum_nprobes: 1])

    assert {:ok, [%{"id" => 9}]} =
             LanceDB.Query.search(table, [0, 0], opts ++ [maximum_nprobes: 2])

    assert {:ok, [%{"id" => 9}]} =
             LanceDB.Query.search(table, [0, 0], opts ++ [maximum_nprobes: nil])
  end

  test("fast search omits unindexed appends until optimization", %{table: table}) do
    assert {:ok, :created} =
             LanceDB.Index.create(table, :vector, type: :ivf_flat, num_partitions: 1)

    assert {:ok, :created} = LanceDB.Index.create(table, :text, type: :fts)
    assert {:ok, :created} = LanceDB.Index.create(table, :id, type: :btree)

    assert {:ok, %{inserted: 1}} =
             LanceDB.Table.add(table, [%{id: 9, vector: [0, 0], text: "needle"}])

    runs = [(&LanceDB.Query.query(table, &1)) | search_functions(table)]

    for run <- runs do
      assert {:ok, [%{"id" => 9}]} = run.(filter: "id = 9", fast_search: false)
      assert {:ok, []} = run.(filter: "id = 9", fast_search: true)
    end

    assert {:ok, :optimized} = LanceDB.Maintenance.optimize_indices(table)

    for run <- runs do
      assert {:ok, [%{"id" => 9}]} = run.(filter: "id = 9", fast_search: true)
    end
  end

  test("search offsets paginate ranked results and hybrid applies offset after fusion", %{
    table: table
  }) do
    for run <- search_functions(table) do
      assert {:ok, full} = run.(limit: 4, columns: [:id])
      assert {:ok, page} = run.(limit: 2, offset: 1, columns: [:id])
      assert page == Enum.slice(full, 1, 2)
      assert {:ok, []} = run.(offset: 100)
    end
  end

  test("row IDs survive projection, ordering, searches and hybrid fusion", %{table: table}) do
    assert {:ok, ordinary} =
             LanceDB.Query.query(table, columns: [:id], with_row_id: true, order_by: [desc: :id])

    ids = Map.new(ordinary, &{&1["id"], &1["_rowid"]})
    assert map_size(ids) == 8
    assert Enum.all?(Map.values(ids), &(is_integer(&1) and &1 >= 0))
    assert length(Enum.uniq(Map.values(ids))) == 8

    for run <- search_functions(table) do
      assert {:ok, rows} = run.(columns: [:id], with_row_id: true, offset: 1, limit: 3)
      assert length(rows) == 3
      assert Enum.all?(rows, &(&1["_rowid"] == ids[&1["id"]]))
      assert {:ok, without} = run.(columns: [:id])
      refute Enum.any?(without, &Map.has_key?(&1, "_rowid"))
      assert {:ok, metadata} = run.(columns: [:_rowid], with_row_id: true)
      assert length(metadata) == 8
    end

    assert {:ok, only_ids} = LanceDB.Query.query(table, columns: [:_rowid], with_row_id: true)
    assert Enum.all?(only_ids, &(Map.keys(&1) == ["_rowid"]))
  end

  test("explanations use remaining controls including hybrid output options", %{table: table}) do
    for selectors <- [[], [vector: [0, 0]], [text: "needle", text_column: :text]] do
      assert {:ok, plan} =
               LanceDB.TestSupport.explain_plan(
                 table,
                 selectors ++ [offset: 1, with_row_id: true]
               )

      assert plan =~ ~r/_rowid|row_id=true/
    end

    assert {:ok, hybrid} =
             LanceDB.TestSupport.explain_plan(table,
               vector: [0, 0],
               text: "needle",
               text_column: :text,
               limit: 2,
               offset: 1,
               with_row_id: true,
               prefilter: false,
               distance_range: {1, 20}
             )

    assert hybrid =~ "Pagination: offset=1, candidate_limit=3"
    assert hybrid =~ "retain _rowid"
  end

  test "rejects invalid controls and conflicting settings before native execution" do
    options = [
      [offset: -1],
      [offset: 1.0],
      [offset: nil],
      [offset: 9_223_372_036_854_775_807],
      [offset: 1, limit: 9_223_372_036_854_775_807],
      [offset: 1, limit: 4_611_686_018_427_387_903, refine_factor: 2],
      [prefilter: nil],
      [fast_search: "true"],
      [with_row_id: 1],
      [fast_search: true, bypass_vector_index: true],
      [nprobes: 1, minimum_nprobes: 1],
      [nprobes: 1, maximum_nprobes: nil],
      [minimum_nprobes: nil],
      [minimum_nprobes: 0],
      [minimum_nprobes: 21],
      [maximum_nprobes: 19],
      [maximum_nprobes: 4_294_967_296],
      [minimum_nprobes: 1, maximum_nprobes: 0],
      [distance_range: [0, 1]],
      [distance_range: {2, 1}],
      [distance_range: {1.0, 1.00000000001}],
      [distance_range: {"1", nil}],
      [distance_range: {nil, 1.0e40}]
    ]

    for opts <- options do
      assert_raise ArgumentError, fn ->
        LanceDB.Query.search(%LanceDB.Table{resource: make_ref()}, [0, 0], opts)
      end

      assert_raise ArgumentError, fn ->
        LanceDB.TestSupport.explain_plan(
          :unused,
          [vector: [0, 0]] ++ opts
        )
      end
    end
  end

  defp search_functions(table) do
    [
      &LanceDB.Query.search(table, [0, 0], &1),
      &LanceDB.Query.full_text_search(table, "needle", [text_column: :text] ++ &1),
      &LanceDB.Query.hybrid_search(table, [0, 0], "needle", [text_column: :text] ++ &1)
    ]
  end
end
