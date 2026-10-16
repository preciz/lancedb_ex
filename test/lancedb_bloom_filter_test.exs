defmodule LanceDB.BloomFilterTest do
  use LanceDB.DataCase, async: true
  alias LanceDB.{Connection, Error, Expr, History, Index, Job, Maintenance, Query, Table}

  setup %{path: path} do
    {:ok, conn} = LanceDB.connect(path)
    %{conn: conn}
  end

  test "equality and membership filters prune groups and recheck candidate rows", %{
    conn: conn,
    path: path
  } do
    rows = [%{id: 0, value: nil} | Enum.map(1..128, &%{id: &1, value: &1})]

    {:ok, table} =
      Connection.create_table(conn, "values", rows,
        max_rows_per_file: 32,
        schema: [id: :int64, value: :int64]
      )

    filters = [
      {Expr.eq(Expr.col(:value), 64), [64]},
      {"value IN (1, 64, 128)", [1, 64, 128]},
      {"value IN (64, NULL)", [64]},
      {"value NOT IN (64, NULL)", []},
      {"value IS NULL", [0]},
      {"value = NULL", []},
      {"value = 500", []},
      {"value != 64", Enum.to_list(1..128) -- [64]}
    ]

    for {filter, expected} <- filters, do: assert_ids(table, filter, expected)

    assert {:ok, :created} =
             Index.create(table, :value,
               type: :bloom_filter,
               name: "lookup",
               number_of_items: 16,
               probability: 0.25
             )

    for {filter, expected} <- filters, do: assert_ids(table, filter, expected)

    for filter <- ["value = 64", "value IN (1, 64, 128)", "value IS NULL"] do
      assert {:ok, plan} = LanceDB.TestSupport.explain_plan(table, filter: filter)
      assert plan =~ "lookup(BloomFilter)", plan
    end

    assert_ids(table, "value >= 60 AND value < 65", Enum.to_list(60..64))

    assert {:ok, plan} =
             LanceDB.TestSupport.explain_plan(table, filter: "value >= 60 AND value < 65")

    refute plan =~ "ScalarIndexQuery"

    assert {:ok,
            [%Index.Description{name: "lookup", index_type: "BLOOM_FILTER", columns: ["value"]}]} =
             Index.list(table)

    assert {:ok, %{"num_indexed_rows" => 129, "num_unindexed_rows" => 0, "loss" => nil}} =
             Index.stats(table, "lookup")

    assert {:ok, :prewarmed} = Index.prewarm(table, "lookup")
    assert {:error, %Error{}} = Index.create(table, :value, type: :bloom_filter, name: "lookup")

    assert {:ok, %{inserted: 2}} =
             Table.add(table, [%{id: 129, value: 64}, %{id: 130, value: 500}])

    assert {:ok, %{"num_unindexed_rows" => 2}} = Index.stats(table, "lookup")
    assert_ids(table, "value = 64", [64, 129])
    assert_ids(table, "value = 500", [130])
    {:ok, other} = LanceDB.connect(path)
    {:ok, reopened} = Connection.open_table(other, "values")
    assert_ids(reopened, "value = 64", [64, 129])
    assert {:ok, :optimized} = Maintenance.optimize_indices(table)

    assert {:ok, %{"num_indexed_rows" => 131, "num_unindexed_rows" => 0}} =
             Index.stats(table, "lookup")

    assert_ids(table, "value = 64", [64, 129])

    assert {:ok, :created} =
             Index.create(table, :value, type: :bloom_filter, name: "lookup", replace: true)

    assert_ids(table, "value = 64", [64, 129])
    assert {:ok, :dropped} = Index.drop(table, "lookup")
    assert {:ok, []} = Index.list(table)
    assert_ids(table, "value = 64", [64, 129])
  end

  test "supported scalar families retain equality and null semantics", %{conn: conn} do
    cases = [
      {:int8, -128, 127, Expr.eq(Expr.col(:value), -128)},
      {:int16, -32_768, 32_767, Expr.eq(Expr.col(:value), -32_768)},
      {:int32, -2_147_483_648, 2_147_483_647, Expr.eq(Expr.col(:value), -2_147_483_648)},
      {:int64, -9_223_372_036_854_775_808, 9_223_372_036_854_775_807,
       Expr.eq(Expr.col(:value), -9_223_372_036_854_775_808)},
      {:uint8, 0, 255, Expr.eq(Expr.col(:value), 0)},
      {:uint16, 0, 65_535, Expr.eq(Expr.col(:value), 0)},
      {:uint32, 0, 4_294_967_295, Expr.eq(Expr.col(:value), 0)},
      {:uint64, 18_446_744_073_709_551_615, 0,
       Expr.eq(Expr.col(:value), 18_446_744_073_709_551_615)},
      {:string, "雪", "", Expr.eq(Expr.col(:value), "雪")},
      {:large_string, "雪", "", Expr.eq(Expr.col(:value), "雪")},
      {:binary, <<0, 255>>, <<>>, Expr.eq(Expr.col(:value), Expr.binary(<<0, 255>>))},
      {:large_binary, <<0, 255>>, <<>>, Expr.eq(Expr.col(:value), Expr.binary(<<0, 255>>))},
      {:date32, ~D[2026-01-01], ~D[2026-01-02], Expr.eq(Expr.col(:value), ~D[2026-01-01])},
      {:date64, ~D[2026-01-01], ~D[2026-01-02], Expr.eq(Expr.col(:value), ~D[2026-01-01])},
      {{:time32, :second}, ~T[12:00:00], ~T[13:00:00],
       "value = arrow_cast('12:00:00', 'Time32(Second)')"},
      {{:time64, :microsecond}, ~T[12:00:00], ~T[13:00:00],
       "value = arrow_cast('12:00:00', 'Time64(Microsecond)')"},
      {{:timestamp, :microsecond}, 1000, 2000, "value = CAST(1000 AS TIMESTAMP)"}
    ]

    for {{type, first, second, filter}, index} <- Enum.with_index(cases) do
      {:ok, table} =
        Connection.create_table(
          conn,
          "scalar_#{index}",
          [%{id: 1, value: first}, %{id: 2, value: second}, %{id: 3, value: nil}],
          schema: [id: :int64, value: type]
        )

      assert_ids(table, filter, [1])
      assert {:ok, :created} = Index.create(table, :value, type: :bloom_filter)
      assert_ids(table, filter, [1])
      assert_ids(table, "value IS NULL", [3])
      assert {:ok, plan} = LanceDB.TestSupport.explain_plan(table, filter: filter)
      assert plan =~ "value_idx(BloomFilter)", "#{inspect(type)}: #{plan}"
    end
  end

  test "floating equality keeps both zero encodings across filter groups", %{conn: conn} do
    for type <- [:float32, :float64] do
      {:ok, table} =
        Connection.create_table(
          conn,
          Atom.to_string(type),
          [
            %{id: 1, value: -0.0},
            %{id: 2, value: 0.0},
            %{id: 3, value: 1.5},
            %{id: 4, value: nil}
          ],
          schema: [id: :int64, value: type],
          max_rows_per_file: 1
        )

      filters = [
        {Expr.eq(Expr.col(:value), 0.0), [1, 2]},
        {Expr.eq(Expr.col(:value), -0.0), [1, 2]},
        {"value IN (-0.0, 1.5)", [1, 2, 3]},
        {"value != 0.0", [3]},
        {"value IS NULL", [4]}
      ]

      for {filter, expected} <- filters, do: assert_ids(table, filter, expected)

      assert {:ok, :created} =
               Index.create(table, :value, type: :bloom_filter, number_of_items: 1)

      for {filter, expected} <- filters, do: assert_ids(table, filter, expected)
      assert {:ok, plan} = LanceDB.TestSupport.explain_plan(table, filter: "value = 0.0")
      assert plan =~ "value_idx(BloomFilter)", plan
    end
  end

  test "reusable definitions support empty indexes and asynchronous rebuilds", %{conn: conn} do
    {:ok, table} = Connection.create_table(conn, "jobs", Enum.map(1..5, &%{id: &1}))

    definition =
      Index.new(
        type: :bloom_filter,
        name: "ids",
        train: false,
        number_of_items: 4,
        probability: 0.01
      )

    assert {:ok, %Job{} = job} = Index.create_async(table, :id, definition)
    assert {:ok, :created} = Job.wait(job, timeout_ms: 10_000)

    assert {:ok, %{"num_indexed_rows" => 0, "num_unindexed_rows" => 5}} =
             Index.stats(table, "ids")

    assert_ids(table, "id IN (1, 5)", [1, 5])
    assert {:ok, :optimized} = Maintenance.optimize_indices(table)
    assert {:ok, :ready} = Index.wait(table, "ids")

    assert {:ok, %{"num_indexed_rows" => 5, "num_unindexed_rows" => 0}} =
             Index.stats(table, "ids")

    replacement = Index.new(type: :bloom_filter, name: "ids", replace: true)
    assert {:ok, %Job{} = job} = Index.create_async(table, :id, replacement)
    assert {:ok, :created} = Job.wait(job, timeout_ms: 10_000)
    assert_ids(table, "id IN (1, 5)", [1, 5])
  end

  test "unsupported types and snapshots reject creation before changing data", %{conn: conn} do
    for {{type, value}, index} <-
          Enum.with_index([
            {:boolean, true},
            {:float16, 1.0},
            {{:fixed_size_binary, 2}, <<0, 1>>},
            {{:decimal128, 5, 2}, "1.23"},
            {{:list, :int64}, [1]}
          ]) do
      {:ok, table} =
        Connection.create_table(conn, "invalid_#{index}", [%{id: 1, value: value}],
          schema: [id: :int64, value: type]
        )

      {:ok, version} = History.version(table)
      assert {:error, %Error{}} = Index.create(table, :value, type: :bloom_filter)
      {:ok, snapshot} = History.snapshot(table, version)
      assert {:error, %Error{}} = Index.create(snapshot, :id, type: :bloom_filter)
      assert {:error, %Error{}} = Index.create_async(snapshot, :id, type: :bloom_filter)
      assert {:ok, ^version} = History.version(table)
      assert {:ok, []} = Index.list(table)
      assert {:ok, 1} = Table.count_rows(table)
    end
  end

  test "Bloom tuning validates boundaries and rejects options on other index types" do
    assert %Index.Definition{} =
             Index.new(type: :bloom_filter, number_of_items: 18_446_744_073_709_551_615)

    assert %Index.Definition{} = Index.new(type: :bloom_filter, probability: 1.0e-300)

    for value <- [0, -1, 1.5, nil, true, "4", 18_446_744_073_709_551_616] do
      assert_raise ArgumentError, fn -> Index.new(type: :bloom_filter, number_of_items: value) end
    end

    for value <- [0, 1, -0.1, 1.1, nil, true, "0.01", :nan] do
      assert_raise ArgumentError, fn -> Index.new(type: :bloom_filter, probability: value) end
    end

    for type <- [:auto, :btree, :bitmap, :zonemap, :ngram, :fts, :ivf_flat],
        tuning <- [[number_of_items: 4], [probability: 0.01]] do
      assert_raise ArgumentError, fn -> Index.new([type: type] ++ tuning) end
    end

    for opts <- [[distance: :cosine], [num_partitions: 1], [analyzer: []], [block_size: 128]] do
      assert_raise ArgumentError, fn -> Index.new([type: :bloom_filter] ++ opts) end
    end
  end

  defp assert_ids(table, filter, expected) do
    assert {:ok, rows} = Query.query(table, filter: filter, columns: [:id], limit: nil)

    assert Enum.sort(Enum.map(rows, & &1["id"])) == expected,
           "Filter: #{inspect(filter)}; rows: #{inspect(rows)}"
  end
end
