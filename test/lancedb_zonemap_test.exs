defmodule LanceDB.ZoneMapTest do
  use LanceDB.DataCase, async: true
  alias LanceDB.{Connection, Error, History, Index, Job, Maintenance, Query, Table}

  setup %{path: path} do
    {:ok, conn} = LanceDB.connect(path)
    %{conn: conn}
  end

  test "range indexes preserve results through appends, reopening and maintenance", %{
    conn: conn,
    path: path
  } do
    rows = [%{id: 0, value: nil} | Enum.map(1..20_000, &%{id: &1, value: &1})]

    {:ok, table} =
      Connection.create_table(conn, "ranges", rows, schema: [id: :int64, value: :int64])

    filters = [
      {"value >= 9000 AND value < 9010", Enum.to_list(9000..9009)},
      {"value = 9005", [9005]},
      {"value IN (1, 10000, 20000)", [1, 10_000, 20_000]},
      {"value IS NULL", [0]},
      {"value > 30000", []}
    ]

    for {filter, expected} <- filters, do: assert_ids(table, filter, expected)

    assert {:ok, :created} = Index.create(table, :value, type: :zonemap, name: "ranges_idx")

    assert {:ok, [%Index.Description{index_type: "ZONEMAP", columns: ["value"]}]} =
             Index.list(table)

    assert {:ok, %{"num_indexed_rows" => 20_001, "num_unindexed_rows" => 0, "loss" => nil}} =
             Index.stats(table, "ranges_idx")

    for {filter, expected} <- filters, do: assert_ids(table, filter, expected)

    assert {:ok, plan} =
             LanceDB.TestSupport.explain_plan(table, filter: "value >= 9000 AND value < 9010")

    assert plan =~ "ScalarIndexQuery"
    assert plan =~ "ranges_idx(ZoneMap)"
    assert {:ok, :prewarmed} = Index.prewarm(table, "ranges_idx")
    assert {:error, %Error{}} = Index.create(table, :value, type: :zonemap, name: "ranges_idx")

    assert {:ok, %{inserted: 2}} =
             Table.add(table, [%{id: 20_001, value: 9005}, %{id: 20_002, value: 30_001}])

    assert {:ok, %{"num_unindexed_rows" => 2}} = Index.stats(table, "ranges_idx")
    expected = Enum.to_list(9000..9009) ++ [20_001]
    assert_ids(table, "value >= 9000 AND value < 9010", expected)
    assert_ids(table, "value > 30000", [20_002])

    {:ok, other} = LanceDB.connect(path)
    {:ok, reopened} = Connection.open_table(other, "ranges")
    assert_ids(reopened, "value >= 9000 AND value < 9010", expected)

    assert {:ok, :optimized} = Maintenance.optimize_indices(table)

    assert {:ok, %{"num_indexed_rows" => 20_003, "num_unindexed_rows" => 0}} =
             Index.stats(table, "ranges_idx")

    assert_ids(table, "value >= 9000 AND value < 9010", expected)

    assert {:ok, :created} =
             Index.create(table, :value, type: :zonemap, name: "ranges_idx", replace: true)

    assert {:ok, :dropped} = Index.drop(table, "ranges_idx")
    assert {:ok, []} = Index.list(table)
    assert_ids(table, "value >= 9000 AND value < 9010", expected)
  end

  test "reusable definitions support asynchronous and initially empty indexes", %{conn: conn} do
    {:ok, table} = Connection.create_table(conn, "jobs", Enum.map(1..5, &%{id: &1}))
    definition = Index.new(type: :zonemap, name: "ids", train: false)

    assert {:ok, %Job{} = job} = Index.create_async(table, :id, definition)
    assert {:ok, :created} = Job.wait(job, timeout_ms: 10_000)

    assert {:ok, %{"num_indexed_rows" => 0, "num_unindexed_rows" => 5}} =
             Index.stats(table, "ids")

    assert_ids(table, "id >= 3", [3, 4, 5])
    assert {:ok, :optimized} = Maintenance.optimize_indices(table)

    assert {:ok, %{"num_indexed_rows" => 5, "num_unindexed_rows" => 0}} =
             Index.stats(table, "ids")

    assert_ids(table, "id >= 3", [3, 4, 5])

    replacement = Index.new(type: :zonemap, name: "ids", replace: true)
    assert {:ok, %Job{} = job} = Index.create_async(table, :id, replacement)
    assert {:ok, :created} = Job.wait(job, timeout_ms: 10_000)
    assert {:ok, :ready} = Index.wait(table, "ids")
    assert {:ok, [%Index.Description{name: "ids", index_type: "ZONEMAP"}]} = Index.list(table)
    assert_ids(table, "id >= 3", [3, 4, 5])
  end

  test "timestamp and wider string and binary types support indexed filters and nulls", %{
    conn: conn
  } do
    cases = [
      {{:timestamp, :microsecond}, 1000, 2000, "value >= CAST(1500 AS TIMESTAMP)"},
      {:string, "alpha", "omega", "value >= 'm'"},
      {:large_string, "alpha", "omega", "value >= 'm'"},
      {:binary, <<0, 1>>, <<255, 0>>, "value >= X'80'"},
      {:large_binary, <<0, 1>>, <<255, 0>>, "value >= X'80'"}
    ]

    for {{type, low, high, filter}, index} <- Enum.with_index(cases) do
      {:ok, table} =
        Connection.create_table(
          conn,
          "scalar_#{index}",
          [%{id: 1, value: low}, %{id: 2, value: high}, %{id: 3, value: nil}],
          schema: [id: :int64, value: type]
        )

      assert_ids(table, filter, [2])
      assert {:ok, :created} = Index.create(table, :value, type: :zonemap)
      assert_ids(table, filter, [2])
      assert_ids(table, "value IS NULL", [3])
      assert {:ok, plan} = LanceDB.TestSupport.explain_plan(table, filter: filter)
      assert plan =~ "value_idx(ZoneMap)"
    end
  end

  test "incompatible columns and snapshots fail before changing table data", %{conn: conn} do
    {:ok, table} =
      Connection.create_table(conn, "invalid", [%{id: 1, values: [1], vector: [1.0, 0.0]}],
        schema: [id: :int64, values: {:list, :int64}, vector: {:vector, 2}]
      )

    {:ok, version} = History.version(table)

    for column <- [:values, :vector, :missing] do
      assert {:error, %Error{}} = Index.create(table, column, type: :zonemap)
    end

    {:ok, snapshot} = History.snapshot(table, version)
    assert {:error, %Error{}} = Index.create(snapshot, :id, type: :zonemap)
    assert {:error, %Error{}} = Index.create_async(snapshot, :id, type: :zonemap)
    assert {:ok, ^version} = History.version(table)
    assert {:ok, []} = Index.list(table)
    assert_ids(table, "id = 1", [1])
  end

  test "zone maps reject vector, analyzer and FTS build options" do
    for opts <- [
          [distance: :cosine],
          [num_partitions: 1],
          [num_sub_vectors: 1],
          [num_bits: 8],
          [sample_rate: 1],
          [max_iterations: 1],
          [target_partition_size: 1],
          [m: 4],
          [ef_construction: 4],
          [analyzer: []],
          [block_size: 128],
          [memory_limit_mb: 1],
          [num_workers: 1],
          [format_version: :v3]
        ] do
      assert_raise ArgumentError, fn -> Index.new([type: :zonemap] ++ opts) end
    end
  end

  defp assert_ids(table, filter, expected) do
    assert {:ok, rows} = Query.query(table, filter: filter, columns: [:id], limit: nil)
    assert Enum.sort(Enum.map(rows, & &1["id"])) == expected
  end
end
