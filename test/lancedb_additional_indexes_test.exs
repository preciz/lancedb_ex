defmodule LanceDB.AdditionalIndexesTest do
  use LanceDB.DataCase, async: true

  setup %{path: path} do
    {:ok, conn} = LanceDB.connect(path)

    {:ok, table} =
      LanceDB.Connection.create_table(
        conn,
        "docs",
        [
          %{id: 1, tags: ["elixir", "rust"], text: "LanceDB and Elixir"},
          %{id: 2, tags: ["elixir"], text: "an elixir of life"},
          %{id: 3, tags: ["rust"], text: "Rust bindings"},
          %{id: 4, tags: [], text: ""},
          %{id: 5, tags: nil, text: nil}
        ],
        schema: [id: :int64, tags: {:list, :string}, text: :string]
      )

    %{conn: conn, table: table}
  end

  test("LabelList indexes accelerate any/all filters and retain appended rows", context) do
    check_index(context, :tags, :label_list, "LABEL_LIST", [
      {"array_has_any(tags, ['elixir'])", [1, 2]},
      {"array_has_all(tags, ['elixir', 'rust'])", [1]}
    ])
  end

  test("FM indexes match case-sensitive substrings, including within words", context) do
    check_index(context, :text, :fm, "FM", [
      {"contains(text, 'ixir')", [1, 2]},
      {"contains(text, 'Elixir')", [1]},
      {"contains(text, 'absent')", []}
    ])
  end

  defp check_index(%{conn: conn, table: table}, column, type, index_type, filters) do
    for {filter, expected} <- filters do
      assert_ids(table, filter, expected)
    end

    assert {:ok, :created} = LanceDB.Index.create(table, column, type: type, name: "lookup")

    assert {:ok, [%LanceDB.Index.Description{index_type: ^index_type}]} =
             LanceDB.Index.list(table)

    assert {:ok, %{"num_indexed_rows" => 5}} = LanceDB.Index.stats(table, "lookup")
    assert {:ok, :prewarmed} = LanceDB.Index.prewarm(table, "lookup")
    {:ok, table} = LanceDB.Connection.open_table(conn, "docs")

    for {filter, expected} <- filters do
      assert_ids(table, filter, expected)
      assert {:ok, plan} = LanceDB.TestSupport.explain_plan(table, filter: filter)
      assert plan =~ "Index"
    end

    assert {:ok, %{inserted: 1}} =
             LanceDB.Table.add(table, [%{id: 6, tags: ["elixir"], text: "Elixir"}])

    {filter, expected} = hd(filters)
    assert_ids(table, filter, expected ++ [6])
    assert {:ok, :optimized} = LanceDB.Maintenance.optimize_indices(table)
    assert_ids(table, filter, expected ++ [6])

    assert {:ok, :created} =
             LanceDB.Index.create(table, column, type: type, name: "lookup", replace: true)

    assert {:ok, :dropped} = LanceDB.Index.drop(table, "lookup")
    assert_ids(table, filter, expected ++ [6])
  end

  defp assert_ids(table, filter, expected) do
    assert {:ok, rows} = LanceDB.Query.query(table, filter: filter, columns: [:id])
    assert Enum.sort(Enum.map(rows, & &1["id"])) == expected
  end

  test("new index types reject incompatible fields and vector/analyzer options", %{table: table}) do
    assert {:error, _} = LanceDB.Index.create(table, :id, type: :fm)
    assert {:error, _} = LanceDB.Index.create(table, :text, type: :label_list)

    for type <- [:label_list, :fm],
        opts <- [[num_partitions: 1], [num_sub_vectors: 1], [m: 4], [analyzer: []]] do
      assert_raise ArgumentError, fn ->
        LanceDB.Index.create(:unused, :tags, [type: type] ++ opts)
      end
    end
  end
end
