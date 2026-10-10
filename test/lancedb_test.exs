defmodule LanceDBTest do
  use LanceDB.DataCase, async: true

  setup %{path: path} do
    {:ok, conn} = LanceDB.connect(path)
    %{conn: conn, path: path}
  end

  test "local table lifecycle, persistence, and similarity search", %{conn: conn, path: path} do
    records = [
      %{id: "a", category: "code", vector: [1.0, 0.0]},
      %{id: "b", category: "food", vector: [0.0, 1.0]}
    ]

    assert {:ok, table} = LanceDB.Connection.create_table(conn, "docs", records)
    assert {:ok, ["docs"]} = LanceDB.Connection.table_names(conn)
    assert {:ok, 2} = LanceDB.Table.count_rows(table)
    assert {:ok, 1} = LanceDB.Table.count_rows(table, "category = 'food'")

    for distance <- [:l2, :cosine, :dot] do
      assert {:ok, [%{"id" => "a", "_distance" => distance_value}]} =
               LanceDB.Query.search(table, [1, 0], limit: 1, distance: distance)

      assert is_number(distance_value)
    end

    assert {:ok, [%{"id" => "b"}]} =
             LanceDB.Query.search(table, [1, 0], filter: "category = 'food'")

    assert {:ok, []} = LanceDB.Query.search(table, [1, 0], filter: "category = 'missing'")

    assert {:ok, %{inserted: 1}} =
             LanceDB.Table.add(table, [%{id: "c", category: "code", vector: [0.8, 0.2]}])

    assert {:ok, 3} = LanceDB.Table.count_rows(table)
    assert {:ok, %LanceDB.Schema{fields: fields}} = LanceDB.Table.schema(table)
    assert Enum.sort(Enum.map(fields, & &1.name)) == ["category", "id", "vector"]

    assert {:ok, other_conn} = LanceDB.connect(path)
    assert {:ok, reopened} = LanceDB.Connection.open_table(other_conn, "docs")
    assert {:ok, 3} = LanceDB.Table.count_rows(reopened)
    assert {:ok, %{operation: :delete}} = LanceDB.Table.delete(table, "id = 'c'")
    assert {:ok, 2} = LanceDB.Table.count_rows(table)
    assert {:ok, :dropped} = LanceDB.Connection.drop_table(conn, "docs")
    assert {:ok, []} = LanceDB.Connection.table_names(conn)
    assert {:error, _} = LanceDB.Connection.open_table(conn, "missing")
  end

  test "create modes are explicit", %{conn: conn} do
    records = [%{id: 1, vector: [1.0, 0.0]}]
    assert {:ok, _} = LanceDB.Connection.create_table(conn, "docs", records)
    assert {:error, _} = LanceDB.Connection.create_table(conn, "docs", records)

    assert {:ok, existing} =
             LanceDB.Connection.create_table(conn, "docs", records, mode: :exist_ok)

    assert {:ok, 1} = LanceDB.Table.count_rows(existing)

    assert {:ok, replaced} =
             LanceDB.Connection.create_table(conn, "docs", records ++ records, mode: :overwrite)

    assert {:ok, 2} = LanceDB.Table.count_rows(replaced)

    assert_raise ArgumentError, fn ->
      LanceDB.Connection.create_table(conn, "docs", records, mode: :typo)
    end
  end

  test "rejects malformed inferred records and incompatible appends", %{conn: conn} do
    {:ok, table} = LanceDB.Connection.create_table(conn, "valid", [%{id: 1, vector: [1, 2]}])

    for records <- [
          [],
          [%{}],
          [fn -> :ok end],
          [%{vector: []}],
          [%{vector: [1, 2]}, %{vector: [1]}],
          [%{id: 1}, %{id: "two"}],
          [%{id: 1}, %{id: 2, extra: true}],
          [%{id: nil}],
          [%{metadata: %{nested: true}}],
          [%{:id => 1, "id" => 2}]
        ] do
      assert {:error, _} = LanceDB.Connection.create_table(conn, "bad", records)
      if records != [], do: assert({:error, _} = LanceDB.Table.add(table, records))
    end

    assert {:ok, ["valid"]} = LanceDB.Connection.table_names(conn)
    assert {:ok, 1} = LanceDB.Table.count_rows(table)
  end

  test "validates search options", %{conn: conn} do
    {:ok, table} = LanceDB.Connection.create_table(conn, "docs", [%{vector: [1.0, 0.0]}])

    for opts <- [[limit: 0], [limit: -1], [distance: :typo], [filter: 1]] do
      assert_raise ArgumentError, fn -> LanceDB.Query.search(table, [1, 0], opts) end
    end

    for vector <- [[], ["bad"]] do
      assert_raise ArgumentError, fn -> LanceDB.Query.search(table, vector) end
    end

    assert {:error, _} = LanceDB.Query.search(table, [1, 2, 3])
  end

  test "supports scalar types, string keys, and nullable scalar values", %{conn: conn} do
    records = [
      %{"id" => 1, "score" => 1.5, "active" => true, "text" => "hello", "vector" => [1, 0]},
      %{"id" => 2, "score" => nil, "active" => false, "text" => nil, "vector" => [0, 1]}
    ]

    assert {:ok, table} = LanceDB.Connection.create_table(conn, "types", records)

    assert {:ok, [%{"id" => 1, "score" => 1.5, "active" => true, "text" => "hello"}]} =
             LanceDB.Query.search(table, [1, 0], limit: 1)

    assert {:error, _} = LanceDB.Table.add(table, [%{wrong: 1}])
  end
end
