defmodule LanceDB.SchemaTest do
  use LanceDB.DataCase, async: true

  setup %{path: path} do
    {:ok, conn} = LanceDB.connect(path)
    %{conn: conn, path: path}
  end

  test "float64 values retain exact precision across JSON and Arrow", %{conn: conn} do
    # Reduced by StreamData: serde_json's default parser rounded this by one ULP.
    value = -900.7297297297297

    assert {:ok, table} =
             LanceDB.Connection.create_table(conn, "precision", [%{id: 1, score: value}],
               schema: [id: :int64, score: :float64]
             )

    assert {:ok, [%{"score" => ^value}]} = LanceDB.Query.query(table)
    assert {:ok, %{updated: 1}} = LanceDB.Table.update(table, %{score: value})
    assert {:ok, [%{"score" => ^value}]} = LanceDB.Query.query(table)
  end

  test "empty schemas persist and support nullable typed writes and updates", %{
    conn: conn,
    path: path
  } do
    schema = [
      %{name: :id, type: :int64, nullable: false},
      {:text, :string},
      {:score, :float64},
      {:weight, :float32},
      {:active, :boolean},
      {:vector, {:vector, 2}}
    ]

    assert {:ok, table} = LanceDB.Connection.create_empty_table(conn, "typed", schema)
    assert {:ok, 0} = LanceDB.Table.count_rows(table)
    assert {:ok, []} = LanceDB.Query.query(table)
    assert {:ok, %LanceDB.Schema{fields: [id | _] = fields}} = LanceDB.Table.schema(table)
    assert id == %LanceDB.Field{name: "id", type: :int64, nullable: false}
    assert Enum.find(fields, &(&1.name == "weight")).type == :float32
    row = %{id: 1, text: nil, score: nil, weight: nil, active: nil, vector: nil}
    assert {:ok, %{inserted: 1}} = LanceDB.Table.add(table, [row])

    assert {:ok, %{inserted: 1, updated: 1}} =
             LanceDB.Table.upsert(table, [
               row,
               %{row | id: 2, vector: [1, 0], score: 2, weight: 1, active: true}
             ])

    assert {:ok, [%{"text" => nil, "vector" => nil}, %{"score" => 2.0, "weight" => 1.0}]} =
             LanceDB.Query.query(table, order_by: [asc: :id])

    assert {:ok, %{updated: 1}} =
             LanceDB.Table.update(table, %{weight: 1.5, text: "hello"}, filter: "id = 1")

    assert {:error, _} = LanceDB.Table.update(table, %{id: nil})
    assert {:error, _} = LanceDB.Table.update(table, %{weight: 1.0e100})
    assert {:ok, reopened_conn} = LanceDB.connect(path)
    assert {:ok, reopened} = LanceDB.Connection.open_table(reopened_conn, "typed")
    assert {:ok, 2} = LanceDB.Table.count_rows(reopened)
    assert {:ok, [%{"id" => 2}]} = LanceDB.Query.search(reopened, [1, 0], limit: 1)
  end

  test "explicit schemas accept initial nulls, preserve types and honor create modes", %{
    conn: conn
  } do
    schema = [id: :string, score: :float64]

    assert {:ok, table} =
             LanceDB.Connection.create_table(conn, "docs", [%{id: nil, score: 1}], schema: schema)

    assert {:ok, [%{"id" => nil, "score" => 1.0}]} = LanceDB.Query.query(table)
    assert {:error, _} = LanceDB.Connection.create_empty_table(conn, "docs", schema)

    assert {:ok, existing} =
             LanceDB.Connection.create_empty_table(conn, "docs", schema, mode: :exist_ok)

    assert {:ok, 1} = LanceDB.Table.count_rows(existing)

    assert {:ok, empty} =
             LanceDB.Connection.create_empty_table(conn, "docs", schema, mode: :overwrite)

    assert {:ok, 0} = LanceDB.Table.count_rows(empty)
    assert {:ok, %LanceDB.WriteResult{inserted: 0, commits: 0}} = LanceDB.Table.add(empty, [])
    assert {:ok, %LanceDB.WriteResult{inserted: 0, updated: 0}} = LanceDB.Table.upsert(empty, [])
  end

  test "typed writes reject missing, extra, mistyped and non-nullable values atomically", %{
    conn: conn
  } do
    schema = [
      %{name: :id, type: :int64, nullable: false},
      {:vector, {:vector, 2}},
      {:text, :string}
    ]

    assert {:ok, table} = LanceDB.Connection.create_empty_table(conn, "docs", schema)
    valid = %{id: 1, vector: [1, 0], text: nil}

    for invalid <- [
          %{valid | id: nil},
          %{valid | id: 1.5},
          %{valid | vector: [1]},
          %{valid | vector: [nil, 1]},
          %{valid | text: 4},
          Map.delete(valid, :text),
          Map.put(valid, :extra, true),
          %{valid | vector: [1.0e100, 0]}
        ] do
      assert {:error, _} =
               LanceDB.Connection.create_table(conn, "invalid", [valid, invalid], schema: schema)

      assert {:error, _} = LanceDB.Table.add(table, [valid, invalid])
      assert {:error, _} = LanceDB.Table.upsert(table, [%{valid | id: 2}, invalid])
      assert {:ok, 0} = LanceDB.Table.count_rows(table)
    end

    assert {:ok, ["docs"]} = LanceDB.Connection.table_names(conn)
  end

  test "large typed batches and upserts into empty tables retain all records", %{conn: conn} do
    {:ok, table} = LanceDB.Connection.create_empty_table(conn, "large", id: :int64, text: :string)
    rows = for id <- 1..1500, do: %{id: id, text: nil}
    assert {:ok, %{inserted: 1500, updated: 0}} = LanceDB.Table.upsert(table, rows)
    assert {:ok, %{inserted: 1500}} = LanceDB.Table.add(table, rows)
    assert {:ok, 3000} = LanceDB.Table.count_rows(table)
    assert {:ok, [%{"id" => 1500}]} = LanceDB.Query.query(table, order_by: [desc: :id], limit: 1)
  end

  test "validates schema definitions and create options", %{conn: conn} do
    for schema <- [
          nil,
          [],
          %{},
          [id: :unknown],
          [id: {:vector, 0}],
          [id: {:vector, 2.0}],
          [id: :int64, id: :string],
          [{"", :string}],
          [{nil, :string}],
          [%{name: :id, type: :string, nullable: :yes}],
          [%{name: :id, type: :string, typo: true}],
          [%{name: :id}],
          [:bad]
        ] do
      assert_raise ArgumentError, fn ->
        LanceDB.Connection.create_empty_table(conn, "bad", schema)
      end
    end

    assert_raise ArgumentError, fn ->
      LanceDB.Connection.create_empty_table(conn, "bad", [id: :int64], typo: true)
    end

    assert_raise ArgumentError, fn ->
      LanceDB.Connection.create_table(conn, "bad", [], typo: true)
    end
  end
end
