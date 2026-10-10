defmodule LanceDB.QueryWriteTest do
  use LanceDB.DataCase, async: true

  setup %{path: path} do
    {:ok, db} = LanceDB.connect(path)

    records = [
      %{id: 1, tenant: "a", text: "first", score: 10, active: true, vector: [1.0, 0.0]},
      %{id: 2, tenant: "a", text: "second", score: 20, active: true, vector: [0.0, 1.0]},
      %{id: 3, tenant: "b", text: "third", score: 30, active: false, vector: [0.5, 0.5]}
    ]

    {:ok, table} = LanceDB.Connection.create_table(db, "documents", records)
    %{db: db, table: table, records: records}
  end

  test "queries all columns and filters by ID without a vector", %{table: table} do
    assert {:ok, rows} = LanceDB.Query.query(table)
    assert length(rows) == 3
    assert Enum.all?(rows, &Map.has_key?(&1, "vector"))
    refute Enum.any?(rows, &Map.has_key?(&1, "_distance"))

    assert {:ok, [%{"id" => 2, "text" => "second"}]} =
             LanceDB.Query.query(table, filter: "id = 2")

    assert {:ok, []} = LanceDB.Query.query(table, filter: "id = 999")
  end

  test "selects columns and paginates ordered results", %{table: table} do
    assert {:ok, [%{"id" => 2, "text" => "second"}]} =
             LanceDB.Query.query(table,
               columns: [:id, "text"],
               order_by: [asc: :id],
               limit: 1,
               offset: 1
             )

    assert {:ok, [%{"id" => 3}, %{"id" => 2}]} =
             LanceDB.Query.query(table, columns: [:id], order_by: [desc: :score], limit: 2)

    assert {:ok, [%{"id" => 2}, %{"id" => 1}]} =
             LanceDB.Query.query(table,
               filter: "tenant = 'a'",
               columns: [:id],
               order_by: [asc: :tenant, desc: :id]
             )

    assert {:ok, []} = LanceDB.Query.query(table, offset: 99)
  end

  test "queries scalar-only tables and defaults to 100 rows", %{db: db} do
    {:ok, table} = LanceDB.Connection.create_table(db, "scalars", Enum.map(1..105, &%{id: &1}))
    assert {:ok, rows} = LanceDB.Query.query(table)
    assert length(rows) == 100

    assert {:ok, [%{"id" => 101}, %{"id" => 102}, %{"id" => 103}, %{"id" => 104}, %{"id" => 105}]} =
             LanceDB.Query.query(table, order_by: [asc: :id], offset: 100)
  end

  test "invalid query options raise before calling the NIF", %{table: table} do
    for opts <- [
          [limit: 0],
          [limit: -1],
          [limit: 1.2],
          [limit: Integer.pow(2, 64)],
          [offset: -1],
          [offset: nil],
          [offset: Integer.pow(2, 64)],
          [filter: "  "],
          [filter: 1],
          [filter: <<255>>],
          [columns: []],
          [columns: :id],
          [columns: [nil]],
          [columns: [1]],
          [columns: [""]],
          [columns: [:id, "id"]],
          [order_by: :id],
          [order_by: [up: :id]],
          [order_by: [asc: nil]],
          [typo: true]
        ] do
      assert_raise ArgumentError, fn -> LanceDB.Query.query(table, opts) end
    end
  end

  test "query errors are returned as tuples", %{table: table} do
    assert {:error, _} = LanceDB.Query.query(table, filter: "id =")
    assert {:error, _} = LanceDB.Query.query(table, columns: [:missing])
    assert {:error, _} = LanceDB.Query.query(table, order_by: [asc: :missing])
  end

  test "upsert replaces matches, inserts new rows, and preserves other rows", %{
    table: table,
    records: [first | _]
  } do
    incoming = [%{first | text: "replaced", vector: [0.2, 0.8]}, %{first | id: 4, text: "new"}]
    assert {:ok, %{inserted: 1, updated: 1}} = LanceDB.Table.upsert(table, incoming)
    assert {:ok, 4} = LanceDB.Table.count_rows(table)

    assert {:ok, [%{"text" => "replaced", "vector" => vector}]} =
             LanceDB.Query.query(table, filter: "id = 1")

    assert_in_delta hd(vector), 0.2, 0.0001
    assert {:ok, [%{"text" => "second"}]} = LanceDB.Query.query(table, filter: "id = 2")
    assert {:ok, %{inserted: 0}} = LanceDB.Table.upsert(table, incoming)
    assert {:ok, 4} = LanceDB.Table.count_rows(table)
  end

  test "upsert supports composite keys and string-keyed records", %{db: db} do
    {:ok, table} =
      LanceDB.Connection.create_table(db, "composite", [
        %{"tenant" => "a", "id" => 1, "text" => "old"},
        %{"tenant" => "b", "id" => 1, "text" => "unchanged"}
      ])

    assert {:ok, %{inserted: 1, updated: 1}} =
             LanceDB.Table.upsert(
               table,
               [
                 %{"tenant" => "a", "id" => 1, "text" => "new"},
                 %{"tenant" => "a", "id" => 2, "text" => "inserted"}
               ],
               on: [:tenant, "id"]
             )

    assert {:ok, [%{"text" => "unchanged"}]} = LanceDB.Query.query(table, filter: "tenant = 'b'")
    assert {:ok, 3} = LanceDB.Table.count_rows(table)
  end

  test "upsert supports a custom single string key", %{db: db} do
    {:ok, table} = LanceDB.Connection.create_table(db, "keys", [%{key: "a", value: 1}])

    assert {:ok, %{inserted: 0, updated: 1}} =
             LanceDB.Table.upsert(table, [%{key: "a", value: 2}], on: "key")

    assert {:ok, [%{"value" => 2}]} = LanceDB.Query.query(table)
  end

  test "upsert rejects duplicate or missing keys and mismatched schemas", %{
    table: table,
    records: [first | _]
  } do
    for records <- [
          [%{id: nil}],
          [first, first],
          [Map.delete(first, :id)],
          [first, %{first | id: nil}],
          [%{first | vector: [1.0]}],
          [Map.delete(first, :text)],
          [Map.put(first, :extra, true)]
        ] do
      assert {:error, _} = LanceDB.Table.upsert(table, records)
    end

    assert {:error, _} = LanceDB.Table.upsert(table, [first], on: :missing)
    assert {:error, _} = LanceDB.Table.upsert(table, [first], on: :vector)

    for opts <- [[on: []], [on: nil], [on: ["id", :id]], [typo: :id]] do
      assert_raise ArgumentError, fn -> LanceDB.Table.upsert(table, [first], opts) end
    end

    assert {:ok, 3} = LanceDB.Table.count_rows(table)
    assert {:ok, [%{"text" => "first"}]} = LanceDB.Query.query(table, filter: "id = 1")
  end

  test "update metadata identifies the literal, SQL, and no-match operation versions", %{
    table: table
  } do
    {:ok, before_update} = LanceDB.History.version(table)

    assert {:ok, %{updated: 1, version: literal_version}} =
             LanceDB.Table.update(table, %{score: 99}, filter: "id = 1")

    assert literal_version > before_update
    assert {:ok, ^literal_version} = LanceDB.History.version(table)

    assert {:ok, %{updated: 3, version: sql_version}} =
             LanceDB.Table.update(table, %{score: {:sql, "score + 1"}})

    assert sql_version > literal_version
    assert {:ok, ^sql_version} = LanceDB.History.version(table)

    assert {:ok, %{updated: 0, version: no_match_version}} =
             LanceDB.Table.update(table, %{score: 0}, filter: "id = 999")

    assert {:ok, ^no_match_version} = LanceDB.History.version(table)

    assert {:ok, %{inserted: 1}} =
             LanceDB.Table.add(table, [
               %{id: 4, tenant: "a", text: "later", active: true, score: 40, vector: [1.0, 0.0]}
             ])

    assert {:ok, table} = LanceDB.History.snapshot(table, literal_version)
    assert {:ok, [%{"score" => 99}]} = LanceDB.Query.query(table, filter: "id = 1")
    assert {:ok, table} = LanceDB.History.snapshot(table, sql_version)
    assert {:ok, [%{"score" => 100}]} = LanceDB.Query.query(table, filter: "id = 1")
  end

  test "updates literal values safely and preserves unmatched rows", %{table: table} do
    text = "O'Brien'; DROP TABLE documents; --\nhello"

    assert {:ok, %{updated: 1}} =
             LanceDB.Table.update(table, %{text: text, active: false, score: 99},
               filter: "id = 1"
             )

    assert {:ok, [%{"text" => ^text, "active" => false, "score" => 99}]} =
             LanceDB.Query.query(table, filter: "id = 1")

    assert {:ok, [%{"text" => "second", "score" => 20}]} =
             LanceDB.Query.query(table, filter: "id = 2")

    assert {:ok, %{updated: 0}} = LanceDB.Table.update(table, %{score: 0}, filter: "id = 999")
  end

  test "updates using SQL expressions and defaults to every row", %{table: table} do
    assert {:ok, %{updated: 3}} =
             LanceDB.Table.update(table, %{"score" => {:sql, "score + 5"}, "active" => true})

    assert {:ok, [%{"score" => 15}, %{"score" => 25}, %{"score" => 35}]} =
             LanceDB.Query.query(table, columns: [:score], order_by: [asc: :id])

    assert {:ok, 3} = LanceDB.Table.count_rows(table, "active = true")
  end

  test "updates floats, vectors, and nulls", %{db: db} do
    {:ok, table} =
      LanceDB.Connection.create_table(db, "types", [
        %{id: 1, score: 1.5, text: "a", vector: [1.0, 0.0]}
      ])

    assert {:ok, %{updated: 1}} =
             LanceDB.Table.update(table, %{score: 2.5, text: nil, vector: [0, 1]})

    assert {:ok, [row]} = LanceDB.Query.query(table)
    assert row["score"] == 2.5
    assert row["text"] == nil
    assert Map.has_key?(row, "text")
    assert row["vector"] == [0.0, 1.0]
    assert {:ok, 1} = LanceDB.Table.count_rows(table, "text IS NULL")
  end

  test "invalid updates raise before mutation", %{table: table} do
    for changes <- [
          %{},
          [],
          %URI{},
          %{score: :bad},
          %{score: Integer.pow(2, 64)},
          %{score: {:sql, nil}},
          %{score: {:sql, ""}},
          %{:text => "a", "text" => "b"},
          %{nil => "bad"}
        ] do
      assert_raise ArgumentError, fn -> LanceDB.Table.update(table, changes) end
    end

    assert {:error, _} = LanceDB.Table.update(table, %{text: <<255>>})
    assert_raise ArgumentError, fn -> LanceDB.Table.update(table, %{score: 1}, filter: "") end
    assert_raise ArgumentError, fn -> LanceDB.Table.update(table, %{score: 1}, typo: true) end
    assert {:ok, 1} = LanceDB.Table.count_rows(table, "score = 10")
  end

  test "SQL errors and incompatible updates return error tuples", %{table: table} do
    for {changes, opts} <- [
          {%{missing: 1}, []},
          {%{score: {:sql, "invalid +"}}, []},
          {%{score: "not a number"}, []},
          {%{score: 1.5}, []},
          {%{active: 1}, []},
          {%{text: 1}, []},
          {%{vector: [1.0]}, []},
          {%{vector: []}, []},
          {%{vector: ["bad"]}, []},
          {%{score: %{}}, []},
          {%{score: 1}, [filter: "id ="]}
        ] do
      assert {:error, _} = LanceDB.Table.update(table, changes, opts)
    end

    assert {:ok, 1} = LanceDB.Table.count_rows(table, "score = 10")
  end

  test "orders nulls last and preserves nulls in projections", %{table: table} do
    assert {:ok, %{updated: 1}} = LanceDB.Table.update(table, %{text: nil}, filter: "id = 1")

    assert {:ok, [%{"id" => 2}, %{"id" => 3}, %{"id" => 1}]} =
             LanceDB.Query.query(table, columns: [:id], order_by: [asc: :text])

    assert {:ok, [%{"text" => nil}]} =
             LanceDB.Query.query(table, columns: [:text], filter: "id = 1")
  end

  test "literal text and explicit SQL stay distinct in the same update", %{table: table} do
    literal = "score + 1; 'quoted'"

    assert {:ok, %{updated: 1}} =
             LanceDB.Table.update(table, %{text: literal, score: {:sql, "score + 1"}},
               filter: "id = 1"
             )

    assert {:ok, [%{"text" => ^literal, "score" => score}]} =
             LanceDB.Query.query(table, filter: "id = 1", columns: [:text, :score])

    assert is_integer(score)
  end
end
