defmodule LanceDB.EvolutionTest do
  use LanceDB.DataCase, async: true

  setup %{path: path} do
    {:ok, conn} = LanceDB.connect(path)

    {:ok, table} =
      LanceDB.Connection.create_table(conn, "docs", [
        %{id: 1, text: "elixir", score: 3, vector: [1, 0]},
        %{id: 2, text: nil, score: 4, vector: [0, 1]}
      ])

    %{conn: conn, table: table, path: path}
  end

  test("adds derived and backfilled columns, persists and supports subsequent writes", %{
    table: table,
    path: path
  }) do
    assert {:ok, %{version: version}} =
             LanceDB.SchemaEvolution.add_columns(table,
               doubled: "score * 2",
               status: "'new'",
               note: "CAST(NULL AS STRING)"
             )

    assert version > 1

    assert {:ok, [%{"doubled" => 6, "status" => "new", "note" => nil}, %{"doubled" => 8}]} =
             LanceDB.Query.query(table, order_by: [asc: :id])

    assert {:ok, %{version: next}} =
             LanceDB.SchemaEvolution.add_columns(table, %{"ratio" => "CAST(score AS DOUBLE) / 2"})

    assert next > version
    assert {:ok, conn} = LanceDB.connect(path)
    assert {:ok, reopened} = LanceDB.Connection.open_table(conn, "docs")

    row = %{
      id: 3,
      text: nil,
      score: 5,
      vector: [1, 1],
      doubled: 10,
      status: "manual",
      note: nil,
      ratio: 2.5
    }

    assert {:ok, %{inserted: 1}} = LanceDB.Table.add(reopened, [row])

    assert {:ok, %{inserted: 0, updated: 1}} =
             LanceDB.Table.upsert(reopened, [%{row | doubled: 99}])

    assert {:ok, [%{"doubled" => 99}]} = LanceDB.Query.query(reopened, filter: "id = 3")
    assert {:error, _} = LanceDB.Table.add(reopened, [Map.delete(row, :status)])
  end

  test("renames indexed scalar, text and vector fields while preserving searches", %{
    table: table,
    path: path
  }) do
    assert {:ok, :created} = LanceDB.Index.create(table, :id, type: :btree, name: "ids")
    assert {:ok, :created} = LanceDB.Index.create(table, :text, type: :fts, name: "words")

    assert {:ok, :created} =
             LanceDB.Index.create(table, :vector,
               type: :ivf_flat,
               num_partitions: 1,
               name: "vectors"
             )

    assert {:ok, %{version: version}} =
             LanceDB.SchemaEvolution.alter_columns(table,
               id: [rename: :key, nullable: false],
               text: [rename: :title],
               vector: [rename: :embedding]
             )

    assert version > 1
    assert {:ok, [%{"key" => 1}]} = LanceDB.Query.query(table, filter: "key = 1")

    assert {:ok, [%{"key" => 1}]} =
             LanceDB.Query.full_text_search(table, "elixir", text_column: :title)

    assert {:ok, [%{"key" => 1}]} =
             LanceDB.Query.search(table, [1, 0], vector_column: :embedding, limit: 1)

    assert {:ok, indices} = LanceDB.Index.list(table)
    assert Enum.sort(Enum.flat_map(indices, & &1.columns)) == ["embedding", "key", "title"]
    assert {:ok, %LanceDB.Schema{fields: fields}} = LanceDB.Table.schema(table)
    assert Enum.find(fields, &(&1.name == "key")).nullable == false
    assert {:error, _} = LanceDB.Query.query(table, columns: [:id])
    assert {:ok, conn} = LanceDB.connect(path)
    assert {:ok, reopened} = LanceDB.Connection.open_table(conn, "docs")

    assert {:ok, %{inserted: 1}} =
             LanceDB.Table.add(reopened, [%{key: 3, title: "new", score: 5, embedding: [1, 1]}])

    assert {:error, _} =
             LanceDB.Table.add(reopened, [%{key: nil, title: "bad", score: 5, embedding: [1, 1]}])
  end

  test("nullability changes validate existing rows and preserve failed batches", %{table: table}) do
    assert {:ok, original} = LanceDB.Table.schema(table)

    assert {:error, _} =
             LanceDB.SchemaEvolution.alter_columns(table,
               score: [rename: :points],
               text: [nullable: false]
             )

    assert {:ok, ^original} = LanceDB.Table.schema(table)
    assert {:ok, %{updated: 1}} = LanceDB.Table.update(table, %{text: "filled"}, filter: "id = 2")

    assert {:ok, %{version: _}} =
             LanceDB.SchemaEvolution.alter_columns(table, text: [nullable: false])

    assert {:error, _} = LanceDB.Table.update(table, %{text: nil})

    assert {:ok, %{version: _}} =
             LanceDB.SchemaEvolution.alter_columns(table, %{text: [nullable: true]})

    assert {:ok, %{updated: 2}} = LanceDB.Table.update(table, %{text: nil})
  end

  test("drops columns and dependent indexes without losing other data", %{
    table: table,
    path: path
  }) do
    for {column, type} <- [id: :btree, text: :fts] do
      assert {:ok, :created} = LanceDB.Index.create(table, column, type: type)
    end

    assert {:ok, %{version: version}} =
             LanceDB.SchemaEvolution.drop_columns(table, [:text, "score"])

    assert version > 1
    assert {:ok, [%LanceDB.Index.Description{columns: ["id"]}]} = LanceDB.Index.list(table)
    assert {:ok, rows} = LanceDB.Query.query(table, order_by: [asc: :id])
    assert Enum.all?(rows, &(Enum.sort(Map.keys(&1)) == ["id", "vector"]))
    assert {:ok, [%{"id" => 1}]} = LanceDB.Query.search(table, [1, 0], limit: 1)
    assert {:ok, conn} = LanceDB.connect(path)
    assert {:ok, reopened} = LanceDB.Connection.open_table(conn, "docs")
    assert {:ok, ^rows} = LanceDB.Query.query(reopened, order_by: [asc: :id])
    assert {:ok, %{inserted: 1}} = LanceDB.Table.add(reopened, [%{id: 3, vector: [1, 1]}])
  end

  test("supports evolution on empty tables", %{conn: conn}) do
    {:ok, table} = LanceDB.Connection.create_empty_table(conn, "empty", id: :int64)

    assert {:ok, %{version: _}} =
             LanceDB.SchemaEvolution.add_columns(table,
               text: "CAST(NULL AS STRING)",
               amount: "CAST(0 AS BIGINT)"
             )

    assert {:ok, %{version: _}} =
             LanceDB.SchemaEvolution.alter_columns(table,
               amount: [rename: :total, nullable: false]
             )

    assert {:ok, %{version: _}} = LanceDB.SchemaEvolution.drop_columns(table, [:text])
    assert {:ok, %{inserted: 1}} = LanceDB.Table.add(table, [%{id: 1, total: 5}])
  end

  test("database errors do not partially change the schema", %{table: table}) do
    assert {:ok, original} = LanceDB.Table.schema(table)

    for changes <- [
          [existing: "score * 2", id: "1"],
          [bad: "missing + 1"],
          [bad: "score +"],
          [bad: "INTERVAL '1 day'"],
          [bad: "NULL"],
          [bad: "make_array(INTERVAL '1 day')"]
        ] do
      assert {:error, _} = LanceDB.SchemaEvolution.add_columns(table, changes)
      assert {:ok, ^original} = LanceDB.Table.schema(table)
    end

    for changes <- [
          [missing: [rename: :new]],
          [id: [rename: :score]],
          [id: [rename: :same], score: [rename: :same]],
          [id: [rename: :score], score: [rename: :id]]
        ] do
      assert {:error, _} = LanceDB.SchemaEvolution.alter_columns(table, changes)
      assert {:ok, ^original} = LanceDB.Table.schema(table)
    end

    for columns <- [[:missing], [:id, :missing], [:id, :text, :score, :vector]] do
      assert {:error, _} = LanceDB.SchemaEvolution.drop_columns(table, columns)
      assert {:ok, ^original} = LanceDB.Table.schema(table)
    end

    assert {:ok, 2} = LanceDB.Table.count_rows(table)
  end

  test "validates evolution input before native calls" do
    for changes <- [
          [],
          %{},
          nil,
          [:bad],
          [id: ""],
          [id: nil],
          [id: 1],
          [id: "a", id: "b"],
          [{"nested.name", "1"}],
          [nil: "1"]
        ] do
      assert_raise ArgumentError, fn -> LanceDB.SchemaEvolution.add_columns(:unused, changes) end
    end

    for changes <- [
          [],
          %{},
          [id: []],
          [id: [type: :unsupported]],
          [id: [nullable: nil]],
          [id: [rename: nil]],
          [id: [rename: "a.b"]],
          [id: [rename: ""]]
        ] do
      assert_raise ArgumentError, fn ->
        LanceDB.SchemaEvolution.alter_columns(:unused, changes)
      end
    end

    for columns <- [[], [:id, "id"], [nil]] do
      assert_raise ArgumentError, fn -> LanceDB.SchemaEvolution.drop_columns(:unused, columns) end
    end
  end
end
