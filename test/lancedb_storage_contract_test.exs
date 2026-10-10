defmodule LanceDB.StorageContractTest do
  use LanceDB.DataCase, async: true

  alias LanceDB.{
    ArrowSchema,
    Batch,
    Blob,
    Connection,
    Error,
    History,
    Metadata,
    Query,
    Schema,
    SchemaEvolution,
    Source,
    Table
  }

  setup %{path: path} do
    {:ok, conn} = Connection.connect(path)
    %{conn: conn}
  end

  test "field metadata replacement preserves logical types on empty and populated tables", %{
    conn: conn
  } do
    for type <- [:blob, :json], populated <- [false, true] do
      name = "metadata_#{type}_#{populated}"
      value = if type == :blob, do: <<0, 255>>, else: %{"hello" => [1, true]}
      schema = Schema.new(id: :int64, value: type)
      {:ok, table} = Connection.create_empty_table(conn, name, schema)
      if populated, do: assert({:ok, _} = Table.add(table, [%{id: 1, value: value}]))

      for changes <- [%{description: "payload"}, %{}] do
        assert {:ok, _} = Metadata.update_field(table, :value, changes, replace: true)
        {:ok, reopened} = Connection.open_table(conn, name)
        {:ok, actual} = Table.schema(reopened)
        assert Enum.find(actual.fields, &(&1.name == "value")).type == type
        if type == :blob, do: assert({:ok, ["value"]} = Blob.columns(reopened))
        assert {:ok, _} = Table.add(reopened, [%{id: 2, value: value}])

        if type == :blob do
          {:ok, rows} = Query.query(reopened, with_row_id: true)
          assert {:ok, values} = Blob.fetch(reopened, :value, Enum.map(rows, & &1["_rowid"]))
          assert Enum.all?(values, &(&1 == value))
        else
          assert {:ok, rows} = Query.query(reopened)
          assert Enum.all?(rows, &(&1["value"] == value))
        end
      end
    end
  end

  test "metadata cannot introduce, remove or change type markers", %{conn: conn} do
    {:ok, table} = Connection.create_empty_table(conn, "markers", id: :int64, value: :blob)
    {:ok, version} = History.version(table)

    for {field, changes} <- [
          {:value, %{"ARROW:extension:name" => nil}},
          {:value, %{"ARROW:extension:name" => "other"}},
          {:id, %{"ARROW:extension:name" => "lance.blob.v2"}},
          {:id, %{"lance-encoding:blob" => "true"}}
        ],
        replace <- [false, true] do
      assert {:error,
              %Error{
                kind: :unsupported,
                details: %{phase: :validation, commit_outcome: :not_started}
              }} =
               Metadata.update_field(table, field, changes, replace: replace)

      assert {:ok, ^version} = History.version(table)
    end
  end

  test "SQL schema additions apply the same persistence policy as schema definitions", %{
    conn: conn
  } do
    {:ok, table} = Connection.create_table(conn, "sql_schema", [%{id: 1}])
    {:ok, version} = History.version(table)

    expression =
      "arrow_cast(make_array(1.0, 2.0), 'FixedSizeList(2 x non-null Float32, field: ''custom'')')"

    query = table |> Query.new() |> Query.select_as(vector: expression)
    assert {:ok, output} = Query.schema(query)
    assert {:error, %Error{kind: :unsupported}} = ArrowSchema.to_schema(output)
    assert {:ok, [_]} = Query.all(query)

    assert {:error, %Error{kind: :unsupported, details: %{commit_outcome: :not_started}}} =
             SchemaEvolution.add_columns(table, vector: expression)

    assert {:ok, ^version} = History.version(table)
    assert {:ok, %Schema{fields: [%{name: "id"}]}} = Table.schema(table)

    assert {:ok, _} =
             SchemaEvolution.add_columns(table,
               vector: "arrow_cast(make_array(1.0, 2.0), 'FixedSizeList(2 x Float32)')"
             )

    assert {:ok, %Schema{fields: [_, %{type: {:vector, 2}}]}} = Table.schema(table)
  end

  test "casts retain field metadata in the persistence policy", %{conn: conn} do
    for type <- [:blob, :json] do
      {:ok, table} = Connection.create_empty_table(conn, "cast_#{type}", id: :int64, value: type)
      {:ok, version} = History.version(table)

      assert {:error, %Error{kind: :unsupported, details: %{commit_outcome: :not_started}}} =
               SchemaEvolution.alter_columns(table, value: [type: :int64])

      assert {:ok, ^version} = History.version(table)
      assert {:ok, %Schema{fields: [_, %{type: ^type}]}} = Table.schema(table)
    end
  end

  test "blob table updates report their capability limit before executing", %{conn: conn} do
    {:ok, table} =
      Connection.create_table(conn, "blob_updates", [%{id: 1, value: "payload"}],
        schema: [id: :int64, value: :blob]
      )

    {:ok, version} = History.version(table)

    for changes <- [%{id: 2}, %{value: nil}, %{value: "new"}] do
      assert {:error,
              %Error{
                kind: :unsupported,
                details: %{phase: :validation, commit_outcome: :not_started}
              }} = Table.update(table, changes)

      assert {:ok, ^version} = History.version(table)
      assert {:ok, [%{"id" => 1}]} = Query.query(table, columns: [:id])
    end

    assert {:ok, %{updated: 1}} = Table.upsert(table, [%{id: 1, value: "new"}])
  end

  test "map uniqueness is checked after narrowing and accepted values can be reinserted", %{
    conn: conn
  } do
    for {type, duplicate} <- [
          {:float16, [{2048, "a"}, {2049, "b"}]},
          {:float32, [{16_777_216, "a"}, {16_777_217, "b"}]},
          {:float64, [{1, "a"}, {1.0, "b"}]},
          {:float32, [{-0.0, "a"}, {0, "b"}]}
        ] do
      schema = Schema.new(id: :int64, value: {:map, type, :string})

      assert {:error, %Error{kind: :invalid_data}} =
               Batch.from_rows([%{id: 1, value: duplicate}], schema)

      name = "map_#{System.unique_integer([:positive])}"

      {:ok, table} =
        Connection.create_empty_table(conn, name, schema, data_storage_version: "2.2")

      {:ok, version} = History.version(table)
      assert {:error, %Error{}} = Table.add(table, [%{id: 1, value: duplicate}])
      assert {:ok, ^version} = History.version(table)
      good = [%{id: 1, value: [{1, "a"}, {2, "b"}]}, %{id: 2, value: nil}]
      assert {:ok, _} = Table.add(table, good)
      {:ok, rows} = Query.query(table)
      assert {:ok, _} = Table.add(table, rows)
      {:ok, batch} = Batch.from_rows(rows, schema)
      assert {:ok, _} = Table.add(table, Source.batches([batch]))
      assert {:ok, 6} = Table.count_rows(table)
    end
  end
end
