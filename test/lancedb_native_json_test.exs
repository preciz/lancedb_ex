defmodule LanceDB.NativeJSONTest do
  use LanceDB.DataCase, async: true
  alias LanceDB.{ArrowSchema, Batch, Connection, Field, Query, Schema, Source, Table}
  alias LanceDB.JSON, as: J

  setup %{path: path} do
    {:ok, conn} = LanceDB.connect(path)
    %{conn: conn}
  end

  test "native JSON values distinguish SQL null, JSON null, and tagged user objects", %{
    conn: conn
  } do
    values = [
      nil,
      J.null(),
      %{},
      [],
      "plain text",
      42,
      true,
      false,
      -9_223_372_036_854_775_808,
      18_446_744_073_709_551_615,
      1.25,
      %{"__lancedb_binary__" => "ff"},
      %{"__lancedb_json__" => "null"},
      %{nested: [1, nil, "雪", %{ok: true}]},
      J.new(%{explicit: true})
    ]

    expected = [
      nil,
      J.null(),
      %{},
      [],
      "plain text",
      42,
      true,
      false,
      -9_223_372_036_854_775_808,
      18_446_744_073_709_551_615,
      1.25,
      %{"__lancedb_binary__" => "ff"},
      %{"__lancedb_json__" => "null"},
      %{"nested" => [1, nil, "雪", %{"ok" => true}]},
      %{"explicit" => true}
    ]

    rows = values |> Enum.with_index() |> Enum.map(fn {value, id} -> %{id: id, data: value} end)
    schema = Schema.new(id: :int64, data: :json)
    assert {:ok, batch} = Batch.from_rows(rows, schema)
    assert {:ok, arrow} = Batch.schema(batch)

    assert %Field{type: :large_binary, metadata: %{"ARROW:extension:name" => "lance.json"}} =
             Enum.at(arrow.fields, 1)

    assert {:ok, ^schema} = ArrowSchema.to_schema(arrow)
    assert {:ok, decoded} = Batch.to_rows(batch)
    assert Enum.map(decoded, & &1["data"]) == expected
    assert {:ok, ipc} = Batch.to_ipc(batch)
    assert {:ok, [imported]} = Batch.from_ipc(ipc)
    assert {:ok, ^decoded} = Batch.to_rows(imported)

    assert {:ok, table} =
             Connection.create_table(conn, "json", Source.batches([batch], schema: schema))

    assert {:ok, ^schema} = Table.schema(table)
    query = Query.new(table, limit: nil, order_by: [asc: :id])
    assert {:ok, ^decoded} = Query.all(query)
    assert {:ok, output_schema} = Query.schema(query)

    [output_batch | _] = Enum.to_list(Query.stream_batches(query))
    assert {:ok, ^output_schema} = Batch.schema(output_batch)

    assert {:ok, ^schema} = ArrowSchema.to_schema(output_schema)

    assert {:ok, copy} =
             Connection.create_table(
               conn,
               "copy",
               Source.batches(Query.stream_batches(query), schema: schema)
             )

    assert {:ok, ^decoded} = copy |> Query.new(limit: nil, order_by: [asc: :id]) |> Query.all()
    assert {:ok, %{updated: 1}} = Table.update(table, %{data: %{updated: true}}, filter: "id = 0")

    assert {:ok, [%{"data" => %{"updated" => true}}]} =
             Query.query(table, filter: "id = 0", columns: [:data])

    assert {:ok, %{updated: 1}} = Table.upsert(table, [%{id: 1, data: %{merged: true}}])
  end

  test "JSON nests in lists, structs and maps", %{conn: conn} do
    schema =
      Schema.new(value: {:struct, [array: {:large_list, :json}, map: {:map, :string, :json}]})

    rows = [
      %{value: %{array: [%{x: 1}, J.null(), nil], map: [{"x", %{a: [nil]}}]}},
      %{value: nil}
    ]

    assert {:ok, table} =
             Connection.create_table(conn, "nested", rows,
               schema: schema,
               data_storage_version: "2.2"
             )

    assert {:ok, ^schema} = Table.schema(table)

    assert {:ok,
            [
              %{
                "value" => %{
                  "array" => [%{"x" => 1}, %J{value: nil}, nil],
                  "map" => [{"x", %{"a" => [nil]}}]
                }
              },
              %{"value" => nil}
            ]} = Query.query(table)
  end

  test "JSON validation rejects invalid strings, keys, aliases and structs" do
    for value <- [
          <<255>>,
          18_446_744_073_709_551_616,
          %{1 => 2},
          %{"a" => 2, a: 1},
          ~D[2024-01-01],
          {:a, 1}
        ] do
      assert_raise ArgumentError, fn -> J.new(value) end
    end

    assert J.new(%{a: [nil]}) == %J{value: %{"a" => [nil]}}
    assert {:error, _} = Batch.from_rows([%{data: <<255>>}], Schema.new(data: :json))

    assert {:error, _} =
             Batch.from_rows([%{data: %J{value: ~D[2024-01-01]}}], Schema.new(data: :json))
  end

  test "native JSON functions and all search sources retain the output schema contract", %{
    conn: conn
  } do
    alias LanceDB.Expr, as: E

    rows = [
      %{id: 1, data: %{title: "Elixir"}, text: "elixir beam", vector: [0, 0]},
      %{id: 2, data: J.null(), text: "elixir language", vector: [1, 0]}
    ]

    assert {:ok, table} =
             Connection.create_table(conn, "search_json", rows,
               schema: [id: :int64, data: :json, text: :string, vector: {:vector, 2}]
             )

    assert {:ok, :created} = LanceDB.Index.create(table, :text, type: :fts)
    title = E.call(:json_get_string, [E.col(:data), "title"])

    assert {:ok, [%{"title" => "Elixir"}]} =
             Query.query(table, filter: E.eq(title, "Elixir"), select: [title: title])

    for source <- [
          :scan,
          {:vector, [0, 0]},
          {:text, "elixir"},
          {:hybrid, [0, 0], "elixir"},
          {:take, :row_offsets, [0, 1]}
        ] do
      query = Query.new(table, source, columns: [:id, :data])
      assert {:ok, expected} = Query.schema(query)
      batches = query |> Query.stream_batches(batch_size: 1) |> Enum.to_list()
      assert batches != []

      for batch <- batches do
        assert {:ok, ^expected} = Batch.schema(batch), inspect(source)
        assert {:ok, [_ | _]} = Batch.to_rows(batch)
      end

      assert {:ok, _} = Query.analyze(query)
    end
  end
end
