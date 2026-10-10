defmodule LanceDB.ExtendedSchemaTest do
  use LanceDB.DataCase, async: true
  use ExUnitProperties

  alias LanceDB.{
    ArrowSchema,
    Batch,
    Connection,
    Field,
    Query,
    Schema,
    SchemaEvolution,
    Source,
    Table
  }

  @cases [
    {:large_string, "雪", "雪"},
    {:large_binary, <<0, 255>>, <<0, 255>>},
    {{:fixed_size_binary, 2}, <<0, 255>>, <<0, 255>>},
    {{:decimal128, 10, 2}, "0012.300", "12.30"},
    {{:decimal256, 76, 4}, "12345678901234567890123456789012345678901234567890.1234",
     "12345678901234567890123456789012345678901234567890.1234"},
    {{:decimal128, 4, -2}, "1200", "1200"},
    {{:time32, :second}, ~T[12:34:56], "12:34:56"},
    {{:time32, :millisecond}, "12:34:56.123", "12:34:56.123"},
    {{:time64, :microsecond}, ~T[12:34:56.123456], "12:34:56.123456"},
    {{:time64, :nanosecond}, "12:34:56.123456789", "12:34:56.123456789"},
    {{:duration, :second}, -1, -1},
    {{:duration, :millisecond}, 123, 123},
    {{:duration, :microsecond}, -9_223_372_036_854_775_808, -9_223_372_036_854_775_808},
    {{:duration, :nanosecond}, 9_223_372_036_854_775_807, 9_223_372_036_854_775_807},
    {{:large_list, :large_binary}, [<<0, 255>>, nil, ""], [<<0, 255>>, nil, ""]},
    {{:dictionary, :int8, :string}, "red", "red"},
    {{:dictionary, :uint16, :int32}, -10, -10},
    {{:dictionary, :int32, :binary}, <<255>>, <<255>>},
    {{:map, :int64, :large_binary}, [{1, <<255>>}, {2, nil}], [{1, <<255>>}, {2, nil}]},
    {{:map, :string, {:list, :int16}}, [{"a", [1, nil]}, {"b", []}], [{"a", [1, nil]}, {"b", []}]}
  ]

  setup %{path: path} do
    {:ok, conn} = LanceDB.connect(path)
    %{conn: conn, path: path}
  end

  test "remaining types preserve values and physical schemas through rows, IPC and storage", %{
    conn: conn,
    path: path
  } do
    for {{type, input, expected}, index} <- Enum.with_index(@cases) do
      schema = Schema.new(id: :int64, value: type)
      rows = [%{id: 1, value: input}, %{id: 2, value: nil}]
      expected_rows = [%{"id" => 1, "value" => expected}, %{"id" => 2, "value" => nil}]
      assert {:ok, batch} = Batch.from_rows(rows, schema), inspect(type)
      assert {:ok, ^expected_rows} = Batch.to_rows(batch), inspect(type)
      assert {:ok, arrow} = Batch.schema(batch)
      assert {:ok, ^schema} = ArrowSchema.to_schema(arrow), inspect(type)
      assert {:ok, ipc} = Batch.to_ipc(batch)
      assert {:ok, [imported]} = Batch.from_ipc(ipc)
      assert {:ok, ^arrow} = Batch.schema(imported)
      assert {:ok, ^expected_rows} = Batch.to_rows(imported)
      name = "type_#{index}"

      assert {:ok, table} =
               Connection.create_table(conn, name, Source.batches([imported], schema: schema),
                 data_storage_version: "2.2"
               ),
             inspect(type)

      assert {:ok, ^schema} = Table.schema(table), inspect(type)
      assert {:ok, ^expected_rows} = Query.query(table, order_by: [asc: :id]), inspect(type)
      assert {:ok, %{inserted: 1}} = Table.add(table, [%{id: 3, value: input}]), inspect(type)
      assert {:ok, %{updated: 1}} = Table.upsert(table, [%{id: 1, value: input}]), inspect(type)
      assert {:ok, reopened_conn} = LanceDB.connect(path)
      assert {:ok, reopened} = Connection.open_table(reopened_conn, name)
      assert {:ok, ^schema} = Table.schema(reopened)

      assert {:ok, [%{"value" => ^expected}]} =
               Query.query(reopened, filter: "id = 3", columns: [:value]),
             inspect(type)
    end
  end

  test "literal updates and nested extended values", %{conn: conn} do
    for {{type, input, expected}, index} <- Enum.with_index(@cases),
        elem_or_self(type) not in [:map, :dictionary] and type != {:decimal128, 4, -2} do
      assert {:ok, table} =
               Connection.create_table(conn, "update_#{index}", [%{id: 1, value: nil}],
                 schema: [id: :int64, value: type]
               )

      assert {:ok, %{updated: 1}} = Table.update(table, %{value: input}), inspect(type)

      assert {:ok, [%{"value" => ^expected}]} = Query.query(table, columns: [:value]),
             inspect(type)
    end

    nested =
      {:struct,
       [
         items: {:large_list, {:map, :int64, {:duration, :nanosecond}}},
         amount: {:decimal128, 5, 2}
       ]}

    schema = Schema.new(value: nested)

    assert {:ok, batch} =
             Batch.from_rows(
               [%{value: %{items: [[{1, -123}], nil, []], amount: "1.20"}}, %{value: nil}],
               schema
             )

    assert {:ok,
            [
              %{"value" => %{"items" => [[{1, -123}], nil, []], "amount" => "1.20"}},
              %{"value" => nil}
            ]} = Batch.to_rows(batch)
  end

  test "invalid definitions and lossy values are rejected before committing", %{conn: conn} do
    for type <- [
          {:decimal128, 39, 0},
          {:decimal256, 77, 0},
          {:decimal128, 0, 0},
          {:decimal128, 3, 4},
          {:decimal128, 3, -129},
          {:time32, :nanosecond},
          {:time64, :second},
          {:duration, :minute},
          {:fixed_size_binary, 0},
          {:dictionary, :string, :int64},
          {:dictionary, :int8, :date32},
          {:map, %Field{name: "entries", type: :string}}
        ] do
      assert_raise ArgumentError, fn -> Schema.new(value: type) end
    end

    for {type, invalids} <- [
          {{:decimal128, 5, 2}, ["1000.00", "0.001", "1e2", "NaN", "1.", "", 1.2, 1]},
          {{:decimal128, 3, -2}, ["123", "1.23", "100000"]},
          {{:time32, :second}, [-1, 86_400, "24:00:00", "23:59:60", "00:00:00.1", "00:00:00Z"]},
          {{:time64, :nanosecond}, [86_400_000_000_000, "00:00:00.0000000001", "x", 1.5]},
          {{:fixed_size_binary, 2}, [<<0>>, <<0, 1, 2>>, "", 12]},
          {{:duration, :second}, [1.5, "1", 18_446_744_073_709_551_615]},
          {{:map, :int64, :int64}, [[{nil, 1}], [{1, 2}, {1, 3}], [1, 2], %{x: 1}]}
        ] do
      name = "invalid_#{System.unique_integer([:positive])}"
      assert {:ok, table} = Connection.create_empty_table(conn, name, value: type)

      for value <- invalids,
          do: assert({:error, _} = Table.add(table, [%{value: value}]), inspect({type, value}))

      assert {:ok, 0} = Table.count_rows(table)
    end
  end

  property "exact decimals and integer map keys round trip through batches and IPC" do
    check all(
            cents <- integer(-9_999_999..9_999_999),
            keys <- uniq_list_of(integer(-1000..1000), max_length: 12),
            max_runs: 40
          ) do
      amount =
        "#{if cents < 0, do: "-", else: ""}#{div(abs(cents), 100)}.#{abs(cents) |> rem(100) |> Integer.to_string() |> String.pad_leading(2, "0")}"

      pairs = Enum.map(keys, &{&1, <<rem(abs(&1), 256)>>})
      schema = Schema.new(amount: {:decimal128, 9, 2}, pairs: {:map, :int64, :binary})
      rows = [%{amount: amount, pairs: pairs}]
      assert {:ok, batch} = Batch.from_rows(rows, schema)
      assert {:ok, [%{"amount" => ^amount, "pairs" => ^pairs}]} = Batch.to_rows(batch)
      assert {:ok, ipc} = Batch.to_ipc(batch)
      assert {:ok, [imported]} = Batch.from_ipc(ipc)
      assert Batch.schema(imported) == Batch.schema(batch)
      assert {:ok, [%{"amount" => ^amount, "pairs" => ^pairs}]} = Batch.to_rows(imported)
    end
  end

  test "schema evolution creates and converts extended primitives", %{conn: conn} do
    assert {:ok, table} =
             Connection.create_table(conn, "evolve", [%{id: 1, value: "12.50"}],
               schema: [id: :int64, value: {:decimal128, 6, 2}]
             )

    assert {:ok, _} = SchemaEvolution.alter_columns(table, value: [type: {:decimal128, 8, 2}])
    assert {:ok, _} = SchemaEvolution.add_columns(table, amount: "CAST(1.25 AS DECIMAL(8, 2))")

    assert {:ok, [%{"value" => "12.50", "amount" => "1.25"}]} =
             Query.query(table, columns: [:value, :amount])
  end

  defp elem_or_self(type) when is_tuple(type), do: elem(type, 0)
  defp elem_or_self(type), do: type

  test "dictionary values can change across batches and literal updates", %{conn: conn} do
    schema = Schema.new(id: :int64, value: {:dictionary, :int8, :string})

    assert {:ok, table} =
             Connection.create_table(conn, "categories", [%{id: 1, value: "red"}], schema: schema)

    assert {:ok, %{inserted: 1}} = Table.add(table, [%{id: 2, value: "green"}])
    assert {:ok, batch} = Batch.from_rows([%{id: 3, value: "blue"}], schema)
    assert {:ok, %{inserted: 1}} = Table.add(table, Source.batches([batch]))
    assert {:ok, %{updated: 1}} = Table.update(table, %{value: "yellow"}, filter: "id = 1")

    assert {:ok, [%{"value" => "yellow"}, %{"value" => "green"}, %{"value" => "blue"}]} =
             Query.query(table, order_by: [asc: :id], columns: [:value])
  end

  test "map metadata and empty values survive storage; unsupported updates fail before writing",
       %{conn: conn} do
    key = %Field{name: "code", type: :int64, nullable: false, metadata: %{"role" => "key"}}

    value = %Field{
      name: "description",
      type: :string,
      nullable: false,
      metadata: %{"role" => "value"}
    }

    schema = Schema.new(id: :int64, pairs: {:map, key, value}, decimal: {:decimal128, 3, -2})

    assert {:ok, table} =
             Connection.create_table(
               conn,
               "map_metadata",
               [
                 %{id: 1, pairs: [{1, "one"}], decimal: "1200"},
                 %{id: 2, pairs: [], decimal: nil}
               ],
               schema: schema,
               data_storage_version: "2.2"
             )

    assert {:ok, ^schema} = Table.schema(table)

    assert {:ok, [%{"pairs" => [{1, "one"}]}, %{"pairs" => []}]} =
             Query.query(table, order_by: [asc: :id], columns: [:pairs])

    assert {:error, _} = Table.update(table, %{pairs: [{2, "two"}]})
    assert {:error, %LanceDB.Error{kind: :unsupported}} = Table.update(table, %{decimal: "1300"})

    assert {:ok, [%{"decimal" => "1200"}]} =
             Query.query(table, filter: "id = 1", columns: [:decimal])

    assert {:error, _} = Table.add(table, [%{id: 3, pairs: [{1, nil}], decimal: nil}])
  end
end
