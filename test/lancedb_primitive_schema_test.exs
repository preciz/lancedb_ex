defmodule LanceDB.PrimitiveSchemaTest do
  use LanceDB.DataCase, async: true
  use ExUnitProperties

  @integers [
    {:int8, -128, 127},
    {:int16, -32_768, 32_767},
    {:int32, -2_147_483_648, 2_147_483_647},
    {:int64, -9_223_372_036_854_775_808, 9_223_372_036_854_775_807},
    {:uint8, 0, 255},
    {:uint16, 0, 65_535},
    {:uint32, 0, 4_294_967_295},
    {:uint64, 0, 18_446_744_073_709_551_615}
  ]
  setup %{path: path} do
    {:ok, conn} = LanceDB.connect(path)
    %{conn: conn, path: path}
  end

  test("integer widths preserve boundaries across writes, updates, merges and reopening", %{
    conn: conn,
    path: path
  }) do
    for {type, min, max} <- @integers do
      name = Atom.to_string(type)

      assert {:ok, table} =
               LanceDB.Connection.create_empty_table(conn, name, id: type, value: type)

      assert {:ok, %{inserted: 2}} =
               LanceDB.Table.add(table, [%{id: min, value: min}, %{id: max, value: max}])

      assert {:ok, [%{"value" => ^min}, %{"value" => ^max}]} =
               LanceDB.Query.query(table, order_by: [asc: :id])

      assert {:ok, %{updated: 1}} =
               LanceDB.Table.update(table, %{value: max},
                 filter:
                   "id = arrow_cast('#{min}', '#{type |> Atom.to_string() |> String.replace_prefix("uint", "UInt") |> String.replace_prefix("int", "Int")}')"
               )

      assert {:ok, %{updated: 1, inserted: 0}} =
               LanceDB.Table.upsert(table, [%{id: max, value: min}])

      assert {:ok, reopened_conn} = LanceDB.connect(path)
      assert {:ok, reopened} = LanceDB.Connection.open_table(reopened_conn, name)

      assert {:ok, [%{"value" => ^max}, %{"value" => ^min}]} =
               LanceDB.Query.query(reopened, order_by: [asc: :id])

      for invalid <- [min - 1, max + 1, 1.5, "1", true] do
        assert {:error, _} = LanceDB.Table.add(table, [%{id: min, value: invalid}])
      end

      assert {:ok, 2} = LanceDB.Table.count_rows(table)
    end
  end

  test("date32 and date64 preserve calendar days through all literal writes", %{conn: conn}) do
    assert {:ok, table} =
             LanceDB.Connection.create_table(
               conn,
               "dates",
               [%{id: 1, day: ~D[2024-02-29], millis: "0001-01-01"}],
               schema: [id: :uint8, day: :date32, millis: :date64]
             )

    assert {:ok, [%{"day" => "2024-02-29", "millis" => "0001-01-01"}]} =
             LanceDB.Query.query(table)

    assert {:ok, %{inserted: 2}} =
             LanceDB.Table.add(table, [
               %{id: 2, day: -1, millis: -86_400_000},
               %{id: 3, day: nil, millis: nil}
             ])

    assert {:ok, %{updated: 1}} =
             LanceDB.Table.update(table, %{day: ~D[9999-12-31], millis: "9999-12-31"},
               filter: "id = 1"
             )

    assert {:ok, %{updated: 1, inserted: 0}} =
             LanceDB.Table.upsert(table, [%{id: 2, day: 0, millis: 0}])

    assert {:ok, [%{"day" => "9999-12-31", "millis" => "9999-12-31"}]} =
             LanceDB.Query.query(table, filter: "day > DATE '2024-01-01'")

    assert {:ok, [%{"day" => "1970-01-01", "millis" => "1970-01-01"}]} =
             LanceDB.Query.query(table, filter: "id = 2")

    for bad <- ["2023-02-29", "0000-01-01", "2024-01-01T00:00:00", 2_932_897, -719_163, 1.5] do
      assert {:error, _} = LanceDB.Table.add(table, [%{id: 4, day: bad, millis: nil}])
      assert {:error, _} = LanceDB.Table.update(table, %{day: bad})
    end

    for bad <- [1, -1, 253_402_300_800_000] do
      assert {:error, _} = LanceDB.Table.add(table, [%{id: 4, day: nil, millis: bad}])
    end

    assert {:ok, :created} = LanceDB.Index.create(table, :day, type: :btree)
  end

  test(
    "binary values survive nested, projected, streamed and merged results without marker collisions",
    %{conn: conn}
  ) do
    schema = [
      id: :int64,
      payload: :binary,
      data: {:struct, bytes: {:list, :binary}, day: :date32, count: :uint64},
      marker: {:struct, [{"__lancedb_binary__", :string}]}
    ]

    bytes = <<0, 255, 128, 39, 92>>

    row = %{
      id: 1,
      payload: bytes,
      data: %{
        bytes: [bytes, "", nil, "雪"],
        day: ~D[2024-02-29],
        count: 18_446_744_073_709_551_615
      },
      marker: %{"__lancedb_binary__" => "ff"}
    }

    assert {:ok, table} = LanceDB.Connection.create_table(conn, "bytes", [row], schema: schema)
    assert {:ok, [result]} = LanceDB.Query.query(table)
    assert result["payload"] == bytes

    assert result["data"] == %{
             "bytes" => [bytes, "", nil, "雪"],
             "day" => "2024-02-29",
             "count" => 18_446_744_073_709_551_615
           }

    assert result["marker"] == %{"__lancedb_binary__" => "ff"}
    assert [^result] = LanceDB.TestSupport.rows(table, batch_size: 1) |> Enum.to_list()
    assert {:ok, [^result]} = LanceDB.Query.take_offsets(table, [0])
    assert {:ok, [%{"copy" => ^bytes}]} = LanceDB.Query.query(table, select: [copy: "payload"])

    assert {:ok, %{updated: 1}} =
             LanceDB.Table.update(table, %{payload: "", data: %{row.data | bytes: [<<254>>, nil]}})

    assert {:ok, [%{"payload" => "", "data" => %{"bytes" => [<<254>>, nil]}}]} =
             LanceDB.Query.query(table)

    assert {:ok, %{updated: 1, inserted: 0}} = LanceDB.Table.upsert(table, [row])
    assert {:ok, destination} = LanceDB.Connection.create_empty_table(conn, "copy", schema)

    assert {:ok, %{inserted: 1}} =
             LanceDB.Table.add(
               destination,
               LanceDB.Source.rows(LanceDB.TestSupport.rows(table), batch_size: 1),
               []
             )

    assert {:ok, [^result]} = LanceDB.Query.query(destination)

    assert {:ok, %{updated: 1}} =
             LanceDB.Table.update(table, %{payload: nil, data: nil, marker: nil})

    assert {:ok, [%{"payload" => nil, "data" => nil}]} = LanceDB.Query.query(table)

    assert {:ok, string_table} =
             LanceDB.Connection.create_empty_table(conn, "text", value: :string)

    assert {:error, _} = LanceDB.Table.add(string_table, [%{value: bytes}])
  end

  test("binary results stay binary across vector, FTS, hybrid and computed queries", %{conn: conn}) do
    bytes = <<255, 0, 128>>
    schema = [id: :int16, text: :string, vector: {:vector, 2}, payload: :binary]

    {:ok, table} =
      LanceDB.Connection.create_table(
        conn,
        "search",
        [%{id: 1, text: "elixir", vector: [1, 0], payload: bytes}],
        schema: schema
      )

    assert {:ok, :created} = LanceDB.Index.create(table, :text, type: :fts)
    assert {:ok, [%{"payload" => ^bytes}]} = LanceDB.Query.search(table, [1, 0])
    assert {:ok, [%{"payload" => ^bytes}]} = LanceDB.Query.full_text_search(table, "elixir")
    assert {:ok, [%{"payload" => ^bytes}]} = LanceDB.Query.hybrid_search(table, [1, 0], "elixir")

    assert [%{"payload" => ^bytes}] =
             LanceDB.TestSupport.rows(table, vector: [1, 0], batch_size: 1) |> Enum.to_list()

    assert {:ok, [%{"bytes" => <<255>>}]} =
             LanceDB.Query.query(table, select: [bytes: "decode('ff', 'hex')"])

    assert {:ok, %{version: _}} =
             LanceDB.SchemaEvolution.add_columns(table,
               added: "decode('ff', 'hex')",
               day: "DATE '2024-02-29'",
               small: "CAST(127 AS TINYINT)"
             )

    assert {:ok, [%{"added" => <<255>>, "day" => "2024-02-29", "small" => 127}]} =
             LanceDB.Query.query(table)

    assert {:ok, %{updated: 1}} = LanceDB.Table.update(table, %{small: -128})
  end

  property("arbitrary byte sequences and unsigned integers round trip together") do
    check(
      all(
        bytes <- list_of(binary(max_length: 128), max_length: 8),
        id <- integer(0..18_446_744_073_709_551_615),
        max_runs: 20
      )
    ) do
      with_database(fn conn ->
        {:ok, table} =
          LanceDB.Connection.create_empty_table(conn, "property",
            id: :uint64,
            bytes: {:list, :binary}
          )

        assert {:ok, %{inserted: 1}} = LanceDB.Table.add(table, [%{id: id, bytes: bytes}])
        assert {:ok, [%{"id" => ^id, "bytes" => ^bytes}]} = LanceDB.Query.query(table)
      end)
    end
  end
end
