defmodule LanceDB.TimestampTest do
  use LanceDB.DataCase, async: true
  use ExUnitProperties

  setup %{path: path} do
    {:ok, conn} = LanceDB.connect(path)
    %{conn: conn, path: path}
  end

  test "all precisions preserve timestamps, nulls, and stored schemas", %{conn: conn, path: path} do
    schema = [
      id: :int64,
      seconds: {:timestamp, :second},
      millis: {:timestamp, :millisecond},
      micros: {:timestamp, :microsecond, "UTC"},
      nanos: {:timestamp, :nanosecond, "UTC"}
    ]

    row = %{
      id: 1,
      seconds: ~N[1969-12-31 23:59:59],
      millis: -1,
      micros: ~U[1969-12-31 23:59:59.999999Z],
      nanos: "1969-12-31T23:59:59.999999999Z"
    }

    assert {:ok, table} = LanceDB.Connection.create_table(conn, "events", [row], schema: schema)
    assert {:ok, [result]} = LanceDB.Query.query(table)

    assert result == %{
             "id" => 1,
             "seconds" => "1969-12-31T23:59:59",
             "millis" => "1969-12-31T23:59:59.999",
             "micros" => "1969-12-31T23:59:59.999999Z",
             "nanos" => "1969-12-31T23:59:59.999999999Z"
           }

    assert {:ok, %{inserted: 1}} = LanceDB.Table.add(table, [%{result | "id" => 2}])

    assert {:ok, %{inserted: 1}} =
             LanceDB.Table.add(table, [
               %{id: 3, seconds: nil, millis: nil, micros: nil, nanos: nil}
             ])

    assert {:ok, [%{"nanos" => nil}]} = LanceDB.Query.query(table, filter: "id = 3")
    assert {:ok, reopened_conn} = LanceDB.connect(path)
    assert {:ok, reopened} = LanceDB.Connection.open_table(reopened_conn, "events")
    assert {:ok, %LanceDB.Schema{fields: fields}} = LanceDB.Table.schema(reopened)
    assert Enum.find(fields, &(&1.name == "seconds")).type == {:timestamp, :second, nil}

    assert Enum.find(fields, &(&1.name == "nanos")).type ==
             {:timestamp, :nanosecond, "UTC"}

    assert {:ok, [^result]} = LanceDB.Query.query(reopened, filter: "id = 1")
  end

  test "timezone metadata preserves instants and disambiguates daylight saving time", %{
    conn: conn
  } do
    schema = [id: :int64, at: {:timestamp, :microsecond, "Europe/Vienna"}]
    assert {:ok, table} = LanceDB.Connection.create_empty_table(conn, "zoned", schema)

    assert {:ok, %{inserted: 2}} =
             LanceDB.Table.add(table, [
               %{id: 1, at: "2024-10-27T02:30:00+02:00"},
               %{id: 2, at: "2024-10-27T02:30:00+01:00"}
             ])

    assert {:ok, [%{"at" => early}, %{"at" => late}]} =
             LanceDB.Query.query(table, order_by: [asc: :at])

    assert early == "2024-10-27T00:30:00Z"
    assert late == "2024-10-27T01:30:00Z"

    assert {:ok, %{inserted: 1}} =
             LanceDB.Table.add(table, [%{id: 3, at: ~U[2024-07-01 12:00:00Z]}])

    assert {:ok, [%{"at" => "2024-07-01T12:00:00Z"}]} =
             LanceDB.Query.query(table, filter: "id = 3")

    assert {:ok, %LanceDB.Schema{fields: fields}} = LanceDB.Table.schema(table)

    assert Enum.find(fields, &(&1.name == "at")).type ==
             {:timestamp, :microsecond, "Europe/Vienna"}

    assert {:ok, :created} = LanceDB.Index.create(table, :at, type: :btree)

    assert {:ok, 2} =
             LanceDB.Table.count_rows(table, "at >= to_timestamp('2024-10-27T00:00:00Z')")

    assert {:error, _} = LanceDB.Table.add(table, [%{id: 4, at: ~N[2024-10-27 02:30:00]}])
  end

  test "updates, upserts and conditional merges use the same timestamp rules", %{conn: conn} do
    schema = [id: :int64, at: {:timestamp, :nanosecond, "UTC"}]
    assert {:ok, table} = LanceDB.Connection.create_empty_table(conn, "writes", schema)
    assert {:ok, %{inserted: 1}} = LanceDB.Table.upsert(table, [%{id: 1, at: 1}])
    assert {:ok, %{updated: 1}} = LanceDB.Table.update(table, %{at: -1})
    assert {:ok, [%{"at" => "1969-12-31T23:59:59.999999999Z"}]} = LanceDB.Query.query(table)

    assert {:ok, %{updated: 1}} =
             LanceDB.Table.update(table, %{at: ~U[2024-01-01 00:00:00.123456Z]})

    assert {:ok, [%{"at" => "2024-01-01T00:00:00.123456Z"}]} = LanceDB.Query.query(table)

    assert {:ok, %{updated: 1, inserted: 1}} =
             LanceDB.Table.merge(table, [%{id: 1, at: 10}, %{id: 2, at: 20}],
               on: :id,
               when_matched_update_all: "source.at < target.at",
               when_not_matched_insert_all: true
             )

    assert {:ok, [%{"id" => 2}]} =
             LanceDB.Query.query(table, filter: "at > to_timestamp_nanos(15)", columns: [:id])

    assert {:ok, %{updated: 1}} =
             LanceDB.Table.update(table, %{at: {:sql, "to_timestamp_nanos(30)"}},
               filter: "id = 2"
             )

    assert {:ok, [%{"at" => "1970-01-01T00:00:00.000000030Z"}]} =
             LanceDB.Query.query(table, filter: "id = 2")

    assert {:ok, %{updated: 2}} = LanceDB.Table.update(table, %{at: nil})
    assert {:ok, 2} = LanceDB.Table.count_rows(table, "at IS NULL")
  end

  test "invalid values and precision loss fail before creating or mutating a table", %{conn: conn} do
    schema = [
      {:id, :int64},
      %{name: :at, type: {:timestamp, :millisecond, "UTC"}, nullable: false}
    ]

    assert {:ok, table} = LanceDB.Connection.create_empty_table(conn, "strict", schema)
    good = %{id: 1, at: "2024-01-01T00:00:00.123000000Z"}

    for invalid <- [
          nil,
          true,
          1.5,
          "not a timestamp",
          "2024-02-30T00:00:00Z",
          "2024-01-01",
          "2024-01-01T00:00:00",
          "2024-01-01T00:00:00.123001Z",
          "2024-01-01T00:00:00.1230000000Z",
          "2024-01-01T00:00:00+01:60",
          "2016-12-31T23:59:60Z",
          ~N[2024-01-01 00:00:00],
          9_223_372_036_854_775_807
        ] do
      bad = %{id: 2, at: invalid}

      assert {:error, _} =
               LanceDB.Connection.create_table(conn, "invalid", [good, bad], schema: schema)

      assert {:error, _} = LanceDB.Table.add(table, [good, bad])
      assert {:error, _} = LanceDB.Table.upsert(table, [good, bad])

      assert {:error, _} =
               LanceDB.Table.merge(table, [good, bad], when_not_matched_insert_all: true)

      assert {:error, _} = LanceDB.Table.update(table, %{at: invalid})
      assert {:ok, 0} = LanceDB.Table.count_rows(table)
    end

    assert {:ok, ["strict"]} = LanceDB.Connection.table_names(conn)
    assert {:ok, %{inserted: 1}} = LanceDB.Table.add(table, [good])
  end

  test "timestamps work in search results and SQL-added columns", %{conn: conn} do
    assert {:ok, table} =
             LanceDB.Connection.create_table(conn, "evolve", [
               %{id: 1, text: "event", vector: [1, 0]}
             ])

    assert {:ok, %{version: _}} =
             LanceDB.SchemaEvolution.add_columns(table, at: "to_timestamp_micros(1)")

    assert {:ok, %{inserted: 1}} =
             LanceDB.Table.add(table, [
               %{id: 2, text: "event", vector: [0, 1], at: ~N[2024-01-01 00:00:00]}
             ])

    assert {:error, _} =
             LanceDB.Table.add(table, [
               %{id: 3, text: "event", vector: [0, 1], at: ~U[2024-01-01 00:00:00Z]}
             ])

    assert {:ok, [%{"at" => "1970-01-01T00:00:00.000001"}]} =
             LanceDB.Query.search(table, [1, 0], limit: 1)

    assert {:ok, :created} = LanceDB.Index.create(table, :text, type: :fts)

    assert {:ok, [%{"at" => "2024-01-01T00:00:00"}]} =
             LanceDB.Query.full_text_search(table, "event", filter: "id = 2")

    assert {:ok, [%{"at" => "2024-01-01T00:00:00"}]} =
             LanceDB.Query.hybrid_search(table, [0, 1], "event", filter: "id = 2")

    assert {:ok, %{updated: 1}} =
             LanceDB.Table.update(table, %{at: ~N[2025-01-01 00:00:00]}, filter: "id = 2")
  end

  test "schema options and inference remain explicit", %{conn: conn} do
    for type <- [
          :timestamp,
          {:timestamp, :minute},
          {:timestamp, :microsecond, :utc},
          {:timestamp, :second, ""}
        ] do
      assert_raise ArgumentError, fn ->
        LanceDB.Connection.create_empty_table(conn, "invalid", at: type)
      end
    end

    assert {:error, _} =
             LanceDB.Connection.create_empty_table(conn, "invalid",
               at: {:timestamp, :second, "Invalid/Timezone"}
             )

    assert {:error, _} =
             LanceDB.Connection.create_table(conn, "invalid", [%{at: ~U[2024-01-01 00:00:00Z]}])

    assert {:ok, text} =
             LanceDB.Connection.create_table(conn, "text", [%{at: "2024-01-01T00:00:00Z"}])

    assert {:ok, %LanceDB.Schema{fields: [%LanceDB.Field{type: :string}]}} =
             LanceDB.Table.schema(text)
  end

  property "microsecond epochs round trip exactly through Elixir dates and persisted ISO output" do
    check all(ticks <- integer(-10_000_000_000_000..10_000_000_000_000), max_runs: 40) do
      with_database(fn conn ->
        {:ok, table} =
          LanceDB.Connection.create_empty_table(conn, "property",
            id: :int64,
            at: {:timestamp, :microsecond, "UTC"}
          )

        date = DateTime.from_unix!(ticks, :microsecond)
        assert {:ok, _} = LanceDB.Table.upsert(table, [%{id: 1, at: date}])
        assert {:ok, [%{"at" => iso} = row]} = LanceDB.Query.query(table)
        assert {:ok, actual, 0} = DateTime.from_iso8601(iso)
        assert DateTime.to_unix(actual, :microsecond) == ticks
        assert {:ok, _} = LanceDB.Table.upsert(table, [row])
        assert {:ok, [^row]} = LanceDB.Query.query(table)
        assert {:ok, %{updated: 1}} = LanceDB.Table.update(table, %{at: date})
        assert {:ok, [^row]} = LanceDB.Query.query(table)
      end)
    end
  end
end
