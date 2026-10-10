defmodule LanceDB.NestedSchemaTest do
  use LanceDB.DataCase, async: true
  use ExUnitProperties

  setup %{path: path} do
    {:ok, db} = LanceDB.connect(path)
    %{db: db, path: path}
  end

  defp schema do
    [
      id: :int64,
      tags: {:list, :string},
      details:
        {:struct,
         title: :string,
         events: {:list, {:struct, at: {:timestamp, :nanosecond, "Europe/Paris"}, size: :int64}},
         matrix: {:list, {:list, :float64}},
         embedding: {:vector, 2}}
    ]
  end

  defp row(id) do
    %{
      id: id,
      tags: ["elixir", nil, "雪 '"],
      details: %{
        title: "hello",
        events: [%{at: "1900-01-01T00:00:00.000000001Z", size: 9_007_199_254_740_993}, nil],
        matrix: [[1.25, nil], [], nil],
        embedding: [1, 0]
      }
    }
  end

  test("nested schemas persist and records round trip through reads, projections and streams", %{
    db: db,
    path: path
  }) do
    assert {:ok, table} =
             LanceDB.Connection.create_table(db, "nested", [row(1)], schema: schema())

    assert {:ok, [result]} = LanceDB.Query.query(table)

    assert result["details"]["events"] == [
             %{"at" => "1900-01-01T00:00:00.000000001Z", "size" => 9_007_199_254_740_993},
             nil
           ]

    assert result["details"]["matrix"] == [[1.25, nil], [], nil]
    assert {:ok, %{inserted: 1}} = LanceDB.Table.add(table, [%{result | "id" => 2}])

    assert {:ok, %{inserted: 2}} =
             LanceDB.Table.add(table, [
               %{id: 3, tags: [], details: nil},
               %{id: 4, tags: nil, details: nil}
             ])

    assert {:ok, rows} = LanceDB.Query.query(table, order_by: [asc: :id])

    assert Enum.to_list(LanceDB.TestSupport.rows(table, batch_size: 1, order_by: [asc: :id])) ==
             rows

    assert {:ok, [%{"title" => "hello", "tags" => ["elixir", nil, "雪 '"]}]} =
             LanceDB.Query.query(table,
               select: [title: "details.title", tags: "tags"],
               filter: "id = 1"
             )

    assert {:ok, 2} = LanceDB.Table.count_rows(table, "array_contains(tags, 'elixir')")
    assert {:ok, %LanceDB.Schema{fields: fields} = stored_schema} = LanceDB.Table.schema(table)
    details = Enum.find(fields, &(&1.name == "details"))
    assert Enum.find(elem(details.type, 1), &(&1.name == "title")).nullable == true
    assert elem(Enum.find(fields, &(&1.name == "tags")).type, 1).type == :string

    assert {:ok, ^stored_schema} =
             LanceDB.TestSupport.logical_schema(LanceDB.TestSupport.output_schema(table))

    assert {:ok, reopened_db} = LanceDB.connect(path)
    assert {:ok, reopened} = LanceDB.Connection.open_table(reopened_db, "nested")
    assert {:ok, ^rows} = LanceDB.Query.query(reopened, order_by: [asc: :id])
  end

  test("literal updates replace nested values exactly, including empty lists and nulls", %{db: db}) do
    {:ok, table} = LanceDB.Connection.create_table(db, "updates", [row(1)], schema: schema())

    replacement = %{
      title: "'); DELETE FROM updates; --",
      events: [%{at: ~U[2024-01-01 00:00:00.123456Z], size: 9_007_199_254_740_993}],
      matrix: [[], [2.5, nil]],
      embedding: [0, 1]
    }

    assert {:ok, %{updated: 1}} = LanceDB.Table.update(table, %{details: replacement, tags: []})
    assert {:ok, [%{"details" => actual, "tags" => []}]} = LanceDB.Query.query(table)
    assert actual["title"] == replacement.title
    assert actual["matrix"] == replacement.matrix

    assert actual["events"] == [
             %{"at" => "2024-01-01T00:00:00.123456Z", "size" => 9_007_199_254_740_993}
           ]

    assert actual["embedding"] == [0.0, 1.0]
    assert {:ok, %{updated: 1}} = LanceDB.Table.update(table, %{tags: [nil, "new"], details: nil})
    assert {:ok, [%{"details" => nil, "tags" => [nil, "new"]}]} = LanceDB.Query.query(table)
  end

  test("nested values work through merges, batched ingestion and historical handles", %{db: db}) do
    {:ok, table} = LanceDB.Connection.create_empty_table(db, "writes", schema())

    assert {:ok, %{inserted: 2}} =
             LanceDB.Table.add(
               table,
               LanceDB.Source.rows(Stream.map(1..2, &row/1), batch_size: 1),
               []
             )

    assert {:ok, saved} = LanceDB.History.version(table)
    updated = put_in(row(1), [:details, :title], "updated")
    assert {:ok, %{updated: 1, inserted: 1}} = LanceDB.Table.upsert(table, [updated, row(3)])

    assert {:ok, %{updated: 1, inserted: 1}} =
             LanceDB.Table.merge(table, [row(1), row(4)],
               when_matched_update_all: true,
               when_not_matched_insert_all: true
             )

    assert {:ok, table} = LanceDB.History.snapshot(table, saved)
    assert {:ok, [%{"details" => %{"title" => "hello"}}]} = LanceDB.Query.take_offsets(table, [0])
    assert {:error, _} = LanceDB.Table.update(table, %{tags: []})
    assert {:ok, table} = LanceDB.History.latest(table)
    assert {:ok, 4} = LanceDB.Table.count_rows(table)
  end

  test("nested shape and nullability errors leave data and version unchanged", %{db: db}) do
    fields = [
      id: :int64,
      data:
        {:list, {:struct, [%{name: :value, type: :int64, nullable: false}]}, item_nullable: false}
    ]

    {:ok, table} = LanceDB.Connection.create_empty_table(db, "strict", fields)
    assert {:ok, %{inserted: 1}} = LanceDB.Table.add(table, [%{id: 1, data: []}])
    assert {:ok, version} = LanceDB.History.version(table)

    for invalid <- [
          nil,
          %{},
          %{value: nil},
          %{value: "1"},
          %{value: 1, extra: true},
          %{"value" => 1, value: 2}
        ] do
      records = [%{id: 2, data: [%{value: 1}, invalid]}]
      assert {:error, _} = LanceDB.Table.add(table, records)
      assert {:error, _} = LanceDB.Table.upsert(table, records)
      assert {:error, _} = LanceDB.Table.merge(table, records, when_not_matched_insert_all: true)
      assert {:error, _} = LanceDB.Connection.create_table(db, "invalid", records, schema: fields)
    end

    for data <- [[nil], [%{}], [%{value: nil}], [%{value: 1.5}], "not a list"] do
      assert {:error, _} = LanceDB.Table.update(table, %{data: data})
    end

    assert {:ok, ^version} = LanceDB.History.version(table)
    assert {:ok, [%{"id" => 1, "data" => []}]} = LanceDB.Query.query(table)
  end

  test(
    "required struct children accept values and reject unsupported null parents before writing",
    %{db: db}
  ) do
    fields = [id: :int64, data: {:struct, [%{name: :value, type: :string, nullable: false}]}]
    {:ok, table} = LanceDB.Connection.create_empty_table(db, "required", fields)
    assert {:ok, %{inserted: 1}} = LanceDB.Table.add(table, [%{id: 1, data: %{value: "first"}}])
    assert {:error, reason} = LanceDB.Table.update(table, %{data: %{value: "next"}})
    assert reason.message =~ "required child fields"
    assert {:ok, %{updated: 1}} = LanceDB.Table.upsert(table, [%{id: 1, data: %{value: "next"}}])
    assert {:ok, version} = LanceDB.History.version(table)
    assert {:error, reason} = LanceDB.Table.add(table, [%{id: 2, data: nil}])
    assert reason.message =~ "null struct"
    assert {:error, reason} = LanceDB.Table.update(table, %{data: nil})
    assert reason.message =~ "null struct"
    assert {:ok, ^version} = LanceDB.History.version(table)
    assert {:ok, [%{"data" => %{"value" => "next"}}]} = LanceDB.Query.query(table)
  end

  test("nested timestamps reject lost precision and vector children retain their validation", %{
    db: db
  }) do
    fields = [
      id: :int64,
      data: {:struct, at: {:timestamp, :millisecond, "UTC"}, vector: {:vector, 2}}
    ]

    {:ok, table} = LanceDB.Connection.create_empty_table(db, "precise", fields)

    for data <- [
          %{at: "2024-01-01T00:00:00.000001Z", vector: [1, 0]},
          %{at: ~N[2024-01-01 00:00:00], vector: [1, 0]},
          %{at: 0, vector: [1]},
          %{at: 0, vector: [nil, 0]}
        ] do
      assert {:error, _} = LanceDB.Table.add(table, [%{id: 1, data: data}])
      assert {:error, _} = LanceDB.Table.update(table, %{data: data})
    end
  end

  test("search and diagnostics retain nested projections", %{db: db}) do
    fields = schema() ++ [text: :string, vector: {:vector, 2}]
    record = Map.merge(row(1), %{text: "elixir", vector: [1, 0]})
    {:ok, table} = LanceDB.Connection.create_table(db, "search", [record], schema: fields)
    {:ok, :created} = LanceDB.Index.create(table, :text, type: :fts)

    for {selectors, search} <- [
          {[vector: [1, 0], vector_column: :vector],
           fn opts ->
             LanceDB.Query.search(table, [1, 0], Keyword.put(opts, :vector_column, :vector))
           end},
          {[text: "elixir"],
           fn opts -> LanceDB.Query.full_text_search(table, "elixir", opts) end},
          {[vector: [1, 0], text: "elixir", vector_column: :vector],
           fn opts ->
             LanceDB.Query.hybrid_search(
               table,
               [1, 0],
               "elixir",
               Keyword.put(opts, :vector_column, :vector)
             )
           end}
        ] do
      opts = [select: [payload: "details", labels: "tags"], with_row_id: true]
      assert {:ok, [result]} = search.(opts)
      assert result["payload"]["title"] == "hello"
      assert result["labels"] == record.tags
      assert Enum.to_list(LanceDB.TestSupport.rows(table, selectors ++ opts)) == [result]

      assert {:ok, %LanceDB.ArrowSchema{fields: fields}} =
               LanceDB.TestSupport.output_schema(table, selectors ++ opts)

      assert match?({:struct, [_ | _]}, Enum.find(fields, &(&1.name == "payload")).type)
      assert {:ok, analysis} = LanceDB.TestSupport.analyze_plan(table, selectors ++ opts)
      assert String.ends_with?(analysis, "Output: rows=1\n")
    end
  end

  test("SQL schema additions can produce supported nested types", %{db: db}) do
    {:ok, table} = LanceDB.Connection.create_table(db, "evolve", [%{id: 1, text: "hello"}])

    assert {:ok, %{version: _}} =
             LanceDB.SchemaEvolution.add_columns(table,
               tags: "make_array(text)",
               detail: "named_struct('count', id)"
             )

    assert {:ok, [%{"tags" => ["hello"], "detail" => %{"count" => 1}}]} =
             LanceDB.Query.query(table)

    assert {:ok, %{inserted: 1}} =
             LanceDB.Table.add(table, [%{id: 2, text: "next", tags: [], detail: %{count: 2}}])
  end

  test("struct field names and literal strings cannot become SQL expressions", %{db: db}) do
    names = ["odd'name", "back`tick", "雪"]
    fields = [id: :int64, data: {:struct, Enum.map(names, &{&1, :string})}]
    value = Map.new(names, &{&1, "'); SELECT 1; --"})
    {:ok, table} = LanceDB.Connection.create_empty_table(db, "names", fields)
    assert {:ok, %{inserted: 1}} = LanceDB.Table.add(table, [%{id: 1, data: value}])
    assert {:ok, %{updated: 1}} = LanceDB.Table.update(table, %{data: value})
    assert {:ok, [%{"data" => ^value}]} = LanceDB.Query.query(table)
  end

  test("unrepresentable SQL type names fail without silently nulling fields", %{db: db}) do
    fields = [id: :int64, data: {:struct, [{"double\"quote", :string}]}]
    value = %{"double\"quote" => "preserve me"}

    {:ok, table} =
      LanceDB.Connection.create_table(db, "escaped", [%{id: 1, data: value}], schema: fields)

    {:ok, version} = LanceDB.History.version(table)
    assert {:error, reason} = LanceDB.Table.update(table, %{data: value})
    assert reason.message =~ "type parser"
    assert {:ok, ^version} = LanceDB.History.version(table)
    assert {:ok, [%{"data" => ^value}]} = LanceDB.Query.query(table)
  end

  test "nested schema definitions reject invalid child types, names and options" do
    for type <- [
          {:list, :unknown},
          {:list, :string, nullable: false},
          {:list, :string, item_nullable: :yes},
          struct: [],
          struct: [name: :string, name: :int64],
          struct: [{"", :string}],
          struct: [child: {:struct, value: :unknown}]
        ] do
      assert_raise ArgumentError, fn ->
        LanceDB.Connection.create_empty_table(:unused, "bad", data: type)
      end
    end

    assert {:error, _} = LanceDB.Records.prepare([%{data: %{value: 1}}], :infer)
    assert {:error, _} = LanceDB.Records.prepare([%{data: ["text"]}], :infer)
    assert {:error, _} = LanceDB.Records.prepare([%{data: [%URI{}]}], :typed)
  end

  property("nested records retain values across inserts, literal updates and streaming") do
    fields = [id: :int64, data: {:list, {:struct, text: :string, values: {:list, :int64}}}]

    number =
      one_of([
        integer(),
        member_of([-9_223_372_036_854_775_808, 9_223_372_036_854_775_807, 9_007_199_254_740_993])
      ])

    item =
      fixed_map(%{
        text: one_of([constant(nil), string(:utf8, max_length: 15)]),
        values: list_of(one_of([constant(nil), number]), max_length: 4)
      })

    check(all(data <- list_of(one_of([constant(nil), item]), max_length: 5), max_runs: 35)) do
      with_database(fn db ->
        {:ok, table} = LanceDB.Connection.create_empty_table(db, "property", fields)

        expected_data =
          Enum.map(data, fn
            nil -> nil
            %{text: text, values: values} -> %{"text" => text, "values" => values}
          end)

        expected = %{"id" => 1, "data" => expected_data}
        assert {:ok, _} = LanceDB.Table.upsert(table, [%{id: 1, data: data}])
        assert {:ok, [^expected]} = LanceDB.Query.query(table)
        assert {:ok, %{updated: 1}} = LanceDB.Table.update(table, %{data: data})
        assert {:ok, [^expected]} = LanceDB.Query.query(table)
        assert Enum.to_list(LanceDB.TestSupport.rows(table, batch_size: 1)) == [expected]
      end)
    end
  end
end
