defmodule LanceDB.ArchitectureTest do
  alias LanceDB.Query.Request
  use LanceDB.DataCase, async: false
  use ExUnitProperties
  alias LanceDB.{Batch, Connection, Error, Index, Query, Schema, Source, Table, WriteResult}

  setup %{path: path} do
    {:ok, conn} = Connection.connect(path)
    %{conn: conn, schema: Schema.new(id: :int64)}
  end

  def event(_name, _measurements, metadata, target) do
    send(target, {:operation, metadata})
  end

  test("callback public writes preserve typed results, error labels and independent spans", %{
    conn: conn,
    schema: schema
  }) do
    {:ok, audit} = Connection.create_empty_table(conn, "audit", schema)
    owner = self()
    handler = make_ref()
    :ok = :telemetry.attach(handler, [:lancedb, :operation, :stop], &__MODULE__.event/4, owner)
    on_exit(fn -> :telemetry.detach(handler) end)
    {:ok, direct} = Table.add(audit, [%{id: 0}])

    input =
      Stream.map([1, 2], fn id ->
        assert {:ok, %WriteResult{operation: :add, inserted: 1}} = Table.add(audit, [%{id: id}])
        assert {:error, %Error{operation: :add}} = Table.add(audit, [%{id: "bad"}])
        %{id: id}
      end)

    assert {:ok, table} =
             Connection.create_table(
               conn,
               "callback",
               Source.rows(input, schema: schema, batch_size: 1)
             )

    assert {:error, %Error{operation: :query}} =
             Query.all(Query.where(Query.new(audit), "missing = 1"))

    assert %WriteResult{} = direct
    assert {:ok, 3} = Table.count_rows(audit)
    assert {:ok, 2} = Table.count_rows(table)
    assert_receive {:operation, %{operation: :add, operation_id: first}}
    assert_receive {:operation, %{operation: :add, operation_id: second}}
    assert first != second

    assert {:ok, %WriteResult{operation: :backfill_columns}} =
             LanceDB.SchemaEvolution.backfill_columns(
               table,
               Schema.new(extra: :int64),
               fn batch ->
                 assert {:ok, %WriteResult{operation: :update}} =
                          Table.update(audit, %{id: {:sql, "id"}})

                 {:ok, rows} = Batch.to_rows(batch)
                 Enum.map(rows, &%{extra: &1["id"] + 10})
               end
             )
  end

  property("row lists, row sources and native batches agree, including empty overwrites", %{
    schema: schema
  }) do
    check(
      all(
        ids <- LanceDB.Generators.unique_subset(Enum.to_list(0..20), 0, 6),
        size <- integer(1..4),
        max_runs: 12
      )
    ) do
      with_database(fn conn ->
        rows = Enum.map(ids, &%{id: &1})

        batches =
          Enum.map(Enum.chunk_every(rows, size), fn chunk ->
            {:ok, batch} = Batch.from_rows(chunk, schema)
            batch
          end)

        inputs = [rows, Source.rows(rows, batch_size: size), Source.batches(batches)]

        for {input, index} <- Enum.with_index(inputs) do
          {:ok, table} =
            Connection.create_table(conn, "copy_#{index}", [%{id: -1}],
              schema: schema,
              mode: :overwrite
            )

          assert {:ok, %WriteResult{operation: :add, inserted: count, commits: 1}} =
                   Table.add(table, input, mode: :overwrite)

          assert count == length(ids)
          assert {:ok, result} = Query.query(table, order_by: [asc: :id], limit: nil)
          assert result == Enum.map(Enum.sort(ids), &%{"id" => &1})
          assert {:ok, %WriteResult{inserted: 0, commits: 0}} = Table.add(table, [])
        end
      end)
    end
  end

  test("late source failures separate confirmed commits from processing", %{
    conn: conn,
    schema: schema
  }) do
    producer = fn ->
      Stream.map([1, 2, 3], fn
        3 -> raise "producer failed"
        id -> %{id: id}
      end)
    end

    {:ok, table} = Connection.create_empty_table(conn, "late", schema)

    assert {:error, %Error{kind: :source, details: %{commits: 0, phase: :input}}} =
             Table.add(table, Source.rows(producer.(), batch_size: 1))

    assert {:ok, 0} = Table.count_rows(table)

    assert {:error, %Error{kind: :source, details: %{inserted: 2, commits: 2}}} =
             Table.add(table, Source.rows(producer.(), batch_size: 1), commit: :batch)

    assert {:ok, 2} = Table.count_rows(table)
  end

  test("normalized query builders retain explicit options and select source defaults", %{
    conn: conn
  }) do
    {:ok, table} =
      Connection.create_table(conn, "vectors", [%{id: 1, vector: [1, 0], text: "elixir"}])

    scan = Query.new(table)

    vector =
      scan
      |> Query.nearest_to([1, 0])
      |> Query.options(distance: :cosine, minimum_nprobes: 1, maximum_nprobes: nil)

    vector =
      vector
      |> Query.where("id > 0")
      |> Query.order_by(asc_nulls_first: :id)
      |> Query.select([:id])

    assert Request.compile(vector.definition).request.limit == 10
    assert Request.compile(scan.definition).request.limit == 100

    assert Request.compile(vector.definition).request.controls.probes == %{
             minimum: 1,
             maximum: nil
           }

    assert {:ok, [%{"id" => 1}]} = Query.all(vector)
    assert {:ok, rows} = Query.all(Query.limit(vector, 1))
    assert rows == Enum.to_list(Query.stream(vector, batch_size: 1))
    assert {:ok, :created} = Index.create(table, :text, Index.new(type: :fts))

    hybrid =
      table
      |> Query.new()
      |> Query.hybrid([1, 0], LanceDB.FTS.match("elixir"))
      |> Query.options(normalize: :rank)
      |> Query.limit(1)

    assert {:ok, [%{"id" => 1}]} = Query.all(hybrid)
  end

  test("index definitions are reusable and retain family-specific options", %{conn: conn}) do
    {:ok, table} =
      Connection.create_table(
        conn,
        "indexes",
        for id <- 1..16 do
          %{id: id, vector: [id / 1, 1.0]}
        end
      )

    scalar = Index.new(type: :btree, name: "ids", train: false)
    assert {:ok, :created} = Index.create(table, :id, scalar)
    assert {:ok, :dropped} = Index.drop(table, "ids")
    assert {:ok, :created} = Index.create(table, :id, scalar)
    vector = Index.new(type: :ivf_flat, num_partitions: 1, sample_rate: 1, max_iterations: 2)
    assert {:ok, job} = Index.create_async(table, :vector, vector)
    assert {:ok, :created} = LanceDB.Job.wait(job)
  end

  test("native write failures report known execution boundaries", %{conn: conn, schema: schema}) do
    {:ok, table} = Connection.create_empty_table(conn, "failures", schema)

    assert {:error, %Error{details: %{phase: :validation, commit_outcome: :not_started}}} =
             LanceDB.History.restore(table, 9999)

    assert {:error, %Error{details: %{phase: :execution, commit_outcome: :unknown}}} =
             Table.delete(table, "id = (")
  end

  test("all logical types share creation, batch, query and evolution codecs", %{conn: conn}) do
    primitives = [
      string: "snow 雪",
      binary: <<0, 255>>,
      boolean: true,
      float16: 1.5,
      float32: 1.5,
      float64: 1.5,
      int8: -128,
      int16: -32_768,
      int32: -2_147_483_648,
      int64: -9_223_372_036_854_775_808,
      uint8: 255,
      uint16: 65_535,
      uint32: 4_294_967_295,
      uint64: 18_446_744_073_709_551_615,
      date32: ~D[2024-02-29],
      date64: ~D[2024-02-29]
    ]

    vectors =
      for type <- [:float16, :float32, :float64, :uint8] do
        {{:vector, 2, type}, [1, 2]}
      end

    times =
      for unit <- [:second, :millisecond, :microsecond, :nanosecond],
          zone <- [nil, "UTC"] do
        {{:timestamp, unit, zone}, 0}
      end

    nested = [
      {{:list, :binary}, [<<0, 255>>, nil]},
      {{:struct, name: :string}, %{name: "nested"}}
    ]

    for {{type, value}, index} <- Enum.with_index(primitives ++ vectors ++ times ++ nested) do
      schema = Schema.new([id: :int64, value: type], %{"matrix" => "roundtrip"})
      {:ok, batch} = Batch.from_rows([%{id: 1, value: value}], schema)
      assert {:ok, ^schema} = LanceDB.TestSupport.logical_schema(Batch.schema(batch))

      {:ok, table} =
        Connection.create_table(conn, "type_#{index}", Source.batches([batch], schema: schema))

      assert {:ok, ^schema} = Table.schema(table)
      assert {:ok, expected} = Batch.to_rows(batch)
      assert {:ok, ^expected} = Query.all(Query.new(table))
      assert {:ok, %WriteResult{}} = Table.add(table, [%{id: 2, value: value}])

      assert {:ok, %WriteResult{}} =
               LanceDB.SchemaEvolution.alter_columns(table, value: [rename: :renamed])

      assert {:ok, changed} = Table.schema(table)
      assert changed == Schema.new([id: :int64, renamed: type], schema.metadata)
      assert {:ok, [first, second]} = Query.query(table, order_by: [asc: :id])
      assert first["renamed"] == second["renamed"]
      assert {:ok, %WriteResult{}} = LanceDB.SchemaEvolution.drop_columns(table, [:renamed])
      assert {:ok, 2} = Table.count_rows(table)
    end
  end

  test("every query source survives further builder composition", %{conn: conn}) do
    schema =
      Schema.new(id: :int64, vector: {:vector, 2}, multi: {:list, {:vector, 2}}, text: :string)

    {:ok, table} =
      Connection.create_table(
        conn,
        "builders",
        [
          %{id: 1, vector: [1, 0], multi: [[1, 0]], text: "elixir"},
          %{id: 2, vector: [0, 1], multi: [[0, 1]], text: "elixir"}
        ],
        schema: schema
      )

    {:ok, :created} = Index.create(table, :text, type: :fts)
    scan = Query.new(table)

    for query <- [
          Query.nearest_to(scan, [1, 0]),
          Query.nearest_to_many(scan, [[1, 0], [0, 1]]),
          Query.multivector(scan, [[1, 0], [0, 1]])
        ] do
      column =
        if Request.compile(query.definition).request[:search_mode] == :multivector do
          :multi
        else
          :vector
        end

      query =
        query
        |> Query.options(distance: :cosine, vector_column: column, distance_range: {-1, 3})
        |> Query.select_as(key: "id")
        |> Query.limit(1)

      assert {:ok, rows} = Query.all(query)
      assert rows != []
      assert Enum.all?(rows, &is_integer(&1["key"]))
    end

    for source <- [
          Query.row_ids(scan, [0]),
          Query.row_offsets(scan, [0]),
          Query.text(scan, "elixir")
        ] do
      query = source |> Query.options(with_row_id: true) |> Query.select_as(key: "id")
      assert {:ok, rows} = Query.all(query)
      assert rows != []
      assert Enum.all?(rows, &(is_integer(&1["key"]) and is_integer(&1["_rowid"])))
    end

    for direction <- [:asc_nulls_first, :asc_nulls_last, :desc_nulls_first, :desc_nulls_last] do
      query = scan |> Query.order_by([{direction, :id}]) |> Query.limit(1)
      assert {:ok, [row]} = Query.all(query)

      assert row["id"] ==
               (if direction in [:asc_nulls_first, :asc_nulls_last] do
                  1
                else
                  2
                end)
    end
  end

  test("empty batches preserve the last confirmed incremental commit", %{
    conn: conn,
    schema: schema
  }) do
    {:ok, table} = Connection.create_empty_table(conn, "empty_batch", schema)
    {:ok, batch} = Batch.from_rows([%{id: 1}], schema)
    {:ok, empty} = Batch.from_rows([], schema)

    assert {:ok, %WriteResult{inserted: 1, commits: 1, version: version}} =
             Table.add(table, Source.batches([batch, empty]), commit: :batch)

    assert is_integer(version)
    assert {:ok, ^version} = LanceDB.History.version(table)
  end
end
