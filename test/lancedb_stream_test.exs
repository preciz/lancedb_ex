defmodule LanceDB.StreamTest do
  use LanceDB.DataCase, async: true
  use ExUnitProperties
  alias LanceDB.Backend, as: Native
  alias LanceDB.Error, as: StreamError
  alias LanceDB.FTS
  alias LanceDB.Query.{Definition, Request}

  setup %{path: path} do
    {:ok, conn} = LanceDB.connect(path)

    schema = [
      id: :int64,
      text: :string,
      at: {:timestamp, :nanosecond, "UTC"},
      vector: {:vector, 2}
    ]

    {:ok, table} = LanceDB.Connection.create_empty_table(conn, "rows", schema)

    rows =
      for id <- 1..205 do
        row(id)
      end

    {:ok, %{inserted: 205}} = LanceDB.Table.add(table, rows)
    %{conn: conn, table: table, schema: schema}
  end

  defp row(id) do
    %{id: id, text: "elixir event", at: id, vector: [id, 1]}
  end

  test("streams all rows lazily and can be enumerated again", %{table: table}) do
    stream = LanceDB.TestSupport.rows(table, limit: nil, batch_size: 7, order_by: [asc: :id])
    assert {:ok, %{inserted: 1}} = LanceDB.Table.add(table, [row(206)])
    assert Enum.count(stream) == 206
    assert Enum.map(stream, & &1["id"]) == Enum.to_list(1..206)
    assert [%{"id" => 1, "at" => "1970-01-01T00:00:00.000000001Z"}] = Enum.take(stream, 1)
    assert Enum.take(stream, 0) == []
    assert {:ok, eager} = LanceDB.Query.query(table)
    assert length(eager) == 100
  end

  test("scan filters, hidden sort columns, pagination and metadata match eager results", %{
    table: table
  }) do
    opts = [
      filter: "id > 10",
      columns: [:at],
      order_by: [desc: :id],
      offset: 3,
      limit: 37,
      with_row_id: true
    ]

    assert {:ok, expected} = LanceDB.Query.query(table, opts)
    assert LanceDB.TestSupport.rows(table, [batch_size: 4] ++ opts) |> Enum.to_list() == expected
    assert Enum.all?(expected, &(Map.keys(&1) == ["_rowid", "at"]))

    assert Enum.to_list(
             LanceDB.TestSupport.rows(table, limit: nil, filter: "id < 0", batch_size: 1)
           ) == []
  end

  test("streams support suspension through zip and enumeration in another process", %{
    table: table
  }) do
    stream =
      LanceDB.TestSupport.rows(table,
        limit: nil,
        batch_size: 2,
        order_by: [asc: :id],
        columns: [:id]
      )

    assert Enum.zip(stream, [:a, :b, :c]) == [
             {%{"id" => 1}, :a},
             {%{"id" => 2}, :b},
             {%{"id" => 3}, :c}
           ]

    task = Task.async(fn -> Enum.take(stream, 2) end)
    assert Task.await(task) == [%{"id" => 1}, %{"id" => 2}]
  end

  test("vector, structured FTS and hybrid streams match eager search", %{table: table}) do
    assert {:ok, :created} = LanceDB.Index.create(table, :text, type: :fts)
    common = [columns: [:id, :at], limit: 5, with_row_id: true, filter: "id < 10", offset: 1]
    assert {:ok, vector} = LanceDB.Query.search(table, [3, 1], common)

    assert Enum.to_list(
             LanceDB.TestSupport.rows(
               table,
               [vector: [3, 1], batch_size: 2] ++ common
             )
           ) == vector

    text = FTS.boolean(must: ["elixir"], must_not: ["obsolete"])
    assert {:ok, fts} = LanceDB.Query.full_text_search(table, text, common)

    assert Enum.to_list(LanceDB.TestSupport.rows(table, [text: text, batch_size: 2] ++ common)) ==
             fts

    assert {:ok, hybrid} = LanceDB.Query.hybrid_search(table, [3, 1], text, common)

    assert Enum.to_list(
             LanceDB.TestSupport.rows(
               table,
               [vector: [3, 1], text: text, batch_size: 2] ++ common
             )
           ) == hybrid

    assert length(Enum.to_list(LanceDB.TestSupport.rows(table, vector: [1, 1]))) == 10
    assert length(Enum.to_list(LanceDB.TestSupport.rows(table, text: "elixir"))) == 10
  end

  test("one enumeration retains its snapshot across later writes", %{table: table}) do
    stream =
      LanceDB.TestSupport.rows(table,
        limit: nil,
        batch_size: 1,
        order_by: [asc: :id],
        columns: [:id]
      )

    rows =
      Enum.map(stream, fn row ->
        if row["id"] == 1 do
          assert {:ok, %{inserted: 1}} = LanceDB.Table.add(table, [row(206)])
        end

        row
      end)

    assert length(rows) == 205
    assert Enum.count(stream) == 206
  end

  test("database errors are deferred and consumer exceptions propagate", %{table: table}) do
    stream = LanceDB.TestSupport.rows(table, limit: nil, filter: "missing_column = 1")
    assert Enum.take(stream, 0) == []
    assert_raise StreamError, fn -> Enum.to_list(stream) end

    assert_raise RuntimeError, "consumer failed", fn ->
      LanceDB.TestSupport.rows(table, batch_size: 1)
      |> Enum.each(fn _ -> raise "consumer failed" end)
    end

    assert [%{"id" => _}] = LanceDB.TestSupport.rows(table) |> Enum.take(1)
  end

  test("deadline includes time between pulls", %{table: table}) do
    assert_raise StreamError, ~r/Query timed out after 100 ms/, fn ->
      LanceDB.TestSupport.rows(table, batch_size: 1, timeout_ms: 100)
      |> Enum.each(fn _ -> Process.sleep(150) end)
    end
  end

  test("native cursor bounds each pull and closes idempotently", %{table: table}) do
    request =
      Definition.new(:scan, columns: [:id], order_by: [asc: :id], limit: nil)
      |> Request.compile()

    {:ok, cursor} = Native.stream_open(table, request, 3)
    assert {:ok, batch} = Native.stream_next_batch(cursor)
    assert LanceDB.Batch.size(batch) == 3
    assert {:ok, [%{"id" => 1}, %{"id" => 2}, %{"id" => 3}]} = LanceDB.Batch.to_rows(batch)
    assert :ok = Native.stream_close(cursor)
    assert :ok = Native.stream_close(cursor)
    assert {:error, %{message: "Stream is closed"}} = Native.stream_next_batch(cursor)
  end

  test("owner death closes a cursor even when another process retains its resource", %{
    table: table
  }) do
    parent = self()

    {pid, monitor} =
      spawn_monitor(fn ->
        request = Definition.new(:scan, limit: nil) |> Request.compile()
        {:ok, cursor} = Native.stream_open(table, request, 1)
        assert {:ok, _} = Native.stream_next_batch(cursor)
        send(parent, {:cursor, cursor})

        receive do
          :stop -> :ok
        end
      end)

    assert_receive {:cursor, cursor}

    assert {:error, %{message: "Stream cursor belongs to another process"}} =
             Native.stream_next_batch(cursor)

    Process.exit(pid, :kill)
    assert_receive {:DOWN, ^monitor, :process, ^pid, :killed}

    # Cleanup may be requested by any holder; pulls remain owned after caller death.
    assert :ok = Native.stream_close(cursor)
  end

  test("row sources consume bounded batches and report partial writes", %{
    conn: conn,
    schema: schema
  }) do
    {:ok, table} = LanceDB.Connection.create_empty_table(conn, "import", schema)

    input =
      Stream.map(1..100, fn id ->
        send(self(), {:consumed, id})

        if id == 4 do
          %{row(id) | at: "invalid"}
        else
          row(id)
        end
      end)

    assert {:error, %LanceDB.Error{details: %{inserted: 3}} = reason} =
             LanceDB.Table.add(
               table,
               LanceDB.Source.rows(input, batch_size: 3),
               commit: :batch
             )

    assert is_struct(reason, LanceDB.Error)
    assert {:ok, 3} = LanceDB.Table.count_rows(table)
    assert_received {:consumed, 6}
    refute_received {:consumed, 7}

    assert {:ok, %{inserted: 0}} =
             LanceDB.Table.add(
               table,
               LanceDB.Source.rows([]),
               []
             )

    assert {:ok, %{inserted: 5}} =
             LanceDB.Table.add(
               table,
               LanceDB.Source.rows(Stream.map(7..11, &row/1), batch_size: 3),
               []
             )

    assert {:ok, 8} = LanceDB.Table.count_rows(table)
  end

  test("upstream ingestion exceptions preserve committed batches", %{conn: conn, schema: schema}) do
    {:ok, table} = LanceDB.Connection.create_empty_table(conn, "exception", schema)

    input =
      Stream.map(1..10, fn id ->
        if id == 4 do
          raise "source failed"
        else
          row(id)
        end
      end)

    assert {:error,
            %LanceDB.Error{
              kind: :source,
              message: "source failed",
              details: %{inserted: 2, commits: 1}
            }} =
             LanceDB.Table.add(
               table,
               LanceDB.Source.rows(input, batch_size: 2),
               commit: :batch
             )

    assert {:ok, 2} = LanceDB.Table.count_rows(table)
  end

  test("stream options validate before enumeration", %{table: table}) do
    for opts <- [
          [batch_size: 0],
          [batch_size: nil],
          [batch_size: 1.5],
          [batch_size: 4_294_967_296],
          [limit: 0],
          [timeout_ms: -1],
          [typo: true],
          [vector: []],
          [text: ""],
          [verbose: true]
        ] do
      assert_raise ArgumentError, fn -> LanceDB.TestSupport.rows(table, opts) end
    end

    assert_raise ArgumentError, fn ->
      LanceDB.Table.add(
        table,
        LanceDB.Source.rows([], batch_size: 0),
        []
      )
    end

    assert_raise ArgumentError, fn ->
      LanceDB.Table.add(
        table,
        LanceDB.Source.rows([]),
        typo: true
      )
    end
  end

  property("batch size does not change scan results or streamed ingestion", %{
    table: source,
    schema: schema
  }) do
    check(
      all(
        batch_size <- integer(1..30),
        offset <- integer(0..150),
        count <- integer(1..50),
        max_runs: 20
      )
    ) do
      with_database(fn conn ->
        {:ok, dest} =
          LanceDB.Connection.create_empty_table(conn, "copy", schema, mode: :overwrite)

        opts = [order_by: [asc: :id], offset: offset, limit: count]
        {:ok, expected} = LanceDB.Query.query(source, opts)
        expected_ids = Enum.to_list(1..205) |> Enum.drop(offset) |> Enum.take(count)
        assert Enum.map(expected, & &1["id"]) == expected_ids
        input = LanceDB.TestSupport.rows(source, [batch_size: batch_size] ++ opts)

        assert {:ok, %{inserted: ^count}} =
                 LanceDB.Table.add(
                   dest,
                   LanceDB.Source.rows(input, batch_size: 7),
                   []
                 )

        assert {:ok, ^expected} = LanceDB.Query.query(dest, order_by: [asc: :id])
      end)
    end
  end
end
