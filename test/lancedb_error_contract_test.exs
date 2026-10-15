defmodule LanceDB.ErrorContractTest do
  use LanceDB.DataCase, async: false
  alias LanceDB.{Connection, Error, History, Query, SchemaEvolution}

  setup %{path: path} do
    {:ok, conn} = Connection.connect(path)
    {:ok, table} = Connection.create_table(conn, "rows", [%{id: 1}])
    handler = make_ref()
    events = for suffix <- [:start, :stop, :exception], do: [:lancedb, :operation, suffix]
    :ok = :telemetry.attach_many(handler, events, &__MODULE__.event/4, self())
    on_exit(fn -> :telemetry.detach(handler) end)
    %{conn: conn, table: table}
  end

  def event(event, _measurements, metadata, pid), do: send(pid, {event, metadata})

  test "connection argument failures raise inside their operation spans", %{conn: conn} do
    for value <- [nil, 123, "", " \n", <<255>>] do
      assert_argument_error(:connect, fn -> Connection.connect(value) end)
      assert_argument_error(:drop_table, fn -> Connection.drop_table(conn, value) end)
    end

    for storage <- [nil, [], :invalid, %URI{}] do
      assert_argument_error(:connect, fn -> Connection.connect("./unused", storage) end)
    end
  end

  test "empty table validation and successful creation each emit one span", %{conn: conn} do
    for schema <- [nil, [], [id: :unknown]] do
      assert_argument_error(:create_table, fn ->
        Connection.create_empty_table(conn, "empty", schema)
      end)
    end

    assert {:ok, _} = Connection.create_empty_table(conn, "empty", id: :int64)
    assert_stop(:create_table, :ok)
  end

  test "structured cleanup failures preserve the primary error and confirmed commits", %{
    table: table
  } do
    {:ok, version} = History.version(table)

    cleanup_error = %Error{
      kind: :storage,
      operation: :stream_close,
      message: "storage cleanup failed",
      details: %{phase: :execution, context: "original"}
    }

    input =
      Stream.resource(
        fn -> [%{id: 2}, %{id: "invalid"}] end,
        fn [row | rest] -> {[row], rest} end,
        fn _ -> raise cleanup_error end
      )

    assert {:error,
            %Error{
              kind: :invalid_data,
              operation: :add,
              details: %{
                inserted: 1,
                commits: 1,
                version: committed,
                cleanup_error: ^cleanup_error
              }
            }} =
             LanceDB.Table.add(table, LanceDB.Source.rows(input, batch_size: 1), commit: :batch)

    assert committed == version + 1
    assert {:ok, ^committed} = History.version(table)
    assert {:ok, [%{"id" => 1}, %{"id" => 2}]} = Query.query(table, order_by: [asc: :id])
  end

  test "backfill validates mapper arity inside its operation span", %{table: table} do
    for mapper <- [nil, :mapper, fn -> [] end, fn _, _ -> [] end] do
      assert_argument_error(:backfill_columns, fn ->
        SchemaEvolution.backfill_columns(table, [extra: :int64], mapper)
      end)
    end
  end

  test "backfill snapshot planning failures keep their kind and do not commit", %{table: table} do
    {:ok, version} = History.version(table)
    owner = self()

    assert {:error,
            %Error{
              kind: :invalid_data,
              operation: :backfill_columns,
              details: %{inserted: 0, commits: 0}
            }} =
             SchemaEvolution.backfill_columns(
               table,
               [extra: :int64],
               fn _ -> send(owner, :mapper_called) end,
               read_columns: [:missing]
             )

    refute_received :mapper_called
    assert {:ok, ^version} = History.version(table)
    assert {:ok, [%{"id" => 1}]} = Query.query(table)
  end

  test "ordinary mapper failures remain source errors and do not commit", %{table: table} do
    {:ok, version} = History.version(table)

    assert {:error,
            %Error{
              kind: :source,
              operation: :backfill_columns,
              message: "mapper failed",
              details: %{phase: :input, inserted: 0, commits: 0}
            }} =
             SchemaEvolution.backfill_columns(table, [extra: :int64], fn _ ->
               raise "mapper failed"
             end)

    assert {:ok, ^version} = History.version(table)
    assert {:ok, [%{"id" => 1}]} = Query.query(table)
  end

  test "stream and backfill errors retain their origins through the native boundary" do
    # Isolate NIF failures in a child VM while exercising the real public API,
    # backend, stream finalizers and ingestion cleanup.
    code = """
    Code.compiler_options(ignore_module_conflict: true)
    defmodule LanceDB.Native do
      def stream_open(_, _, _) do
        if Process.get(:failure) == :open,
          do: {:error, %{kind: :execution, message: "open failed", details: %{}}},
          else: {:ok, make_ref()}
      end
      def stream_next_batch(_) do
        case Process.get(:failure) do
          :rows -> {:ok, make_ref()}
          kind -> {:error, %{kind: kind, message: "snapshot failed", details: %{phase: :execution}}}
        end
      end
      def batch_rows(_),
        do: {:error, %{kind: :invalid_data, message: "invalid JSONB", details: %{}}}
      def stream_close(_) do
        Process.put(:cursor_closed, true)
        :ok
      end
      def table_version(_), do: {:ok, 1}
      def snapshot_table(_, _, _), do: {:ok, make_ref()}
      def columns_stream_open(_, _, _), do: {:ok, make_ref()}
      def ingest_abort(_) do
        Process.put(:writer_aborted, true)
        {:ok, :ok}
      end
      def ingest_finish(_), do: raise("failed input must not commit")
    end

    table = %LanceDB.Table{resource: make_ref()}
    for {failure, operation} <- [open: :stream_open, rows: :batch_rows, storage: :stream_next_batch] do
      Process.put(:failure, failure)
      try do
        table |> LanceDB.Query.new() |> LanceDB.Query.stream() |> Enum.to_list()
        raise "stream should fail"
      rescue
        error in LanceDB.Error -> ^operation = error.operation
      end
      if failure != :open, do: true = Process.get(:cursor_closed)
    end

    for kind <- [:busy, :storage] do
      Process.put(:failure, kind)
      Process.put(:writer_aborted, false)
      {:error, %LanceDB.Error{
        kind: ^kind,
        operation: :backfill_columns,
        details: %{phase: :execution, inserted: 0, commits: 0}
      }} = LanceDB.SchemaEvolution.backfill_columns(table, [extra: :int64], fn _ ->
        raise "mapper must not run after a read failure"
      end)
      true = Process.get(:writer_aborted)
    end
    IO.puts("stream origins and backfill read failures preserved")
    """

    {output, status} =
      System.cmd(
        System.find_executable("elixir"),
        ["--erl", "+S 2:2", "-pa", Path.join(Mix.Project.build_path(), "lib/*/ebin"), "-e", code],
        stderr_to_stdout: true
      )

    assert status == 0, output
    assert output =~ "stream origins and backfill read failures preserved"
  end

  defp assert_argument_error(operation, fun) do
    assert_raise ArgumentError, fun
    assert_receive {[:lancedb, :operation, :start], %{operation: ^operation, operation_id: id}}

    assert_receive {[:lancedb, :operation, :exception],
                    %{operation: ^operation, operation_id: ^id, kind: :error}}

    refute_received {[:lancedb, :operation, _], _}
  end

  defp assert_stop(operation, status, kind \\ nil) do
    assert_receive {[:lancedb, :operation, :start], %{operation: ^operation, operation_id: id}}

    assert_receive {[:lancedb, :operation, :stop],
                    %{
                      operation: ^operation,
                      operation_id: ^id,
                      status: ^status,
                      error_kind: ^kind
                    }}

    refute_received {[:lancedb, :operation, _], _}
  end
end
