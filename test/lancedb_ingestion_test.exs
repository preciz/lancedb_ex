defmodule LanceDB.IngestionTest do
  use LanceDB.DataCase, async: true
  use ExUnitProperties
  alias LanceDB.Backend, as: Native

  setup %{path: path} do
    {:ok, conn} = LanceDB.connect(path)
    {:ok, table} = LanceDB.Connection.create_empty_table(conn, "rows", id: :int64, text: :string)
    %{table: table}
  end

  defp rows(ids) do
    Enum.map(ids, &%{id: &1, text: "row #{&1}"})
  end

  test("rich add/delete results identify the actual commit and preserve old return values", %{
    table: table
  }) do
    {:ok, before} = LanceDB.History.version(table)
    assert {:ok, %{inserted: 3, version: version}} = LanceDB.Table.add(table, rows(1..3))
    assert version == before + 1
    assert {:ok, ^version} = LanceDB.History.version(table)
    assert {:ok, %{deleted: 2, version: deleted_version}} = LanceDB.Table.delete(table, "id > 1")
    assert deleted_version == version + 1
    assert {:ok, [%{"id" => 1}]} = LanceDB.Query.query(table, columns: [:id])
    assert {:ok, %{deleted: 0}} = LanceDB.Table.delete(table, "id > 100")
    assert {:ok, %{inserted: 1}} = LanceDB.Table.add(table, rows([4]))
    assert {:ok, %{operation: :delete}} = LanceDB.Table.delete(table, "id = 4")
  end

  test("overwrite, file limits and parallel writes use the existing table handle", %{table: table}) do
    assert {:ok, %{inserted: 2}} = LanceDB.Table.add(table, rows(1..2))

    assert {:ok, %{inserted: 8}} =
             LanceDB.Table.add(table, rows(10..17),
               mode: :overwrite,
               write_parallelism: 1,
               max_rows_per_file: 2,
               max_rows_per_group: 1,
               max_bytes_per_file: 1_048_576
             )

    assert {:ok, actual} = LanceDB.Query.query(table, columns: [:id], order_by: [asc: :id])
    assert Enum.map(actual, & &1["id"]) == Enum.to_list(10..17)

    assert {:ok, %{"fragment_stats" => %{"num_fragments" => fragments}}} =
             LanceDB.Maintenance.stats(table)

    assert fragments >= 4
    assert {:ok, %{inserted: 20}} = LanceDB.Table.add(table, rows(100..119), write_parallelism: 2)
  end

  test("progress messages preserve arbitrary tags and report cumulative work", %{table: table}) do
    tag = {make_ref(), %{purpose: "import"}}

    assert {:ok, %{inserted: 12}} =
             LanceDB.Table.add(table, rows(1..12), progress: {self(), tag}, write_parallelism: 1)

    assert_receive {:lancedb_write_progress, ^tag, progress}, 1000
    assert progress.output_rows > 0
    assert progress.output_bytes >= 0
    assert progress.elapsed_ms >= 0
    assert progress.total_tasks >= 1

    assert_receive {:lancedb_write_progress, ^tag,
                    %{done: true, output_rows: 12, total_rows: 12}},
                   1000
  end

  test("single commit imports all batches and supports overwrite", %{table: table}) do
    assert {:ok, %{inserted: 1}} = LanceDB.Table.add(table, rows([0]))
    {:ok, before} = LanceDB.History.version(table)

    assert {:ok, %{inserted: 11, version: version}} =
             LanceDB.Table.add(
               table,
               LanceDB.Source.rows(rows(1..11), batch_size: 2),
               commit: :once,
               mode: :overwrite,
               write_parallelism: 1
             )

    assert version == before + 1
    assert {:ok, 11} = LanceDB.Table.count_rows(table)
    assert {:ok, 0} = LanceDB.Table.count_rows(table, "id = 0")

    assert {:ok, %{inserted: 0, version: nil}} =
             LanceDB.Table.add(
               table,
               LanceDB.Source.rows([]),
               commit: :once
             )

    assert {:ok, ^version} = LanceDB.History.version(table)
  end

  test("late validation failure and producer exceptions never commit partial single writes", %{
    table: table
  }) do
    assert {:ok, %{inserted: 1}} = LanceDB.Table.add(table, rows([0]))
    {:ok, before} = LanceDB.History.version(table)
    input = rows(1..8) ++ [%{id: "wrong", text: "bad"}]

    assert {:error, %LanceDB.Error{details: %{inserted: 0}}} =
             LanceDB.Table.add(
               table,
               LanceDB.Source.rows(input, batch_size: 1),
               commit: :once
             )

    assert {:ok, ^before} = LanceDB.History.version(table)
    assert {:ok, 1} = LanceDB.Table.count_rows(table)

    producer =
      Stream.map(1..8, fn
        8 -> raise "producer failed"
        id -> hd(rows([id]))
      end)

    assert {:error,
            %LanceDB.Error{kind: :source, message: "producer failed", details: %{inserted: 0}}} =
             LanceDB.Table.add(
               table,
               LanceDB.Source.rows(producer, batch_size: 1),
               commit: :once,
               mode: :overwrite
             )

    assert {:ok, ^before} = LanceDB.History.version(table)
    assert {:ok, [%{"id" => 0}]} = LanceDB.Query.query(table, columns: [:id])
  end

  test("batch mode retains partial results and returns the last committed version", %{
    table: table
  }) do
    {:ok, before} = LanceDB.History.version(table)

    assert {:ok, %{inserted: 5, version: version}} =
             LanceDB.Table.add(
               table,
               LanceDB.Source.rows(rows(1..5), batch_size: 2),
               commit: :batch
             )

    assert version == before + 3

    assert {:error, %LanceDB.Error{details: %{inserted: 2}}} =
             LanceDB.Table.add(
               table,
               LanceDB.Source.rows(rows(6..7) ++ [%{id: nil, text: 1}], batch_size: 2),
               commit: :batch
             )

    assert {:ok, 7} = LanceDB.Table.count_rows(table)

    assert {:ok, %{inserted: 0, version: nil}} =
             LanceDB.Table.add(
               table,
               LanceDB.Source.rows([]),
               []
             )
  end

  test("producer ownership, abort, historical tables and malformed native options", %{
    table: table
  }) do
    {:ok, writer} = Native.ingest_open(table, %{}, nil)

    assert {:error, %{message: "Writer belongs to another process"}} =
             Task.async(fn -> Native.ingest_finish(writer) end) |> Task.await()

    assert {:error, %{message: "Writer belongs to another process"}} =
             Task.async(fn -> Native.ingest_abort(writer) end) |> Task.await()

    assert {:ok, :ok} = Native.ingest_abort(writer)
    assert {:error, %{message: "Writer is closed"}} = Native.ingest_finish(writer)
    assert {:error, _} = Native.ingest_open(table, %{mode: "bad"}, nil)
    assert {:error, _} = Native.ingest_open(table, %{write_parallelism: 0}, nil)
    assert {:error, _} = LanceDB.Native.ingest_open(table.resource, "not JSON", nil)
    {:ok, version} = LanceDB.History.version(table)
    assert {:ok, table} = LanceDB.History.snapshot(table, version)
    assert {:error, _} = LanceDB.Table.add(table, rows([1]))

    assert {:error, %LanceDB.Error{details: %{inserted: 0}}} =
             LanceDB.Table.add(
               table,
               LanceDB.Source.rows(rows([1])),
               commit: :once
             )
  end

  test("producer death aborts an unfinished native writer", %{table: table}) do
    parent = self()

    {pid, monitor} =
      spawn_monitor(fn ->
        {:ok, writer} = Native.ingest_open(table, %{}, {parent, :owner_death})
        {:ok, :ok} = Native.ingest_push(writer, [%{"id" => 1, "text" => "row"}])
        send(parent, {:writer, writer})

        receive do
          :exit -> :ok
        end
      end)

    assert_receive {:writer, writer}
    send(pid, :exit)
    assert_receive {:DOWN, ^monitor, :process, ^pid, :normal}
    assert is_reference(writer)
    assert_receive {:lancedb_write_progress, :owner_death, %{done: true}}, 2000
    assert {:ok, 0} = LanceDB.Table.count_rows(table)
  end

  test("invalid controls fail before consuming input", %{table: table}) do
    tag = make_ref()

    input =
      Stream.map(rows([1]), fn row ->
        send(self(), {tag, :consumed})
        row
      end)

    for opts <- [
          [mode: :bad],
          [return: :bad],
          [progress: self()],
          [write_parallelism: 0],
          [max_rows_per_file: -1],
          [max_rows_per_group: 1.5],
          [max_bytes_per_file: nil],
          [unknown: 1]
        ] do
      assert_raise ArgumentError, fn ->
        LanceDB.Table.add(table, LanceDB.Source.rows(input), opts)
      end
    end

    for opts <- [
          [commit: :bad],
          [mode: :overwrite, commit: :batch],
          [batch_size: 0],
          [progress: {"pid", :tag}]
        ] do
      assert_raise ArgumentError, fn ->
        LanceDB.Table.add(
          table,
          LanceDB.Source.rows(input, batch_size: Keyword.get(opts, :batch_size, 1024)),
          Keyword.delete(opts, :batch_size)
        )
      end
    end

    assert_raise ArgumentError, fn -> LanceDB.Table.delete(table, "true", return: :bad) end
    refute_received {^tag, :consumed}
    assert {:ok, 0} = LanceDB.Table.count_rows(table)
  end

  property("single-commit results and data are independent of chunk boundaries") do
    check(
      all(
        ids <- uniq_list_of(integer(1..1000), min_length: 1, max_length: 20),
        size <- integer(1..8),
        max_runs: 20
      )
    ) do
      with_database(fn conn ->
        {:ok, table} =
          LanceDB.Connection.create_empty_table(conn, "rows", id: :int64, text: :string)

        {:ok, before} = LanceDB.History.version(table)

        assert {:ok, %{inserted: count, version: version}} =
                 LanceDB.Table.add(
                   table,
                   LanceDB.Source.rows(rows(ids), batch_size: size),
                   commit: :once,
                   mode: :overwrite
                 )

        assert count == length(ids)
        assert version == before + 1
        assert {:ok, actual} = LanceDB.Query.query(table, columns: [:id], order_by: [asc: :id])
        assert Enum.map(actual, & &1["id"]) == Enum.sort(ids)
      end)
    end
  end
end
