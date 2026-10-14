defmodule LanceDB.OptimizationTest do
  use LanceDB.DataCase, async: false

  alias LanceDB.{
    Connection,
    Error,
    Execution,
    History,
    Index,
    Maintenance,
    Query,
    Schema,
    Source,
    Table
  }

  @moduletag :standard_optimization

  setup %{path: path} do
    {:ok, conn} = Connection.connect(path)
    %{conn: conn}
  end

  test "standard optimization handles empty and unindexed tables without creating indexes", %{
    conn: conn
  } do
    for {name, rows} <- [{"empty", []}, {"unindexed", [%{"id" => 1}]}] do
      {:ok, table} = Connection.create_table(conn, name, rows, schema: Schema.new(id: :int64))
      assert {:ok, stats} = Maintenance.optimize(table)
      assert Enum.sort(Map.keys(stats)) == ["cleanup", "compaction"]

      assert Enum.sort(Map.keys(stats["compaction"])) ==
               ["files_added", "files_removed", "fragments_added", "fragments_removed"]

      assert Enum.sort(Map.keys(stats["cleanup"])) ==
               [
                 "bytes_removed",
                 "data_files_removed",
                 "deletion_files_removed",
                 "index_files_removed",
                 "old_versions",
                 "transaction_files_removed"
               ]

      assert Enum.all?(Map.values(stats["compaction"]), &(&1 == 0))
      assert Enum.all?(Map.values(stats["cleanup"]), &(&1 == 0))
      assert {:ok, ^rows} = Query.query(table)
      assert {:ok, []} = Index.list(table)
    end
  end

  test "compaction and index optimization preserve rows and schema across reopen", %{
    conn: conn,
    path: path
  } do
    rows = for id <- 1..32, do: %{id: id, text: "before", vector: [id / 32, 1.0]}
    {:ok, table} = Connection.create_table(conn, "indexed", rows)
    {:ok, :created} = Index.create(table, :id, type: :btree, name: "ids")
    {:ok, :created} = Index.create(table, :text, type: :fts, name: "words")

    {:ok, :created} =
      Index.create(table, :vector, type: :ivf_flat, name: "vectors", num_partitions: 1)

    for id <- 33..38 do
      {:ok, _} = Table.add(table, [%{id: id, text: "appended", vector: [id / 32, 1.0]}])
    end

    {:ok, _} = Table.delete(table, "id = 1")
    {:ok, _} = Table.update(table, %{text: "updated"}, filter: "id = 2")
    {:ok, before} = Query.query(table, order_by: [asc: :id])
    {:ok, schema} = Table.schema(table)

    assert {:ok, %{"compaction" => compaction, "cleanup" => cleanup}} =
             Maintenance.optimize(table)

    assert compaction["fragments_removed"] > compaction["fragments_added"]
    assert compaction["files_added"] > 0
    assert cleanup["old_versions"] == 0
    assert {:ok, ^before} = Query.query(table, order_by: [asc: :id])
    assert {:ok, ^schema} = Table.schema(table)

    for name <- ["ids", "words", "vectors"] do
      assert {:ok, %{"num_unindexed_rows" => 0}} = Index.stats(table, name)
    end

    assert {:ok, [%{"id" => 38}]} = Query.search(table, [38 / 32, 1.0], columns: [:id], limit: 1)
    assert {:ok, [%{"id" => 2}]} = Query.full_text_search(table, "updated", columns: [:id])
    {:ok, fresh} = Connection.connect(path)
    assert {:ok, reopened} = Connection.open_table(fresh, "indexed")
    assert {:ok, ^before} = Query.query(reopened, order_by: [asc: :id])
    assert {:ok, ^schema} = Table.schema(reopened)
    assert {:ok, _} = Maintenance.optimize(reopened)
  end

  test "seven-day retention preserves recent history including tagged versions", %{conn: conn} do
    {:ok, table} = Connection.create_table(conn, "history", [%{id: 1}])
    {:ok, first} = History.version(table)
    {:ok, :created} = History.create_tag(table, "keep", first)
    {:ok, _} = Table.add(table, [%{id: 2}])
    {:ok, second} = History.version(table)
    assert {:ok, %{"cleanup" => %{"old_versions" => 0}}} = Maintenance.optimize(table)

    for version <- [first, second] do
      assert {:ok, snapshot} = History.snapshot(table, version)
      assert {:ok, _} = Query.query(snapshot)
    end

    assert {:ok, tagged} = History.snapshot_tag(table, "keep")
    assert {:ok, [%{"id" => 1}]} = Query.query(tagged)
    assert {:ok, 2} = Table.count_rows(table)
  end

  test "operational failures return errors without fabricated partial statistics", %{conn: conn} do
    {:ok, table} = Connection.create_table(conn, "failure", [%{id: 1}])
    {:ok, version} = History.version(table)
    {:ok, snapshot} = History.snapshot(table, version)

    assert {:error, %Error{operation: :optimize, details: details}} =
             Maintenance.optimize(snapshot)

    assert details.phase == :execution
    assert details.commit_outcome == :unknown
    refute Map.has_key?(details, :compaction)
    refute Map.has_key?(details, :cleanup)
    assert {:ok, 1} = Table.count_rows(table)
    assert {:ok, _} = Maintenance.optimize(table)
  end

  test "standard optimization respects native write admission and can retry", %{conn: conn} do
    {:ok, table} = Connection.create_table(conn, "admission", [%{id: 1}])
    {:ok, :ok} = Execution.configure(writes: 1)
    on_exit(fn -> Execution.configure([]) end)
    owner = self()

    rows =
      Stream.map([%{id: 2}], fn row ->
        send(owner, {:writer_waiting, self()})

        receive do
          :continue -> row
        after
          5_000 -> raise "writer was not released"
        end
      end)

    source = Source.rows(rows, schema: Schema.new(id: :int64))
    writer = Task.async(fn -> Table.add(table, source) end)
    assert_receive {:writer_waiting, pid}, 2_000

    assert {:error, %Error{kind: :busy, operation: :optimize, details: details}} =
             Maintenance.optimize(table)

    assert details.commit_outcome == :not_started
    send(pid, :continue)
    assert {:ok, _} = Task.await(writer)
    assert {:ok, _} = Maintenance.optimize(table)
    assert {:ok, 2} = Table.count_rows(table)
  end

  test "optimization emits one public operation span", %{conn: conn} do
    {:ok, table} = Connection.create_table(conn, "telemetry", [%{id: 1}])
    handler = make_ref()

    :ok =
      :telemetry.attach_many(
        handler,
        [[:lancedb, :operation, :start], [:lancedb, :operation, :stop]],
        &__MODULE__.observe/4,
        self()
      )

    on_exit(fn -> :telemetry.detach(handler) end)
    assert {:ok, _} = Maintenance.optimize(table)
    assert_receive {[:lancedb, :operation, :start], %{operation: :optimize}}
    assert_receive {[:lancedb, :operation, :stop], %{operation: :optimize, status: :ok}}
    refute_received {[:lancedb, :operation, _], _}
  end

  @doc false
  @spec observe([atom()], map(), map(), pid()) :: term()
  def observe(event, _measurements, metadata, owner), do: send(owner, {event, metadata})
end
