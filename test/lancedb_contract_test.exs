defmodule LanceDB.ContractTest do
  use LanceDB.DataCase, async: false
  use ExUnitProperties
  alias LanceDB.{Batch, Connection, Error, Execution, Field, Query, Schema, Table, WriteResult}

  setup %{path: path} do
    {:ok, conn} = LanceDB.connect(path)
    schema = Schema.new([id: :int64, text: :string, vector: {:vector, 2}], %{"owner" => "test"})

    {:ok, table} =
      LanceDB.Connection.create_table(conn, "source", [row(1), row(2)], schema: schema)

    %{conn: conn, table: table, schema: schema}
  end

  test("snapshots are independent across processes and restore leaves snapshots pinned", %{
    conn: conn,
    table: live
  }) do
    assert %Connection{} = conn
    assert %Table{} = live
    {:ok, initial} = LanceDB.History.version(live)
    {:ok, historical} = LanceDB.History.snapshot(live, initial)
    assert {:ok, %WriteResult{inserted: 1, version: later}} = Table.add(live, [row(3)])
    assert later > initial
    assert Task.async(fn -> LanceDB.Table.count_rows(historical) end) |> Task.await() == {:ok, 2}
    assert {:ok, 3} = LanceDB.Table.count_rows(live)
    assert {:error, %Error{}} = Table.add(historical, [row(4)])
    assert {:ok, %WriteResult{version: restored}} = LanceDB.History.restore(live, initial)
    assert restored > later
    assert {:ok, ^initial} = LanceDB.History.version(historical)
    assert {:ok, ^restored} = LanceDB.History.version(live)
    assert {:ok, 2} = LanceDB.Table.count_rows(live)
    {:ok, latest} = LanceDB.History.latest(historical)
    assert {:ok, ^restored} = LanceDB.History.version(latest)
  end

  test("one query has equivalent row, batch, eager and diagnostic execution", %{table: table}) do
    query =
      table
      |> Query.new()
      |> Query.where("id > 0")
      |> Query.select([:id, :text])
      |> Query.order_by(asc: :id)
      |> Query.limit(1)
      |> Query.offset(1)
      |> Query.timeout(10_000)

    assert {:ok, [%{"id" => 2, "text" => "document 2"}] = rows} = Query.all(query)
    assert Enum.to_list(Query.stream(query, batch_size: 1)) == rows
    assert [%Batch{} = batch] = Enum.to_list(Query.stream_batches(query, batch_size: 1))
    assert {:ok, ^rows} = Batch.to_rows(batch)
    assert {:ok, %LanceDB.ArrowSchema{} = schema} = Query.schema(query)
    assert {:ok, ^schema} = Batch.schema(batch)
    assert {:ok, plan} = Query.explain(query)
    assert is_binary(plan)
    assert {:ok, _} = Query.explain(query, verbose: true)
    assert {:ok, _} = Query.analyze(query)
    assert_raise ArgumentError, fn -> Query.explain(query, verbose: :bad) end
    assert_raise ArgumentError, fn -> Query.stream(query, typo: true) end

    assert {:ok, [%{"twice" => 2}]} =
             table
             |> Query.new()
             |> Query.select_as(twice: "id * 2")
             |> Query.limit(1)
             |> Query.all()
  end

  test("query constructors retain all search families", %{table: table}) do
    base = Query.new(table)

    vector =
      base |> Query.nearest_to([1.0, 0.0]) |> Query.options(distance: :cosine) |> Query.limit(1)

    assert {:ok, [_]} = Query.all(vector)
    assert [_] = Enum.to_list(Query.stream(vector))

    assert {:ok, [_, _]} =
             base
             |> Query.nearest_to_many([[1.0, 0.0], [0.0, 1.0]])
             |> Query.limit(1)
             |> Query.all()

    assert {:ok, :created} = LanceDB.Index.create(table, :text, type: :fts)
    assert {:ok, [_, _]} = base |> Query.text("document") |> Query.all()

    assert {:ok, [_]} =
             base |> Query.hybrid([1.0, 0.0], "document") |> Query.limit(1) |> Query.all()

    assert {:ok, [%{"id" => 1}]} = base |> Query.row_offsets([0]) |> Query.all()
    assert {:ok, [%{"_rowid" => id}]} = LanceDB.Query.query(table, limit: 1, with_row_id: true)
    assert {:ok, [%{"id" => 1}]} = base |> Query.row_ids([id]) |> Query.all()
    assert_raise ArgumentError, fn -> Query.multivector(base, []) end
    assert_raise ArgumentError, fn -> Query.limit(vector, nil) end
    assert {:ok, [_, _]} = base |> Query.limit(nil) |> Query.all()
  end

  test("canonical schemas round trip metadata, nested children and exact values", %{conn: conn}) do
    schema =
      Schema.new(
        [
          %{name: :id, type: :uint64, nullable: false},
          %{
            name: :data,
            type:
              {:struct,
               [
                 %{name: :bytes, type: :binary, metadata: %{"format" => "raw"}},
                 items: {:list, :date32, item_nullable: false},
                 at: {:timestamp, :nanosecond, "UTC"}
               ]},
            metadata: %{"scope" => "nested"}
          }
        ],
        %{"schema" => "metadata"}
      )

    {:ok, table} = LanceDB.Connection.create_empty_table(conn, "nested", schema)
    assert {:ok, ^schema} = Table.schema(table)
    {:ok, copy} = LanceDB.Connection.create_empty_table(conn, "copy", schema)
    assert {:ok, ^schema} = Table.schema(copy)

    rows = [
      %{
        id: 18_446_744_073_709_551_615,
        data: %{
          bytes: <<0, 255, 128>>,
          items: [~D[2025-01-01]],
          at: "2025-01-01T00:00:00.123456789Z"
        }
      }
    ]

    assert {:ok, batch} = Batch.from_rows(rows, schema)
    assert Batch.size(batch) == 1
    assert {:ok, wire} = Batch.to_ipc(batch)
    assert {:ok, [decoded]} = Batch.from_ipc(wire)
    assert {:ok, ^schema} = LanceDB.TestSupport.logical_schema(Batch.schema(decoded))
    assert {:ok, [value]} = Batch.to_rows(decoded)
    assert value["data"]["bytes"] == <<0, 255, 128>>
    assert value["data"]["at"] == "2025-01-01T00:00:00.123456789Z"

    assert {:ok, %WriteResult{inserted: 1, commits: 1}} =
             LanceDB.Table.add(
               copy,
               LanceDB.Source.batches([decoded]),
               commit: :once
             )

    assert {:ok, [^value]} = Query.all(Query.new(copy))
    assert {:error, %Error{kind: :invalid_data}} = Batch.from_ipc("invalid")
    assert {:error, %Error{}} = Batch.from_rows([%{id: nil}], schema)
    assert %Field{name: "id", type: :uint64} = hd(schema.fields)
  end

  test("batch pipelines retain Arrow buffers and reject incompatible destination schemas", %{
    conn: conn,
    table: table,
    schema: schema
  }) do
    {:ok, copy} = LanceDB.Connection.create_empty_table(conn, "copy", schema)

    batches =
      table |> Query.new() |> Query.order_by(asc: :id) |> Query.stream_batches(batch_size: 1)

    assert {:ok, %WriteResult{inserted: 2, commits: 1}} =
             LanceDB.Table.add(
               copy,
               LanceDB.Source.batches(batches),
               commit: :once
             )

    assert {:ok, %WriteResult{inserted: 2, commits: 1}} =
             LanceDB.Table.add(
               copy,
               LanceDB.Source.batches(batches),
               []
             )

    {:ok, wrong} = LanceDB.Connection.create_empty_table(conn, "wrong", id: :string)

    assert {:error, %Error{details: %{inserted: 0}}} =
             LanceDB.Table.add(
               wrong,
               LanceDB.Source.batches(batches),
               commit: :once
             )

    assert {:ok, 0} = LanceDB.Table.count_rows(wrong)
  end

  test("errors and telemetry have stable kinds, operation IDs and one public span", %{
    conn: conn,
    table: table
  }) do
    handler = "contract_#{System.unique_integer()}"

    events =
      for event <- [:start, :stop, :exception] do
        [:lancedb, :operation, event]
      end

    :ok = :telemetry.attach_many(handler, events, &__MODULE__.event/4, self())
    on_exit(fn -> :telemetry.detach(handler) end)

    assert {:error, %Error{kind: :not_found, operation: :open_table}} =
             LanceDB.Connection.open_table(conn, "absent")

    assert_receive {:event, [:lancedb, :operation, :start], %{operation_id: id}}

    assert_receive {:event, [:lancedb, :operation, :stop],
                    %{operation_id: ^id, error_kind: :not_found}}

    refute_receive {:event, _, _}, 10
    assert_raise ArgumentError, fn -> LanceDB.Table.add(table, [], typo: true) end
    assert_receive {:event, [:lancedb, :operation, :exception], %{operation: :add}}
    assert {:ok, %WriteResult{operation: :delete, deleted: 0}} = Table.delete(table, "id < 0")

    assert {:ok, %WriteResult{operation: :update, updated: 2}} =
             Table.update(table, %{text: "changed"})

    assert {:ok, %WriteResult{operation: :upsert, version: version, attempts: attempts}} =
             Table.upsert(table, [row(3)])

    assert is_integer(version) and is_integer(attempts)

    assert {:ok, %WriteResult{operation: :merge}} =
             Table.merge(table, [row(4)], when_not_matched_insert_all: true)

    assert_raise ArgumentError, fn -> Table.add(table, [row(5)], return: :result) end
  end

  test("admission is bounded and cancellation releases a cursor slot", %{table: table}) do
    assert {:ok, :ok} = Execution.configure(reads: 1, writes: 1)
    on_exit(fn -> Execution.configure(reads: 64, writes: 16) end)
    stream = Query.stream(Query.new(table), batch_size: 1)

    {:suspended, [_], resume} =
      Enumerable.reduce(stream, {:cont, []}, fn row, _ -> {:suspend, [row]} end)

    assert {:error, %Error{kind: :busy}} = Query.all(Query.new(table))
    assert {:ok, %WriteResult{}} = Table.add(table, [row(3)])
    assert {:halted, []} = resume.({:halt, []})
    assert {:ok, _} = Query.all(Query.new(table))

    for _ <- 1..100 do
      assert [_] = Enum.take(Query.stream(Query.new(table), batch_size: 1), 1)
      assert {:ok, _} = Query.all(Query.new(table))
    end

    assert_raise ArgumentError, fn -> Execution.configure(reads: 0) end
  end

  test "consumer death eventually releases read admission", %{table: table} do
    assert {:ok, :ok} = Execution.configure(reads: 1, writes: 1)
    on_exit(fn -> Execution.configure(reads: 64, writes: 16) end)
    parent = self()

    {pid, monitor} =
      spawn_monitor(fn ->
        stream = Query.stream(Query.new(table), batch_size: 1)

        {:suspended, [_], resume} =
          Enumerable.reduce(stream, {:cont, []}, fn row, _ -> {:suspend, [row]} end)

        send(parent, :cursor_ready)

        receive do
          :halt -> resume.({:halt, []})
        end
      end)

    assert_receive :cursor_ready
    assert {:error, %Error{kind: :busy}} = Query.all(Query.new(table))
    Process.exit(pid, :kill)
    assert_receive {:DOWN, ^monitor, :process, ^pid, :killed}
    assert_eventually(fn -> match?({:ok, _}, Query.all(Query.new(table))) end)
  end

  property("schema inspection is a lossless creation definition") do
    check(
      all(
        primitive <-
          member_of([
            :string,
            :binary,
            :boolean,
            :int8,
            :int16,
            :int32,
            :int64,
            :uint8,
            :uint16,
            :uint32,
            :uint64,
            :date32,
            :date64,
            :float32,
            :float64,
            {:timestamp, :nanosecond, "UTC"}
          ]),
        required <- boolean(),
        label <- string(:alphanumeric, max_length: 12),
        max_runs: 20
      )
    ) do
      with_database(fn conn ->
        child = %Field{
          name: "element",
          type: primitive,
          nullable: not required,
          metadata: %{"label" => label}
        }

        schema =
          Schema.new(
            [
              value: primitive,
              items: {:list, child},
              vector: {:vector, 3},
              nested: {:struct, [child]}
            ],
            %{"label" => label}
          )

        name = "schema_#{System.unique_integer([:positive])}"
        {:ok, table} = Connection.create_empty_table(conn, name, schema)
        assert {:ok, ^schema} = Table.schema(table)
        {:ok, copy} = Connection.create_empty_table(conn, name <> "_copy", schema)
        assert {:ok, ^schema} = Table.schema(copy)
      end)
    end
  end

  test("domain entry points and typed errors preserve operational context", %{
    conn: conn,
    table: table
  }) do
    assert {:ok, names} = Connection.table_names(conn)
    assert "source" in names
    assert {:ok, reopened} = Connection.open_table(conn, "source")
    assert {:ok, :created} = LanceDB.Index.create(reopened, :id, type: :btree, name: "ids")
    assert {:ok, [_]} = LanceDB.Index.list(reopened)
    assert {:ok, %{}} = LanceDB.Index.stats(reopened, "ids")
    assert {:ok, :prewarmed} = LanceDB.Index.prewarm(reopened, "ids")
    assert {:ok, :dropped} = LanceDB.Index.drop(reopened, "ids")

    assert {:error, %Error{kind: :already_exists}} =
             LanceDB.Connection.create_empty_table(conn, "source", id: :int64)

    {:ok, version} = LanceDB.History.version(table)
    {:ok, :created} = LanceDB.History.create_tag(table, "saved", version)
    assert {:ok, snapshot} = LanceDB.History.snapshot_tag(table, "saved")
    assert {:ok, ^version} = LanceDB.History.version(snapshot)

    assert {:ok, %WriteResult{inserted: 1}} =
             LanceDB.Table.add(
               table,
               LanceDB.Source.rows([row(3)]),
               []
             )

    assert {:error, %Error{kind: :source, details: %{inserted: 0}}} =
             LanceDB.Table.add(
               table,
               LanceDB.Source.rows(Stream.map([1], fn _ -> throw(:producer_stopped) end)),
               []
             )
  end

  test("Arrow item metadata is retained in batches and rejected before lossy storage", %{
    conn: conn
  }) do
    item = %Field{name: "component", type: :float32, nullable: false, metadata: %{"unit" => "x"}}
    schema = Schema.new(vector: {:vector, 2, item})
    {:ok, batch} = Batch.from_rows([%{vector: [1.0, 0.0]}], schema)

    assert {:ok, %LanceDB.ArrowSchema{fields: fields, metadata: metadata} = arrow} =
             Batch.schema(batch)

    assert fields == schema.fields and metadata == schema.metadata
    assert {:error, %Error{kind: :unsupported}} = LanceDB.ArrowSchema.to_schema(arrow)
    {:ok, ipc} = Batch.to_ipc(batch)
    {:ok, [batch]} = Batch.from_ipc(ipc)

    assert {:ok, %LanceDB.ArrowSchema{fields: fields, metadata: metadata} = arrow} =
             Batch.schema(batch)

    assert fields == schema.fields and metadata == schema.metadata
    assert {:error, %Error{kind: :unsupported}} = LanceDB.ArrowSchema.to_schema(arrow)

    assert {:error, %Error{kind: :unsupported}} =
             Connection.create_empty_table(conn, "lossy", schema)

    assert {:error, %Error{kind: :not_found}} = Connection.open_table(conn, "lossy")
  end

  test "write transport failures report unknown outcomes and context never leaks" do
    assert {:error, %Error{operation: :add, details: %{commit_outcome: :unknown}}} =
             Execution.run(:add, fn ->
               {:error,
                %Error{
                  kind: :storage,
                  message: "connection lost",
                  details: %{phase: :execution, commit_outcome: :unknown}
                }}
             end)

    assert {:error, %Error{operation: :query, details: details}} =
             Execution.run(:query, fn ->
               {:error, %Error{kind: :storage, message: "connection lost"}}
             end)

    refute Map.has_key?(details, :commit_outcome)
  end

  def event(name, _measurements, metadata, pid) do
    send(pid, {:event, name, metadata})
  end

  defp row(id) do
    %{id: id, text: "document #{id}", vector: [1.0, 0.0]}
  end
end
