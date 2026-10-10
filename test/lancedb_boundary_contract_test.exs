defmodule LanceDB.BoundaryContractTest do
  use LanceDB.DataCase, async: true
  use ExUnitProperties

  alias LanceDB.{
    ArrowSchema,
    Batch,
    Blob,
    Connection,
    Error,
    History,
    Query,
    Schema,
    Source,
    Table
  }

  setup %{path: path} do
    {:ok, conn} = Connection.connect(path)
    %{conn: conn}
  end

  property("explicit row and batch sources agree across chunk sizes and empty input") do
    check(
      all(
        ids <- uniq_list_of(integer(-1000..1000), max_length: 16),
        size <- integer(1..8),
        max_runs: 20
      )
    ) do
      with_database(fn conn ->
        schema = Schema.new([id: :int64], %{"source" => "property"})
        rows = Enum.map(ids, &%{id: &1})
        suffix = System.unique_integer([:positive])
        {:ok, batch} = Batch.from_rows(rows, schema)

        {:ok, from_rows} =
          Connection.create_table(
            conn,
            "rows_#{suffix}",
            Source.rows(rows, schema: schema, batch_size: size)
          )

        {:ok, from_batches} =
          Connection.create_table(
            conn,
            "batches_#{suffix}",
            Source.batches([batch], schema: schema)
          )

        for table <- [from_rows, from_batches] do
          assert {:ok, ^schema} = Table.schema(table)

          assert {:ok, result} =
                   table |> Query.new(limit: nil, order_by: [asc: :id]) |> Query.all()

          assert Enum.map(result, & &1["id"]) == Enum.sort(ids)
          assert {:ok, 1} = History.version(table)
        end
      end)
    end
  end

  test(
    "schema declarations conflict before consuming input, and writes assert supplied fields",
    %{conn: conn}
  ) do
    owner = self()

    rows =
      Stream.map([%{id: 1}], fn row ->
        send(owner, :consumed)
        row
      end)

    schema = Schema.new(id: :int64)
    source = Source.rows(rows, schema: schema)

    assert_raise ArgumentError, ~r/schemas conflict/, fn ->
      Connection.create_table(conn, "conflict", source, schema: [id: :string])
    end

    refute_receive :consumed
    assert {:ok, table} = Connection.create_table(conn, "same", source, schema: schema)
    assert_receive :consumed

    assert_raise ArgumentError, fn ->
      Connection.create_table(conn, "same", source,
        mode: :exist_ok,
        schema_metadata: %{invalid: 1}
      )
    end

    refute_receive :consumed
    wrong = Source.rows(rows, schema: [id: :string])

    for operation <- [&Table.add/2, &Table.upsert/2] do
      assert {:error, %Error{kind: :invalid_data, details: %{commit_outcome: :not_started}}} =
               operation.(table, wrong)

      refute_receive :consumed
    end

    assert {:ok, 1} = Table.count_rows(table)
  end

  test("physical schemas require checked conversion and blob references are typed", %{conn: conn}) do
    schema = Schema.new(id: :int64)
    {:ok, table} = Connection.create_empty_table(conn, "plain", schema)
    assert {:ok, %ArrowSchema{} = arrow} = table |> Query.new() |> Query.schema()
    assert {:ok, ^schema} = ArrowSchema.to_schema(arrow)
    assert_raise ArgumentError, fn -> Source.batches([], schema: arrow) end
    assert %Blob.Reference{uri: "s3://bucket/object"} = Blob.reference("s3://bucket/object")
    assert_raise ArgumentError, fn -> Blob.reference("") end

    external = %ArrowSchema{
      fields: [
        %LanceDB.Field{name: "unknown", type: {:unsupported, "Union"}, nullable: true}
      ]
    }

    assert {:error, %Error{}} = ArrowSchema.to_schema(external)
  end

  test("creation conveniences emit one span, and query events share an execution ID", %{
    conn: conn
  }) do
    handler = "boundary_#{System.unique_integer([:positive])}"
    :ok = :telemetry.attach(handler, [:lancedb, :operation, :stop], &__MODULE__.event/4, self())
    on_exit(fn -> :telemetry.detach(handler) end)
    {:ok, empty} = Connection.create_empty_table(conn, "empty", id: :int64)
    assert [%{operation: :create_table}] = events()
    {:ok, table} = Connection.create_table(conn, "inferred", [%{id: 1}, %{id: 2}])
    assert [%{operation: :create_table}] = events()
    assert {:ok, [%{"id" => 1}, %{"id" => 2}]} = Query.all(Query.new(table, order_by: [asc: :id]))
    trace = events()
    root = Enum.find(trace, &(&1.operation == :query))
    assert Enum.all?(trace, &(&1.execution_id == root.execution_id))
    children = Enum.reject(trace, &(&1.operation == :query))
    assert Enum.all?(children, &(&1.parent_operation_id == root.operation_id))

    assert Enum.map(children, & &1.operation) == [
             :stream_open,
             :stream_next_batch,
             :stream_next_batch,
             :stream_close
           ]

    stream = Query.stream(Query.new(empty))
    assert Enum.to_list(stream) == []
    first = events()
    assert Enum.to_list(stream) == []
    second = events()
    assert length(Enum.uniq_by(first, & &1.execution_id)) == 1
    assert length(Enum.uniq_by(second, & &1.execution_id)) == 1
    refute hd(first).execution_id == hd(second).execution_id
  end

  test "storage option validation is shared and rejects duplicate aliases" do
    invalid = %{"region" => "two", region: "one"}
    assert_raise ArgumentError, ~r/unique/, fn -> Connection.connect("unused", invalid) end

    assert_raise ArgumentError, ~r/unique/, fn ->
      Connection.open_table(:unused, "t", storage_options: invalid)
    end

    assert_raise ArgumentError, ~r/unique/, fn ->
      Connection.create_table(:unused, "t", [], storage_options: invalid)
    end

    assert_raise ArgumentError, fn -> Connection.connect("unused", %{"key" => <<255>>}) end
  end

  def event(_, _, metadata, owner) do
    if self() == owner, do: send(owner, {:boundary_event, metadata})
  end

  defp events do
    receive do
      {:boundary_event, metadata} -> [metadata | events()]
    after
      0 -> []
    end
  end
end
