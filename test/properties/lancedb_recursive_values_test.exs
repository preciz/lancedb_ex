defmodule LanceDB.RecursiveValuesPropertyTest do
  use ExUnit.Case, async: true
  use ExUnitProperties
  import LanceDB.TestSupport, only: [with_database: 1]
  alias LanceDB.{Batch, Connection, Generators, History, Query, Schema, Source, Table}

  property "recursive typed values survive row and batch sources, IPC, writes and reopening" do
    check all({type, input, expected} <- Generators.typed_value(), max_runs: 40) do
      schema = Schema.new(id: :int64, value: type)
      rows = [%{id: 0, value: input}, %{id: 1, value: nil}]
      expected = [%{"id" => 0, "value" => expected}, %{"id" => 1, "value" => nil}]
      assert {:ok, batch} = Batch.from_rows(rows, schema)
      assert {:ok, ^expected} = Batch.to_rows(batch)
      assert {:ok, ipc} = Batch.to_ipc(batch)
      assert {:ok, [imported]} = Batch.from_ipc(ipc)
      assert {:ok, ^expected} = Batch.to_rows(imported)

      with_database(fn conn, path ->
        for {name, source} <- [
              {"rows", Source.rows(rows, schema: schema, batch_size: 1)},
              {"batches", Source.batches([imported], schema: schema)}
            ] do
          assert {:ok, table} = Connection.create_table(conn, name, source)
          assert {:ok, ^schema} = Table.schema(table)
          assert {:ok, ^expected} = Query.query(table, order_by: [asc: :id])
          assert {:ok, fresh} = LanceDB.connect(path)
          assert {:ok, reopened} = Connection.open_table(fresh, name)
          assert {:ok, ^expected} = Query.query(reopened, order_by: [asc: :id])
        end
      end)
    end
  end

  property "recursive JSON preserves SQL null and JSON null through persistence and updates" do
    check all(value <- Generators.json_value(), max_runs: 35) do
      expected_value = if is_nil(value), do: LanceDB.JSON.null(), else: value
      rows = [%{id: 0, value: LanceDB.JSON.new(value)}, %{id: 1, value: nil}]
      expected = [%{"id" => 0, "value" => expected_value}, %{"id" => 1, "value" => nil}]
      schema = Schema.new(id: :int64, value: :json)
      assert {:ok, batch} = Batch.from_rows(rows, schema)
      assert {:ok, ^expected} = Batch.to_rows(batch)

      with_database(fn conn ->
        assert {:ok, table} = Connection.create_table(conn, "json", rows, schema: schema)
        assert {:ok, ^expected} = Query.query(table, order_by: [asc: :id])
        assert {:ok, version} = History.version(table)
        assert {:ok, snapshot} = History.snapshot(table, version)

        assert {:ok, %{updated: 1}} =
                 Table.update(table, %{value: LanceDB.JSON.new(value)}, filter: "id = 1")

        updated = [
          %{"id" => 0, "value" => expected_value},
          %{"id" => 1, "value" => expected_value}
        ]

        assert {:ok, ^updated} = Query.query(table, order_by: [asc: :id])
        assert {:ok, ^expected} = Query.query(snapshot, order_by: [asc: :id])
        assert {:ok, reopened} = Connection.open_table(conn, "json")
        assert {:ok, ^updated} = Query.query(reopened, order_by: [asc: :id])
      end)
    end
  end
end
