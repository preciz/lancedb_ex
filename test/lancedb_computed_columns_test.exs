defmodule LanceDB.ComputedColumnsTest do
  use LanceDB.DataCase, async: true
  @moduletag :native_operation_contract

  alias LanceDB.{
    Batch,
    Branch,
    Connection,
    Error,
    Expr,
    History,
    Index,
    Job,
    Metadata,
    Query,
    RefreshResult,
    SchemaEvolution,
    Source,
    Table
  }

  setup %{path: path} do
    {:ok, conn} = LanceDB.connect(path)

    {:ok, table} =
      Connection.create_table(
        conn,
        "docs",
        [
          %{id: 1, x: 2, label: "hello"},
          %{id: 2, x: 3, label: nil}
        ],
        schema: [id: :int64, x: :int64, label: :string],
        schema_metadata: %{owner: "team"}
      )

    %{conn: conn, table: table}
  end

  test "declaration persists expressions and null placeholders; refresh fills then becomes a no-op",
       %{table: table, conn: conn} do
    assert Table.write_schema(table) == Table.schema(table)
    {:ok, before} = History.version(table)

    assert {:ok, %{operation: :add_computed_columns, version: declared}} =
             SchemaEvolution.add_computed_columns(table,
               doubled: Expr.mul(Expr.col(:x), 2),
               title: "upper(label)"
             )

    assert declared > before
    assert {:ok, schema} = Table.schema(table)
    field = Enum.find(schema.fields, &(&1.name == "doubled"))
    assert field.nullable
    assert field.metadata["computed_column"] == "true"
    assert {:ok, [first, second]} = Query.query(table, order_by: [asc: :id])
    assert first["doubled"] == nil
    assert first["title"] == nil
    assert second["doubled"] == nil
    {:ok, reopened} = Connection.open_table(conn, "docs")
    assert {:ok, ^schema} = Table.schema(reopened)

    assert {:ok, %RefreshResult{rows_filled: 2, version: filled}} =
             Table.refresh_column(table, :doubled)

    assert filled > declared
    assert {:ok, %RefreshResult{rows_filled: 1}} = Table.refresh_column(table, "title")
    assert {:ok, [first, second]} = Query.query(table, order_by: [asc: :id])
    assert first["doubled"] == 4
    assert first["title"] == "HELLO"
    assert second["doubled"] == 6
    assert second["title"] == nil
    {:ok, current} = History.version(table)

    assert {:ok, %RefreshResult{rows_filled: 0, version: ^current}} =
             Table.refresh_column(table, :doubled)

    assert {:ok, ^current} = History.version(table)
  end

  test "append and merge use writable schemas and refresh reflects changed inputs", %{
    table: table
  } do
    {:ok, _} = SchemaEvolution.add_computed_columns(table, doubled: "x * 2")
    {:ok, _} = Table.refresh_column(table, :doubled)
    assert {:ok, writable} = Table.write_schema(table)
    assert Enum.map(writable.fields, & &1.name) == ["id", "x", "label"]
    assert writable.metadata == %{"owner" => "team"}

    source = Source.rows([%{id: 3, x: 4, label: "row"}], schema: writable, batch_size: 1)
    assert {:ok, %{inserted: 1}} = Table.add(table, source)
    {:ok, batch} = Batch.from_rows([%{id: 4, x: 5, label: "batch"}], writable)
    assert {:ok, %{inserted: 1}} = Table.add(table, Source.batches([batch], schema: writable))

    assert {:ok, [%{"doubled" => nil}, %{"doubled" => nil}]} =
             Query.query(table, columns: [:doubled], filter: "id >= 3")

    assert {:ok, %RefreshResult{rows_filled: 2}} = Table.refresh_column(table, :doubled)

    assert {:ok, %{inserted: 1, updated: 1}} =
             Table.upsert(
               table,
               Source.rows(
                 [
                   %{id: 1, x: 10, label: "changed"},
                   %{id: 5, x: 6, label: "new"}
                 ],
                 schema: writable
               )
             )

    assert {:ok, %RefreshResult{rows_filled: count}} = Table.refresh_column(table, :doubled)
    assert count > 0
    assert {:ok, rows} = Query.query(table, columns: [:id, :x, :doubled], order_by: [asc: :id])
    assert Enum.all?(rows, &(&1["doubled"] == &1["x"] * 2))
    assert {:ok, %{updated: 1}} = Table.update(table, %{x: 20}, filter: "id = 2")
    assert {:ok, _} = Table.refresh_column(table, :doubled)
    assert {:ok, [%{"doubled" => 40}]} = Query.query(table, columns: [:doubled], filter: "id = 2")
  end

  test "typed refresh jobs retain results and index jobs retain their existing contract", %{
    table: table
  } do
    {:ok, _} = SchemaEvolution.add_computed_columns(table, doubled: "x * 2")
    assert {:ok, job} = Table.refresh_column_async(table, :doubled)
    assert {:ok, %RefreshResult{rows_filled: 2, version: version} = result} = Job.wait(job)
    assert version > 1
    assert {:ok, ^result} = Job.wait(job, timeout_ms: 0)
    assert {:ok, ^result} = Task.async(fn -> Job.wait(job) end) |> Task.await()
    assert {:ok, "finished"} = Job.status(job)
    assert {:ok, no_op} = Table.refresh_column_async(table, :doubled)
    assert {:ok, %RefreshResult{rows_filled: 0, version: ^version}} = Job.wait(no_op)
    assert {:ok, index} = Index.create_async(table, :x, type: :btree)
    assert {:ok, :created} = Job.wait(index)
    assert {:ok, :created} = Job.wait(index)
  end

  test "JSON input remains writable during computed-column merges", %{conn: conn} do
    {:ok, table} =
      Connection.create_table(conn, "json", [%{id: 1, data: %{"value" => 2}}],
        schema: [id: :int64, data: :json]
      )

    {:ok, _} = SchemaEvolution.add_computed_columns(table, doubled: "id * 2")
    {:ok, _} = Table.refresh_column(table, :doubled)

    assert {:ok, %{inserted: 1, updated: 1}} =
             Table.upsert(table, [
               %{id: 1, data: %{"value" => 3}},
               %{id: 2, data: %{"value" => 4}}
             ])

    assert {:ok, _} = Table.refresh_column(table, :doubled)

    assert {:ok,
            [
              %{"id" => 1, "data" => %{"value" => 3}, "doubled" => 2},
              %{"id" => 2, "data" => %{"value" => 4}, "doubled" => 4}
            ]} = Query.query(table, order_by: [asc: :id])
  end

  test "computed values cannot be supplied through rows, batches, updates or source schemas", %{
    table: table
  } do
    {:ok, _} = SchemaEvolution.add_computed_columns(table, doubled: "x * 2")
    {:ok, stored} = Table.schema(table)
    {:ok, batch} = Batch.from_rows([%{id: 3, x: 3, label: "bad", doubled: 999}], stored)
    {:ok, version} = History.version(table)

    for operation <- [
          fn -> Table.add(table, [%{id: 3, x: 3, label: "bad", doubled: nil}]) end,
          fn -> Table.upsert(table, [%{id: 3, x: 3, label: "bad", doubled: 999}]) end,
          fn -> Table.add(table, Source.batches([batch])) end,
          fn -> Table.upsert(table, Source.batches([batch])) end,
          fn -> Table.update(table, %{doubled: 999}) end,
          fn -> Table.add(table, Source.rows([], schema: stored)) end
        ] do
      assert {:error, %Error{}} = operation.()
      assert {:ok, ^version} = History.version(table)
    end
  end

  test "ordered declarations and refreshes honor computed dependencies", %{table: table} do
    assert {:ok, _} =
             SchemaEvolution.add_computed_columns(table, doubled: "x * 2", next: "doubled + 1")

    assert {:error, %Error{}} = Table.refresh_column(table, :next)
    assert {:ok, _} = Table.refresh_column(table, :doubled)
    assert {:ok, _} = Table.refresh_column(table, :next)

    assert {:ok, [%{"next" => 5}, %{"next" => 7}]} =
             Query.query(table, columns: [:next], order_by: [asc: :id])

    {:ok, version} = History.version(table)

    for operation <- [
          fn -> SchemaEvolution.drop_columns(table, [:x]) end,
          fn -> SchemaEvolution.drop_columns(table, [:doubled]) end,
          fn -> SchemaEvolution.alter_columns(table, x: [rename: :other]) end,
          fn -> SchemaEvolution.alter_columns(table, x: [type: :int32]) end,
          fn -> SchemaEvolution.alter_columns(table, doubled: [type: :int32]) end
        ] do
      assert {:error, %Error{}} = operation.()
      assert {:ok, ^version} = History.version(table)
    end

    assert {:ok, %{inserted: 1}} = Table.add(table, [%{id: 3, x: 4, label: "new"}])
    assert {:error, %Error{}} = Table.refresh_column(table, :next)
    assert {:ok, _} = Table.refresh_column(table, :doubled)
    assert {:ok, _} = Table.refresh_column(table, :next)
    assert {:ok, [%{"next" => 9}]} = Query.query(table, columns: [:next], filter: "id = 3")
    assert {:ok, _} = SchemaEvolution.drop_columns(table, [:doubled, :next])
    assert {:ok, _} = SchemaEvolution.alter_columns(table, x: [rename: :value])
  end

  test "user metadata replacement preserves declarations and freshness signatures", %{
    table: table
  } do
    {:ok, _} = SchemaEvolution.add_computed_columns(table, doubled: "x * 2")
    {:ok, _} = Table.refresh_column(table, :doubled)
    {:ok, schema} = Table.schema(table)
    markers = Enum.find(schema.fields, &(&1.name == "doubled")).metadata
    assert {:ok, _} = Metadata.update_field(table, :doubled, %{label: "Value"}, replace: true)
    assert {:ok, _} = Metadata.update_field(table, :doubled, markers, replace: true)
    {:ok, updated} = Table.schema(table)
    assert Enum.find(updated.fields, &(&1.name == "doubled")).metadata == markers
    {:ok, version} = History.version(table)

    assert {:ok, %RefreshResult{rows_filled: 0, version: ^version}} =
             Table.refresh_column(table, :doubled)

    for changes <- [
          %{"computed_column" => nil},
          %{"computed_column.expression" => "x * 3"},
          %{"computed_column.inputs" => "[]"},
          %{"computed_refresh.source_signature" => "{}"}
        ] do
      assert {:error, %Error{}} = Metadata.update_field(table, :doubled, changes)
      assert {:ok, ^version} = History.version(table)
    end

    assert {:error, %Error{}} = Metadata.update_field(table, :x, %{"computed_column" => "true"})
  end

  test "overwrite rejects before consuming a source and retains declarations", %{table: table} do
    {:ok, _} = SchemaEvolution.add_computed_columns(table, doubled: "x * 2")
    {:ok, version} = History.version(table)
    parent = self()

    source =
      Source.rows(
        Stream.map([%{id: 3, x: 4, label: "new"}], fn row ->
          send(parent, :consumed)
          row
        end)
      )

    assert {:error,
            %Error{
              kind: :unsupported,
              details: %{phase: :validation, commit_outcome: :not_started}
            }} =
             Table.add(table, source, mode: :overwrite)

    refute_received :consumed
    assert {:error, %Error{kind: :unsupported}} = Table.add(table, [], mode: :overwrite)
    assert {:ok, ^version} = History.version(table)
    assert {:ok, 2} = Table.count_rows(table)
  end

  test "declaration failures are atomic and invalid refreshes are structured errors", %{
    table: table
  } do
    {:ok, version} = History.version(table)

    for expressions <- [
          [good: "x + 1", bad: "missing + 1"],
          [id: "x * 2"],
          [random_value: "random()"],
          [bad: "CAST(x AS imaginary)"]
        ] do
      assert {:error, %Error{details: %{phase: :validation, commit_outcome: :not_started}}} =
               SchemaEvolution.add_computed_columns(table, expressions)

      assert {:ok, ^version} = History.version(table)
    end

    for column <- [:missing, :x] do
      assert {:error, %Error{}} = Table.refresh_column(table, column)
      assert {:error, %Error{}} = Table.refresh_column_async(table, column)
    end

    assert {:ok, _} =
             SchemaEvolution.add_computed_columns(table, invalid_value: "CAST(label AS BIGINT)")

    {:ok, version} = History.version(table)
    assert {:error, %Error{}} = Table.refresh_column(table, :invalid_value)
    assert {:ok, job} = Table.refresh_column_async(table, :invalid_value)

    assert {:error, %Error{details: %{phase: :execution, commit_outcome: :unknown}} = failure} =
             Job.wait(job)

    assert {:error, ^failure} = Job.wait(job)
    assert {:ok, "failed"} = Job.status(job)
    assert {:ok, ^version} = History.version(table)
  end

  test "empty tables and deleted rows refresh with correct result counts", %{
    conn: conn,
    table: table
  } do
    {:ok, empty} = Connection.create_empty_table(conn, "empty", id: :int64, x: :int64)
    {:ok, _} = SchemaEvolution.add_computed_columns(empty, doubled: "x * 2")
    assert {:ok, %RefreshResult{rows_filled: 0}} = Table.refresh_column(empty, :doubled)
    assert {:ok, _} = Table.add(empty, [%{id: 1, x: 2}])
    {:ok, job} = Table.refresh_column_async(empty, :doubled)
    assert {:ok, %RefreshResult{rows_filled: 1}} = Job.wait(job)

    {:ok, _} = SchemaEvolution.add_computed_columns(table, doubled: "x * 2")
    {:ok, _} = Table.delete(table, "id = 2")
    assert {:ok, %RefreshResult{rows_filled: 1}} = Table.refresh_column(table, :doubled)
    assert {:ok, [%{"doubled" => 4}]} = Query.query(table, columns: [:doubled])
  end

  test "nested inputs and struct outputs refresh safely across input changes", %{conn: conn} do
    {:ok, table} =
      Connection.create_table(conn, "nested", [%{id: 1, data: %{value: 2, other: 3}}],
        schema: [id: :int64, data: {:struct, [value: :int64, other: :int64]}]
      )

    assert {:ok, _} =
             SchemaEvolution.add_computed_columns(table,
               derived: "named_struct('double', data.value * 2)"
             )

    assert {:ok, %RefreshResult{rows_filled: 1}} = Table.refresh_column(table, :derived)
    assert {:ok, [%{"derived" => %{"double" => 4}}]} = Query.query(table, columns: [:derived])

    {:ok, version} = History.version(table)

    assert {:error, %Error{kind: :unsupported, details: %{commit_outcome: :not_started}}} =
             Metadata.update_field(table, "derived.double", %{unit: "count"}, replace: true)

    assert {:ok, ^version} = History.version(table)

    assert {:ok, _} =
             Metadata.update_field(table, :derived, %{label: "Calculated"}, replace: true)

    assert {:error, %Error{}} =
             SchemaEvolution.alter_columns(table, "data.value": [rename: :amount])

    assert {:error, %Error{}} = SchemaEvolution.drop_columns(table, ["data.value"])
    assert {:ok, _} = Table.update(table, %{data: %{value: 3, other: 3}})
    assert {:ok, _} = Table.refresh_column(table, :derived)
    assert {:ok, [%{"derived" => %{"double" => 6}}]} = Query.query(table, columns: [:derived])
  end

  test "branches and snapshots keep independent declarations, values and refresh state", %{
    table: table
  } do
    {:ok, version} = History.version(table)
    {:ok, old} = History.snapshot(table, version)
    {:ok, _} = SchemaEvolution.add_computed_columns(table, doubled: "x * 2")
    {:ok, declaration} = History.version(table)
    {:ok, unfilled} = History.snapshot(table, declaration)
    {:ok, branch} = Branch.create(table, "computed")
    assert {:ok, _} = Table.refresh_column(branch, :doubled)

    assert {:ok, [%{"doubled" => nil}, %{"doubled" => nil}]} =
             Query.query(table, columns: [:doubled])

    assert {:ok, %RefreshResult{rows_filled: 2}} = Table.refresh_column(table, :doubled)

    assert {:ok, [%{"doubled" => nil}, %{"doubled" => nil}]} =
             Query.query(unfilled, columns: [:doubled])

    assert {:ok, %{fields: fields}} = Table.schema(old)
    refute Enum.any?(fields, &(&1.name == "doubled"))
    assert {:ok, %{fields: fields}} = Table.write_schema(unfilled)
    assert Enum.map(fields, & &1.name) == ["id", "x", "label"]
  end

  test "incremental input and native-batch merges retain computed declarations", %{table: table} do
    {:ok, _} = SchemaEvolution.add_computed_columns(table, doubled: "x * 2")
    source = Source.rows([%{id: 3, x: 4, label: "a"}, %{id: 4, x: 5, label: "b"}], batch_size: 1)
    assert {:ok, %{inserted: 2, commits: 2}} = Table.add(table, source, commit: :batch)
    {:ok, schema} = Table.write_schema(table)
    {:ok, batch} = Batch.from_rows([%{id: 1, x: 20, label: "changed"}], schema)

    assert {:ok, %{updated: 1}} =
             Table.merge(table, Source.batches([batch], schema: schema),
               when_matched_update_all: true
             )

    assert {:ok, _} = Table.refresh_column(table, :doubled)
    assert {:ok, rows} = Query.query(table, columns: [:x, :doubled])
    assert Enum.all?(rows, &(&1["doubled"] == &1["x"] * 2))
  end

  test "nested input metadata cannot invalidate a copied container's refresh type", %{conn: conn} do
    {:ok, table} =
      Connection.create_table(conn, "copy_struct", [%{id: 1, data: %{value: 2}}],
        schema: [id: :int64, data: {:struct, [value: :int64]}]
      )

    {:ok, _} = SchemaEvolution.add_computed_columns(table, copied: "data")
    assert {:ok, _} = Table.refresh_column(table, :copied)
    assert {:ok, _} = Table.update(table, %{data: %{value: 3}})
    {:ok, version} = History.version(table)

    assert {:error, %Error{kind: :unsupported, details: %{commit_outcome: :not_started}}} =
             Metadata.update_field(table, "data.value", %{unit: "count"})

    assert {:ok, ^version} = History.version(table)
    assert {:ok, _} = Table.refresh_column(table, :copied)
    assert {:ok, [%{"copied" => %{"value" => 3}}]} = Query.query(table, columns: [:copied])
  end

  test "refresh job polling and cancellation retain a stable terminal outcome", %{table: table} do
    {:ok, _} = SchemaEvolution.add_computed_columns(table, doubled: "x * 2")
    {:ok, job} = Table.refresh_column_async(table, :doubled)

    case Job.wait(job, timeout_ms: 0) do
      {:ok, %RefreshResult{}} ->
        :ok

      {:error, %Error{kind: :timeout, details: %{phase: :waiting, commit_outcome: :unknown}}} ->
        :ok
    end

    assert {:ok, :ok} = Job.cancel(job)
    result = Job.wait(job)

    assert match?({:ok, %RefreshResult{}}, result) or
             match?({:error, %Error{kind: :cancelled}}, result)

    assert ^result = Job.wait(job, timeout_ms: 0)
  end

  test "ordinary column backfills work while declaration metadata cannot be injected", %{
    table: table
  } do
    {:ok, _} = SchemaEvolution.add_computed_columns(table, doubled: "x * 2")
    {:ok, _} = Table.refresh_column(table, :doubled)

    assert {:ok, _} =
             SchemaEvolution.add_from_source(
               table,
               Source.rows([%{note: "first"}, %{note: "second"}], schema: [note: :string])
             )

    assert {:ok, %RefreshResult{rows_filled: 0}} = Table.refresh_column(table, :doubled)
    {:ok, full} = Table.schema(table)
    field = Enum.find(full.fields, &(&1.name == "doubled"))
    forged_schema = [%{field | name: "forged"}]
    parent = self()

    forged =
      Source.rows(
        Stream.map([%{forged: 999}, %{forged: 999}], fn row ->
          send(parent, :consumed_forged)
          row
        end),
        schema: forged_schema
      )

    {:ok, version} = History.version(table)

    assert {:error, %Error{details: %{phase: :validation, commit_outcome: :not_started}}} =
             SchemaEvolution.add_from_source(table, forged)

    refute_received :consumed_forged
    assert {:ok, ^version} = History.version(table)
  end
end
