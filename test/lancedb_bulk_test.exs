defmodule LanceDB.BulkTest do
  use LanceDB.DataCase, async: true
  alias LanceDB.{Batch, Query, Schema, Table, WriteResult}

  setup %{path: path} do
    {:ok, db} = LanceDB.connect(path)
    schema = Schema.new(id: :int64, value: :string)
    %{db: db, schema: schema}
  end

  test("creation consumes rows lazily and commits batches once, including empty input", %{
    db: db,
    schema: schema
  }) do
    rows = Stream.map(1..9, &%{id: &1, value: "row"})

    assert {:ok, table} =
             LanceDB.Connection.create_table(
               db,
               "rows",
               LanceDB.Source.rows(rows, schema: schema, batch_size: 2),
               []
             )

    assert {:ok, 1} = LanceDB.History.version(table)
    assert {:ok, 9} = LanceDB.Table.count_rows(table)
    batches = table |> Query.new(limit: nil) |> Query.stream_batches(batch_size: 3)

    assert {:ok, copied} =
             LanceDB.Connection.create_table(
               db,
               "copy",
               LanceDB.Source.batches(batches, schema: schema),
               []
             )

    assert {:ok, 9} = LanceDB.Table.count_rows(copied)
    assert {:ok, 1} = LanceDB.History.version(copied)

    assert {:ok, empty} =
             LanceDB.Connection.create_table(
               db,
               "empty",
               LanceDB.Source.rows([], schema: schema),
               []
             )

    assert {:ok, 0} = LanceDB.Table.count_rows(empty)

    assert {:ok, replaced} =
             LanceDB.Connection.create_table(
               db,
               "rows",
               LanceDB.Source.rows([%{id: 20, value: "new"}], schema: schema),
               mode: :overwrite
             )

    assert {:ok, [%{"id" => 20}]} = LanceDB.Query.query(replaced, columns: [:id])

    assert_raise ArgumentError, fn ->
      LanceDB.Connection.create_table(
        db,
        "rows",
        LanceDB.Source.rows([], schema: schema),
        mode: :invalid
      )
    end
  end

  test("producer failures do not create or overwrite a table", %{db: db, schema: schema}) do
    rows =
      Stream.concat([[%{id: 1, value: "ok"}], Stream.map([2], fn _ -> raise "source broke" end)])

    assert {:error, %{kind: :source}} =
             LanceDB.Connection.create_table(
               db,
               "missing",
               LanceDB.Source.rows(rows, schema: schema, batch_size: 1),
               []
             )

    assert {:error, _} = LanceDB.Connection.open_table(db, "missing")

    assert {:ok, _} =
             LanceDB.Connection.create_table(
               db,
               "missing",
               LanceDB.Source.rows([%{id: 9, value: "retry"}], schema: schema),
               []
             )

    {:ok, table} =
      LanceDB.Connection.create_table(
        db,
        "existing",
        LanceDB.Source.rows([%{id: 5, value: "old"}], schema: schema),
        []
      )

    {:ok, version} = LanceDB.History.version(table)

    assert {:error, %{kind: :source}} =
             LanceDB.Connection.create_table(
               db,
               "existing",
               LanceDB.Source.rows(rows, schema: schema, batch_size: 1),
               mode: :overwrite
             )

    {:ok, reopened} = LanceDB.Connection.open_table(db, "existing")
    assert {:ok, ^version} = LanceDB.History.version(reopened)
  end

  test("conditional merges apply source-wide deletion once and return totals", %{
    db: db,
    schema: schema
  }) do
    {:ok, table} =
      LanceDB.Connection.create_table(
        db,
        "merge",
        LanceDB.Source.rows(Enum.map(1..3, &%{id: &1, value: "old"}), schema: schema),
        []
      )

    {:ok, before} = LanceDB.History.version(table)
    rows = [%{id: 1, value: "updated"}, %{id: 4, value: "inserted"}]

    assert {:ok, %WriteResult{inserted: 1, updated: 1, deleted: 2, version: version}} =
             LanceDB.Table.merge(
               table,
               LanceDB.Source.rows(rows, batch_size: 1),
               when_matched_update_all: "target.value = 'old'",
               when_not_matched_insert_all: true,
               when_not_matched_by_source_delete: true
             )

    assert version == before + 1

    assert {:ok, [%{"id" => 1}, %{"id" => 4}]} =
             LanceDB.Query.query(table, columns: [:id], order_by: [asc: :id])

    {:ok, batch} = Batch.from_rows([%{id: 4, value: "batch"}, %{id: 5, value: "batch"}], schema)

    assert {:ok, %WriteResult{inserted: 1, updated: 1}} =
             LanceDB.Table.upsert(
               table,
               LanceDB.Source.batches([batch]),
               []
             )

    assert {:ok, %WriteResult{deleted: 3}} =
             LanceDB.Table.merge(
               table,
               LanceDB.Source.rows([]),
               when_not_matched_by_source_delete: true
             )
  end

  test("duplicate keys across batches and source failures abort the complete merge", %{
    db: db,
    schema: schema
  }) do
    {:ok, table} =
      LanceDB.Connection.create_table(
        db,
        "merge_fail",
        LanceDB.Source.rows([%{id: 1, value: "old"}], schema: schema),
        []
      )

    {:ok, before} = LanceDB.History.version(table)

    assert {:error, _} =
             LanceDB.Table.upsert(
               table,
               LanceDB.Source.rows([%{id: 2, value: "a"}, %{id: 2, value: "b"}], batch_size: 1),
               []
             )

    assert {:ok, ^before} = LanceDB.History.version(table)

    broken =
      Stream.concat([[%{id: 2, value: "new"}], Stream.map([1], fn _ -> throw(:failed) end)])

    assert {:error, %{kind: :source}} =
             LanceDB.Table.upsert(
               table,
               LanceDB.Source.rows(broken, batch_size: 1),
               []
             )

    assert {:ok, ^before} = LanceDB.History.version(table)
    {:ok, invalid} = Batch.from_rows([%{id: nil, value: "a"}], schema)

    assert {:error, _} =
             LanceDB.Table.upsert(
               table,
               LanceDB.Source.batches([invalid]),
               []
             )

    assert {:ok, ^before} = LanceDB.History.version(table)
  end

  test("callback backfills use selected snapshot columns and preserve deleted-row alignment", %{
    db: db,
    schema: schema
  }) do
    {:ok, table} =
      LanceDB.Connection.create_table(
        db,
        "backfill",
        LanceDB.Source.rows(Enum.map(1..7, &%{id: &1, value: "row"}),
          schema: schema,
          batch_size: 2
        ),
        []
      )

    {:ok, _} = Table.delete(table, "id = 3")
    {:ok, before} = LanceDB.History.version(table)

    mapper = fn batch ->
      assert {:ok, %LanceDB.ArrowSchema{fields: [%{name: "id"}]}} = Batch.schema(batch)
      {:ok, rows} = Batch.to_rows(batch)
      Enum.map(rows, &%{doubled: &1["id"] * 2})
    end

    assert {:ok, %WriteResult{version: version}} =
             LanceDB.SchemaEvolution.backfill_columns(table, [doubled: :int64], mapper,
               read_columns: [:id],
               batch_size: 2
             )

    assert version == before + 1
    assert {:ok, rows} = LanceDB.Query.query(table, order_by: [asc: :id])
    assert Enum.all?(rows, &(&1["doubled"] == &1["id"] * 2))
    assert length(rows) == 6
    {:ok, batch} = Batch.from_rows(Enum.map(1..6, &%{ordinal: &1}), Schema.new(ordinal: :int64))

    assert {:ok, %WriteResult{}} =
             LanceDB.SchemaEvolution.add_from_source(
               table,
               LanceDB.Source.batches([batch], schema: [ordinal: :int64]),
               []
             )
  end

  test("backfills reject short, long, late-failing or misaligned output before commit", %{
    db: db,
    schema: schema
  }) do
    {:ok, table} =
      LanceDB.Connection.create_table(
        db,
        "backfill_fail",
        LanceDB.Source.rows([%{id: 1, value: "a"}, %{id: 2, value: "b"}], schema: schema),
        []
      )

    {:ok, version} = LanceDB.History.version(table)

    for rows <- [[%{extra: 1}], [%{extra: 1}, %{extra: 2}, %{extra: 3}]] do
      assert {:error, _} =
               LanceDB.SchemaEvolution.add_from_source(
                 table,
                 LanceDB.Source.rows(rows, schema: [extra: :int64], batch_size: 1),
                 []
               )

      assert {:ok, ^version} = LanceDB.History.version(table)
    end

    late =
      Stream.concat([
        [%{extra: 1}, %{extra: 2}],
        Stream.map([1], fn _ -> raise "late failure" end)
      ])

    assert {:error, %{kind: :source}} =
             LanceDB.SchemaEvolution.add_from_source(
               table,
               LanceDB.Source.rows(late, schema: [extra: :int64], batch_size: 1),
               []
             )

    assert {:ok, ^version} = LanceDB.History.version(table)

    assert {:error, %{kind: :source}} =
             LanceDB.SchemaEvolution.backfill_columns(table, [extra: :int64], fn _ -> [] end)

    assert {:error, %{kind: :conflict}} =
             LanceDB.SchemaEvolution.add_from_source(
               table,
               LanceDB.Source.rows([], schema: [extra: :int64]),
               expected_version: version + 1
             )

    assert {:ok, ^version} = LanceDB.History.version(table)
  end

  test("empty backfills still validate the producer and commit an empty column", %{
    db: db,
    schema: schema
  }) do
    {:ok, table} =
      LanceDB.Connection.create_table(
        db,
        "empty_columns",
        LanceDB.Source.rows([], schema: schema),
        []
      )

    {:ok, version} = LanceDB.History.version(table)

    assert {:error, _} =
             LanceDB.SchemaEvolution.add_from_source(
               table,
               LanceDB.Source.rows([%{extra: 1}], schema: [extra: :int64]),
               []
             )

    assert {:ok, ^version} = LanceDB.History.version(table)

    assert {:ok, %WriteResult{}} =
             LanceDB.SchemaEvolution.add_from_source(
               table,
               LanceDB.Source.rows([], schema: [extra: :int64]),
               []
             )

    assert {:ok, %Schema{fields: fields}} = Table.schema(table)
    assert Enum.map(fields, & &1.name) == ["id", "value", "extra"]
  end
end
