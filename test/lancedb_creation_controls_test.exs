defmodule LanceDB.CreationControlsTest do
  use LanceDB.DataCase, async: true
  alias LanceDB.{Batch, Connection, Query, Schema, Source, Table}

  setup %{path: path} do
    {:ok, conn} = LanceDB.connect(path)
    %{conn: conn, path: path}
  end

  test("creation controls apply to inferred, row and Arrow sources", %{conn: conn}) do
    schema = Schema.new(id: :int64)

    rows =
      for id <- 1..8 do
        %{id: id}
      end

    {:ok, batch} = Batch.from_rows(rows, schema)

    inputs = [
      rows,
      Source.rows(rows, schema: schema, batch_size: 2),
      Source.batches([batch], schema: schema)
    ]

    for {input, n} <- Enum.with_index(inputs) do
      assert {:ok, table} =
               Connection.create_table(conn, "controlled_#{n}", input,
                 max_rows_per_file: 2,
                 max_rows_per_group: 1,
                 max_bytes_per_file: 1_048_576,
                 data_storage_version: "2.2",
                 enable_stable_row_ids: true,
                 enable_v2_manifest_paths: false
               )

      assert {:ok, false} = LanceDB.Maintenance.uses_v2_manifest_paths(table)

      assert {:ok, %{"fragment_stats" => %{"num_fragments" => 4}}} =
               LanceDB.Maintenance.stats(table)

      assert {:ok, before} = Query.query(table, with_row_id: true, order_by: [asc: :id])
      assert {:ok, _} = LanceDB.Maintenance.compact_files(table, target_rows_per_fragment: 100)
      assert {:ok, ^before} = Query.query(table, with_row_id: true, order_by: [asc: :id])
      assert {:ok, :migrated} = LanceDB.Maintenance.migrate_manifest_paths_v2(table)
      assert {:ok, true} = LanceDB.Maintenance.uses_v2_manifest_paths(table)
      assert {:ok, :migrated} = LanceDB.Maintenance.migrate_manifest_paths_v2(table)
      assert {:ok, ^before} = Query.query(table, with_row_id: true, order_by: [asc: :id])
    end
  end

  test("explicit location works for creation, reopen and exist_ok without consuming input", %{
    conn: conn,
    path: path
  }) do
    location = Path.join(path, "external/custom.lance")
    opts = [location: location, storage_options: %{new_table_enable_v2_manifest_paths: false}]
    assert {:ok, table} = Connection.create_table(conn, "custom", [%{id: 1}], opts)
    assert {:ok, false} = LanceDB.Maintenance.uses_v2_manifest_paths(table)
    assert {:ok, reopened} = Connection.open_table(conn, "custom", location: location)
    assert {:ok, [%{"id" => 1}]} = Query.query(reopened)

    source =
      Source.rows(Stream.map([1], fn _ -> flunk("exist_ok consumed input") end),
        schema: Schema.new(id: :int64)
      )

    for input <- [source, []] do
      assert {:ok, existing} =
               Connection.create_table(conn, "custom", input, [mode: :exist_ok] ++ opts)

      assert {:ok, 1} = Table.count_rows(existing)
    end

    assert {:ok, replaced} =
             Connection.create_table(conn, "custom", [%{id: 2}], [mode: :overwrite] ++ opts)

    assert {:ok, [%{"id" => 2}]} = Query.query(replaced)
  end

  test("empty tables accept controls and historical views cannot migrate", %{conn: conn}) do
    assert {:ok, table} =
             Connection.create_empty_table(conn, "empty", [id: :int64],
               enable_v2_manifest_paths: false,
               data_storage_version: :stable
             )

    assert {:ok, false} = LanceDB.Maintenance.uses_v2_manifest_paths(table)
    assert {:ok, snapshot} = LanceDB.History.snapshot(table, 1)
    assert {:error, _} = LanceDB.Maintenance.migrate_manifest_paths_v2(snapshot)
    assert {:ok, false} = LanceDB.Maintenance.uses_v2_manifest_paths(table)

    assert {:ok, _} =
             Connection.create_empty_table(conn, "next", [id: :int64],
               data_storage_version: :next
             )
  end

  test "exist_ok opens the winner when creation races with source consumption", %{
    conn: conn,
    path: path
  } do
    schema = Schema.new(id: :int64)

    for kind <- [:rows, :batches], external <- [false, true] do
      name = "race_#{kind}_#{external}"
      opts = if external, do: [location: Path.join(path, "external/#{name}.lance")], else: []

      rows =
        Stream.map(1..100, fn id ->
          # The initial open has already missed, but no input has reached the
          # competing writer yet. Complete the winning creation before yielding.
          if id == 1 do
            assert {:ok, _} = Connection.create_table(conn, name, [%{id: 99}], opts)
          end

          %{id: id}
        end)

      input =
        case kind do
          :rows ->
            Source.rows(rows, schema: schema, batch_size: 1)

          :batches ->
            batches =
              Stream.map(rows, fn row ->
                {:ok, batch} = Batch.from_rows([row], schema)
                batch
              end)

            Source.batches(batches, schema: schema)
        end

      assert {:ok, existing} =
               Connection.create_table(conn, name, input, [mode: :exist_ok] ++ opts)

      assert {:ok, [%{"id" => 99}]} = Query.query(existing)
      assert {:ok, 1} = LanceDB.History.version(existing)
    end
  end

  test "exist_ok preserves source failures and their details", %{conn: conn} do
    input =
      Source.rows(Stream.map([1], fn _ -> raise "producer failed" end),
        schema: Schema.new(id: :int64)
      )

    assert {:error,
            %LanceDB.Error{
              kind: :source,
              operation: :create_table,
              message: "producer failed",
              details: %{phase: :input, inserted: 0, commits: 0}
            }} = Connection.create_table(conn, "failed_source", input, mode: :exist_ok)

    assert {:ok, []} = Connection.table_names(conn)
  end

  test "invalid creation controls fail before source enumeration" do
    for key <- [:max_rows_per_file, :max_rows_per_group, :max_bytes_per_file],
        value <- [0, -1, nil, 1.5, 4_294_967_296] do
      assert_raise ArgumentError, fn ->
        Connection.create_table(:unused, "bad", [], [{key, value}])
      end
    end

    for opts <- [
          [location: ""],
          [storage_options: []],
          [storage_options: %{key: <<255>>}],
          [enable_stable_row_ids: nil],
          [enable_v2_manifest_paths: :yes],
          [data_storage_version: "3.0"],
          [mode: :append],
          [unknown: true]
        ] do
      assert_raise ArgumentError, fn -> Connection.create_table(:unused, "bad", [], opts) end
    end
  end
end
