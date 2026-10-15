defmodule LanceDB.NativeOperationContractTest do
  use LanceDB.DataCase, async: false
  @moduletag :native_operation_contract

  alias LanceDB.{
    Backend,
    Blob,
    Branch,
    Connection,
    Error,
    Execution,
    History,
    Index,
    Job,
    Maintenance,
    Metadata,
    Native,
    Query,
    SchemaEvolution,
    Table
  }

  alias LanceDB.Query.{Definition, Request}

  setup %{path: path} do
    {:ok, conn} = Connection.connect(path)
    {:ok, table} = Connection.create_table(conn, "rows", [%{id: 1, text: "elixir"}])
    {:ok, :created} = Index.create(table, :id, type: :btree, name: "ids")
    %{conn: conn, table: table}
  end

  test "all table mutations reject snapshots before execution", %{table: table} do
    {:ok, version} = History.version(table)
    {:ok, schema} = Table.schema(table)
    {:ok, snapshot} = History.snapshot(table, version)

    operations = [
      delete: &Table.delete(&1, "id = 1"),
      update: &Table.update(&1, %{text: "changed"}),
      add: &Table.add(&1, [%{id: 2, text: "later"}]),
      upsert: &Table.upsert(&1, [%{id: 2, text: "later"}]),
      add_columns: &SchemaEvolution.add_columns(&1, extra: "missing + 1"),
      alter_columns: &SchemaEvolution.alter_columns(&1, text: [rename: :title]),
      drop_columns: &SchemaEvolution.drop_columns(&1, [:text]),
      create_index: &Index.create(&1, :id, type: :btree, name: "other"),
      create_index_async: &Index.create_async(&1, :id, type: :btree, name: "other"),
      drop_index: &Index.drop(&1, "ids"),
      optimize: &Maintenance.optimize/1,
      optimize_indices: &Maintenance.optimize_indices/1,
      compact_files: &Maintenance.compact_files/1,
      cleanup_old_versions: &Maintenance.cleanup_old_versions/1,
      migrate_manifest_paths_v2: &Maintenance.migrate_manifest_paths_v2/1,
      create_tag: &History.create_tag(&1, "saved", version),
      update_tag: &History.update_tag(&1, "saved", version),
      delete_tag: &History.delete_tag(&1, "saved"),
      create_branch: &Branch.create(&1, "scratch"),
      delete_branch: &Branch.delete(&1, "scratch"),
      update_schema_metadata: &Metadata.update_schema(&1, %{owner: "test"}),
      update_field_metadata: &Metadata.update_field(&1, :id, %{unit: "test"}),
      restore: &History.restore(&1, version)
    ]

    for {name, operation} <- operations do
      result = operation.(snapshot)

      assert match?(
               {:error, %Error{details: %{phase: :validation, commit_outcome: :not_started}}},
               result
             ),
             "#{name} must reject the snapshot before execution, got: #{inspect(result)}"
    end

    assert {:ok, ^version} = History.version(table)
    assert {:ok, ^schema} = Table.schema(table)
    assert {:ok, [%{"id" => 1, "text" => "elixir"}]} = Query.query(table)
    assert {:ok, [%Index.Description{name: "ids"}]} = Index.list(table)
    assert {:ok, %{}} = History.list_tags(table)
  end

  test "column expression planning failures have not started a write", %{table: table} do
    {:ok, version} = History.version(table)

    for expression <- ["missing + 1", "id +", "CAST(id AS imaginary_type)"] do
      assert {:error,
              %Error{
                operation: :add_columns,
                details: %{phase: :validation, commit_outcome: :not_started}
              }} = SchemaEvolution.add_columns(table, extra: expression)
    end

    assert {:ok, ^version} = History.version(table)
  end

  test "native request validation consistently reports writes that have not started", %{
    conn: conn,
    table: table
  } do
    for operation <- [
          fn -> Native.create_index(table.resource, "id", "{}") end,
          fn -> Native.create_index_async(table.resource, "id", "{}") end,
          fn -> Native.optimize_indices(table.resource, "invalid JSON") end,
          fn -> Native.compact_files(table.resource, "invalid JSON") end,
          fn -> Native.cleanup_old_versions(table.resource, "invalid JSON") end,
          fn -> Native.create_branch(table.resource, "scratch", "invalid JSON") end,
          fn -> Native.tag_action(table.resource, "invalid", "tag", nil) end,
          fn -> Native.drop_table(conn.resource, "invalid/name") end
        ] do
      assert {:error, %{details: %{phase: :validation, commit_outcome: :not_started}}} =
               operation.()
    end
  end

  test "SDK mutation failures preserve unknown commit outcomes", %{conn: conn, table: table} do
    {:ok, version} = History.version(table)
    {:ok, :created} = History.create_tag(table, "keep", version)
    {:ok, _} = Table.add(table, [%{id: 2, text: "later"}])
    {:ok, source_uri} = Table.uri(table)

    for operation <- [
          fn -> Index.create(table, :missing, type: :btree) end,
          fn -> Index.drop(table, "missing") end,
          fn -> History.create_tag(table, "missing_version", 999_999) end,
          fn -> History.update_tag(table, "missing", version) end,
          fn -> History.delete_tag(table, "missing") end,
          fn -> Branch.create(table, "scratch", from: {:tag, "missing"}) end,
          fn -> Branch.delete(table, "missing") end,
          fn -> Maintenance.cleanup_old_versions(table, older_than_seconds: 0) end,
          fn -> Connection.clone_table(conn, "rows", source_uri) end
        ] do
      assert {:error, %Error{details: %{phase: :execution, commit_outcome: :unknown}}} =
               operation.()
    end
  end

  test "clone source lookup failures occur before committing", %{conn: conn, path: path} do
    assert {:error, %Error{details: %{phase: :validation, commit_outcome: :not_started}}} =
             Connection.clone_table(conn, "copy", Path.join(path, "missing.lance"))

    assert {:ok, ["rows"]} = Connection.table_names(conn)
  end

  test "asynchronous index failures keep write metadata for repeated waits", %{table: table} do
    assert {:ok, job} = Index.create_async(table, :id, type: :btree, name: "ids")

    assert {:error, %Error{details: %{phase: :execution, commit_outcome: :unknown}} = error} =
             Job.wait(job)

    assert {:error, ^error} = Job.wait(job)
  end

  test "cursor ownership is enforced before active and terminal results", %{table: table} do
    for state <- [:active, :eof, :error, :closed] do
      options =
        case state do
          :eof -> [filter: "id < 0"]
          :error -> [filter: "missing = 1"]
          _ -> []
        end

      plan = :scan |> Definition.new(options) |> Request.compile()
      {:ok, cursor} = Backend.stream_open(table, plan, 1)

      try do
        case state do
          :eof -> assert {:ok, nil} = Backend.stream_next_batch(cursor)
          :error -> assert {:error, _} = Backend.stream_next_batch(cursor)
          :closed -> assert :ok = Backend.stream_close(cursor)
          :active -> :ok
        end

        assert {:error, %{message: "Stream cursor belongs to another process"}} =
                 Task.async(fn -> Backend.stream_next_batch(cursor) end) |> Task.await()
      after
        Backend.stream_close(cursor)
      end
    end
  end

  test "blob cursor inspection and cleanup work while read admission is saturated", %{conn: conn} do
    blob = blob(conn, "abcdef")
    {:ok, metadata} = Blob.info(blob)
    {:ok, :ok} = Execution.configure(reads: 1)
    on_exit(fn -> Execution.configure([]) end)
    {:ok, table} = Connection.open_table(conn, "rows")
    plan = :scan |> Definition.new([]) |> Request.compile()
    {:ok, cursor} = Backend.stream_open(table, plan, 1)
    assert {:ok, _} = Backend.stream_next_batch(cursor)

    try do
      assert {:error, %Error{kind: :busy}} = Blob.read(blob, 1)
      assert {:ok, 0} = Blob.tell(blob)
      assert {:ok, :ok} = Blob.seek(blob, 2)
      assert {:ok, 2} = Blob.tell(blob)
      assert {:ok, ^metadata} = Blob.info(blob)
      assert {:ok, :ok} = Blob.close(blob)
      assert {:error, %Error{kind: :invalid_data}} = Blob.tell(blob)
    after
      Backend.stream_close(cursor)
      Blob.close(blob)
    end
  end

  test "empty blob streams check handle state lazily and retain cursor positions", %{conn: conn} do
    blob = blob(conn, "")
    assert {:ok, :ok} = Blob.seek(blob, 10)
    assert Enum.to_list(Blob.stream(blob)) == []
    assert {:ok, 10} = Blob.tell(blob)
    stream = Blob.stream(blob)
    assert {:ok, :ok} = Blob.close(blob)
    closed_stream = Blob.stream(blob)

    for stream <- [stream, closed_stream] do
      error = assert_raise Error, fn -> Enum.to_list(stream) end
      assert error.kind == :invalid_data
      assert error.operation == :blob_read_range
    end
  end

  defp blob(conn, value) do
    {:ok, table} =
      Connection.create_table(conn, "blobs", [%{id: 1, payload: value}],
        schema: [id: :int64, payload: :blob]
      )

    {:ok, [%{"_rowid" => id}]} = Query.query(table, with_row_id: true)
    {:ok, [blob]} = Blob.open(table, :payload, [id])
    blob
  end
end
