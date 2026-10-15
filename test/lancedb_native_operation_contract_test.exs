defmodule LanceDB.NativeOperationContractTest do
  use LanceDB.DataCase, async: false
  @moduletag :native_operation_contract

  alias LanceDB.{
    Branch,
    Connection,
    Error,
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
end
