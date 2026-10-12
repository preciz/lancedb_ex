import ExUnit.Assertions

alias LanceDB.{
  Batch,
  Branch,
  Connection,
  Error,
  Execution,
  History,
  Index,
  Maintenance,
  Metadata,
  ObjectStoreFixture,
  Query,
  Schema,
  Table
}

[database] = System.argv()
assert :erlang.system_info(:dirty_cpu_schedulers_online) == 1
assert :erlang.system_info(:dirty_io_schedulers) == 1

{:ok, conn} = Connection.connect(database)

{:ok, local} =
  Connection.create_table(
    conn,
    "docs",
    for(id <- 1..10, do: %{id: id, text: "document #{id}", vector: [id / 10, 1.0]})
  )

{:ok, :created} = Index.create(local, :id, type: :btree, name: "ids")
{:ok, :created} = Index.create(local, :text, type: :fts, name: "texts")

{:ok, :created} =
  Index.create(local, :vector, type: :ivf_flat, num_partitions: 1, name: "vectors")

{:ok, :created} = History.create_tag(local, "saved", 1)
{:ok, _branch} = Branch.create(local, "scratch")
{:ok, _write} = Table.add(local, [%{id: 11, text: "later document", vector: [1.1, 1.0]}])
{:ok, batch} = Batch.from_rows([%{id: 7}], Schema.new(id: :int64))
# Warm the decoder so the CPU probe measures Arrow conversion.
{:ok, [%{"id" => 7}]} = Batch.to_rows(batch)
{:ok, server} = ObjectStoreFixture.start_link(database)
port = ObjectStoreFixture.port(server)

try do
  options = %{
    "aws_region" => "us-east-1",
    "aws_endpoint" => "http://127.0.0.1:#{port}",
    "aws_access_key_id" => "fixture",
    "aws_secret_access_key" => "fixture",
    "allow_http" => "true",
    "aws_virtual_hosted_style_request" => "false"
  }

  {:ok, remote_conn} = Connection.connect("s3://probe", options)
  {:ok, remote} = Connection.open_table(remote_conn, "docs")

  {:ok, consistent_conn} =
    Connection.connect("s3://probe", options, read_consistency_interval_ms: 0)

  {:ok, consistent} = Connection.open_table(consistent_conn, "docs")

  flush_notifications = fn flush ->
    receive do
      {^server, :blocked, _key} -> flush.(flush)
    after
      0 -> :ok
    end
  end

  # Missing references exercise storage waits without mutating the read-only
  # fixture. Successful writes are covered by the history and branch suites.
  operations = [
    {:list_tags, fn -> History.list_tags(remote) end, :ok},
    {:tag_version, fn -> History.tag_version(remote, "saved") end, :ok},
    {:tag_action, fn -> History.update_tag(remote, "missing", 1) end, :not_found},
    {:table_version, fn -> History.version(consistent) end, :ok},
    {:list_versions, fn -> History.list_versions(remote) end, :ok},
    {:snapshot_table, fn -> History.snapshot(remote, 1) end, :ok},
    {:restore_version, fn -> History.restore(remote, 9999) end, :not_found},
    {:create_branch, fn -> Branch.create(remote, "new", from: {:tag, "missing"}) end, :not_found},
    {:checkout_branch, fn -> Branch.open(remote, "scratch") end, :ok},
    {:list_branches, fn -> Branch.list(remote) end, :ok},
    {:delete_branch, fn -> Branch.delete(remote, "missing") end, :not_found},
    {:list_indices, fn -> Index.list(consistent) end, :ok},
    {:index_stats, fn -> Index.stats(consistent, "vectors") end, :ok},
    {:drop_index, fn -> Index.drop(consistent, "missing") end, :not_found},
    {:prewarm_index, fn -> Index.prewarm(consistent, "ids") end, :ok},
    {:schema_metadata, fn -> Metadata.schema(consistent) end, :ok},
    {:get_schema_json, fn -> Table.schema(consistent) end, :ok},
    {:table_stats, fn -> Maintenance.stats(consistent) end, :ok},
    {:cache_stats, fn -> Table.cache_stats(consistent) end, :ok},
    {:count_rows, fn -> Table.count_rows(consistent, "id > 0") end, :ok},
    {:query_explain, fn -> consistent |> Query.new() |> Query.explain() end, :ok},
    {:query_analyze, fn -> consistent |> Query.new() |> Query.analyze() end, :ok},
    {:query_schema, fn -> consistent |> Query.new() |> Query.schema() end, :ok},
    {:drop_table, fn -> Connection.drop_table(remote_conn, "missing") end, :ok},
    {:table_names, fn -> Connection.table_names(remote_conn) end, :ok}
  ]

  for {operation, call, expected} <- operations do
    flush_notifications.(flush_notifications)
    :ok = ObjectStoreFixture.hold(server, :all)
    pending = Task.async(call)

    try do
      assert_receive {^server, :blocked, _key}, 5_000
      assert Task.yield(pending, 0) == nil

      # Arrow conversion uses DirtyCpu and no storage or admission slot. It must
      # complete while the metadata operation is still waiting on S3.
      cpu = Task.async(fn -> Batch.to_rows(batch) end)

      result = Task.yield(cpu, 2_000)

      assert match?({:ok, {:ok, [%{"id" => 7}]}}, result),
             "#{operation} occupied the only dirty CPU scheduler: #{inspect(result)}"

      io = Task.async(fn -> File.read(__ENV__.file) end)
      assert {:ok, {:ok, _}} = Task.yield(io, 2_000)

      assert Task.yield(pending, 0) == nil
    after
      ObjectStoreFixture.release(server)
    end

    result = Task.await(pending, 5_000)

    case expected do
      :ok -> assert {:ok, _value} = result
      :not_found -> assert {:error, %Error{kind: :not_found}} = result
    end
  end

  # The pinned embedded SDK's URI accessor only clones a stored string. Even
  # with read consistency enabled, it must not request storage or wait on it.
  flush_notifications.(flush_notifications)
  :ok = ObjectStoreFixture.hold(server, :all)

  try do
    uri = Task.async(fn -> Table.uri(consistent) end)
    assert {:ok, {:ok, "s3://probe/docs.lance"}} = Task.yield(uri, 2_000)
    refute_receive {^server, :blocked, _key}, 100
  after
    ObjectStoreFixture.release(server)
  end

  # One-shot reads cancel on caller death and release admission before storage
  # resumes. Mutations retain their slot until their SDK future has settled.
  assert {:ok, :ok} = Execution.configure(reads: 1)
  flush_notifications.(flush_notifications)
  :ok = ObjectStoreFixture.hold(server, :all)
  {caller, monitor} = spawn_monitor(fn -> Table.count_rows(consistent) end)
  assert_receive {^server, :blocked, _}, 5_000
  assert {:error, %Error{kind: :busy}} = Table.count_rows(local)
  Process.exit(caller, :kill)
  assert_receive {:DOWN, ^monitor, :process, ^caller, :killed}, 2_000

  LanceDB.TestSupport.assert_eventually(fn ->
    Table.count_rows(local) == {:ok, 11}
  end)

  :ok = ObjectStoreFixture.release(server)
  assert {:ok, :ok} = Execution.configure(writes: 1)
  flush_notifications.(flush_notifications)
  :ok = ObjectStoreFixture.hold(server, :all)
  {caller, monitor} = spawn_monitor(fn -> History.update_tag(remote, "missing", 1) end)
  assert_receive {^server, :blocked, _}, 5_000
  assert {:error, %Error{kind: :busy}} = History.update_tag(local, "missing", 1)
  Process.exit(caller, :kill)
  assert_receive {:DOWN, ^monitor, :process, ^caller, :killed}, 2_000
  assert {:error, %Error{kind: :busy}} = History.update_tag(local, "missing", 1)
  :ok = ObjectStoreFixture.release(server)

  LanceDB.TestSupport.assert_eventually(fn ->
    match?({:error, %Error{kind: :not_found}}, History.update_tag(local, "missing", 1))
  end)

  IO.puts(
    "held storage yields both dirty scheduler pools; read cancellation and write settlement hold"
  )
after
  ObjectStoreFixture.release(server)
  GenServer.stop(server)
end
