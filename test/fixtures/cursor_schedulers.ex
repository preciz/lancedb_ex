import ExUnit.Assertions
alias LanceDB.{Backend, Batch, Connection, Error, Execution, Native, ObjectStoreFixture, Query}
alias LanceDB.Query.{Definition, Request}

[database] = System.argv()
assert :erlang.system_info(:dirty_io_schedulers) == 1
{:ok, conn} = LanceDB.connect(database)
{:ok, local} = Connection.create_table(conn, "docs", [%{id: 1}, %{id: 2}])
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

  {:ok, remote_conn} = Connection.connect("s3://probe", options, read_consistency_interval_ms: 0)

  {:ok, remote} =
    Connection.open_table(remote_conn, "docs",
      index_cache_size_bytes: 0,
      metadata_cache_size_bytes: 0
    )

  :ok = ObjectStoreFixture.hold(server)
  pending = Task.async(fn -> Query.query(remote) end)
  assert_receive {^server, :blocked, _key}, 5_000

  # An unrelated query still runs while storage is held. A second held query
  # delivers its timeout without waiting for the first caller to release a thread.
  local_query = Task.async(fn -> Query.query(local, timeout_ms: 1_000) end)
  assert {:ok, {:ok, [%{"id" => 1}, %{"id" => 2}]}} = Task.yield(local_query, 2_000)
  timed = Task.async(fn -> Query.query(remote, timeout_ms: 25) end)
  assert {:ok, {:error, %Error{kind: :timeout}}} = Task.yield(timed, 2_000)
  assert Task.yield(pending, 0) == nil

  :ok = ObjectStoreFixture.release(server)
  assert {:ok, [%{"id" => 1}, %{"id" => 2}]} = Task.await(pending, 5_000)

  flush_notifications = fn flush ->
    receive do
      {^server, :blocked, _key} -> flush.(flush)
    after
      0 -> :ok
    end
  end

  flush_notifications.(flush_notifications)

  # The in-flight demand owns its cursor even when the caller's last BEAM
  # reference is collected before the native worker can return a batch.
  :ok = ObjectStoreFixture.hold(server)

  kickoff = fn ->
    request = Definition.new(:scan, limit: nil) |> Request.compile()
    {:ok, cursor} = Backend.stream_open(remote, request, 2)
    reference = make_ref()
    assert {:ok, :ok} = Native.stream_next_batch(cursor, reference)
    reference
  end

  reference = kickoff.()
  assert_receive {^server, :blocked, _key}, 5_000
  :erlang.garbage_collect()
  refute_receive {^reference, _result}, 100
  :ok = ObjectStoreFixture.release(server)
  assert_receive {^reference, {:ok, batch}}, 5_000
  assert {:ok, [%{"id" => 1}, %{"id" => 2}]} = Batch.to_rows(%Batch{resource: batch})

  assert {:ok, :ok} = Execution.configure(reads: 1)

  LanceDB.TestSupport.assert_eventually(fn ->
    match?({:ok, [%{"id" => 1}, %{"id" => 2}]}, Query.query(local))
  end)

  :ok = ObjectStoreFixture.hold(server)
  parent = self()
  tag = make_ref()

  {owner, monitor} =
    spawn_monitor(fn ->
      kickoff.()
      send(parent, {tag, :waiting})
      receive do: (:stop -> :ok)
    end)

  assert_receive {^tag, :waiting}, 5_000
  assert_receive {^server, :blocked, _key}, 5_000
  assert {:error, %Error{kind: :busy}} = Query.query(local)
  :erlang.garbage_collect(owner)
  Process.exit(owner, :kill)
  assert_receive {:DOWN, ^monitor, :process, ^owner, :killed}, 5_000

  LanceDB.TestSupport.assert_eventually(fn ->
    match?({:ok, [%{"id" => 1}, %{"id" => 2}]}, Query.query(local))
  end)

  :ok = ObjectStoreFixture.release(server)
  IO.puts("scheduler remains available; timeout delivered; storage read completed")
after
  ObjectStoreFixture.release(server)
  GenServer.stop(server)
end
