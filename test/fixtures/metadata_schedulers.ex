import ExUnit.Assertions
alias LanceDB.{Batch, Branch, Connection, Error, History, ObjectStoreFixture, Schema, Table}

[database] = System.argv()
assert :erlang.system_info(:dirty_cpu_schedulers_online) == 1
# One I/O thread waits in the metadata NIF; leave another for fixture/test I/O,
# including lazy BEAM module loading. Only the CPU pool is under test here.
assert :erlang.system_info(:dirty_io_schedulers) == 2

{:ok, conn} = Connection.connect(database)
{:ok, local} = Connection.create_table(conn, "docs", [%{id: 1}, %{id: 2}])
{:ok, :created} = History.create_tag(local, "saved", 1)
{:ok, _branch} = Branch.create(local, "scratch")
{:ok, _write} = Table.add(local, [%{id: 3}])
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
    {:delete_branch, fn -> Branch.delete(remote, "missing") end, :not_found}
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

  IO.puts("history and branch storage leaves dirty CPU work available")
after
  ObjectStoreFixture.release(server)
  GenServer.stop(server)
end
