import ExUnit.Assertions

alias LanceDB.{Batch, Connection, Error, Execution, Native, ObjectStoreFixture, Schema, Table}

[database] = System.argv()
assert :erlang.system_info(:dirty_cpu_schedulers_online) == 1
assert :erlang.system_info(:dirty_io_schedulers) == 1
{:ok, conn} = Connection.connect(database)
{:ok, local} = Connection.create_table(conn, "docs", [%{id: 1}])
{:ok, batch} = Batch.from_rows([%{id: 7}], Schema.new(id: :int64))
{:ok, [%{"id" => 7}]} = Batch.to_rows(batch)
{:ok, server} = ObjectStoreFixture.start_link(database)

try do
  options = %{
    "aws_region" => "us-east-1",
    "aws_endpoint" => "http://127.0.0.1:#{ObjectStoreFixture.port(server)}",
    "aws_access_key_id" => "fixture",
    "aws_secret_access_key" => "fixture",
    "allow_http" => "true",
    "aws_virtual_hosted_style_request" => "false"
  }

  {:ok, remote_conn} = Connection.connect("s3://probe", options)
  {:ok, remote} = Connection.open_table(remote_conn, "docs")
  assert {:ok, :ok} = Execution.configure(writes: 1)
  {:ok, writer} = Native.ingest_open(remote.resource, "{}", nil)
  :ok = ObjectStoreFixture.hold(server, :all)
  assert {:ok, :ok} = Native.ingest_push_batch(writer, batch.resource)

  finish = make_ref()
  assert {:ok, :ok} = Native.ingest_finish(writer, finish)
  assert_receive {^server, :blocked, _key}, 5_000

  cpu = Task.async(fn -> Batch.to_rows(batch) end)
  assert {:ok, {:ok, [%{"id" => 7}]}} = Task.yield(cpu, 2_000)
  io = Task.async(fn -> File.read(__ENV__.file) end)
  assert {:ok, {:ok, _}} = Task.yield(io, 2_000)

  abort = make_ref()
  assert {:ok, :ok} = Native.ingest_abort(writer, abort)
  refute_receive {^finish, _}, 100
  refute_receive {^abort, _}, 100
  assert {:error, %Error{kind: :busy}} = Table.add(local, [%{id: 2}])

  # The read-only fixture rejects the attempted upload after release. Both
  # finish and abort must await that SDK failure, rather than drop its future.
  :ok = ObjectStoreFixture.release(server)
  assert_receive {^finish, {:error, _}}, 5_000
  assert_receive {^abort, {:ok, :ok}}, 5_000
  assert {:ok, _} = Table.add(local, [%{id: 2}])
  assert {:ok, 2} = Table.count_rows(local)
  IO.puts("writer finish and abort yield schedulers and await SDK settlement")
after
  ObjectStoreFixture.release(server)
  GenServer.stop(server)
end
