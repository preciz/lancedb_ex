import ExUnit.Assertions
alias LanceDB.{Batch, Connection, Error, Execution, ObjectStoreFixture, Query, Schema}

defmodule OpenSchedulerTelemetry do
  @moduledoc false
  def handle_event(event, measurements, metadata, pid),
    do: send(pid, {event, measurements, metadata})
end

[database] = System.argv()
assert :erlang.system_info(:dirty_cpu_schedulers_online) == 1
assert :erlang.system_info(:dirty_io_schedulers) == 1
{:ok, conn} = Connection.connect(database)
{:ok, local} = Connection.create_table(conn, "docs", [%{id: 1}])
{:ok, batch} = Batch.from_rows([%{id: 7}], Schema.new(id: :int64))
{:ok, [%{"id" => 7}]} = Batch.to_rows(batch)
{:ok, server} = ObjectStoreFixture.start_link(database)
port = ObjectStoreFixture.port(server)

options = %{
  "aws_region" => "us-east-1",
  "aws_endpoint" => "http://127.0.0.1:#{port}",
  "aws_access_key_id" => "fixture",
  "aws_secret_access_key" => "fixture",
  "allow_http" => "true",
  "aws_virtual_hosted_style_request" => "false"
}

# With no configured region, the SDK resolves it through IMDS during connect.
# Point only this isolated VM at our held HTTP fixture, never at actual AWS.
System.delete_env("AWS_REGION")
System.delete_env("AWS_DEFAULT_REGION")
System.put_env("AWS_CONFIG_FILE", "/dev/null")
System.put_env("AWS_SHARED_CREDENTIALS_FILE", "/dev/null")
System.put_env("AWS_EC2_METADATA_DISABLED", "false")
System.put_env("AWS_EC2_METADATA_SERVICE_ENDPOINT", "http://127.0.0.1:#{port}")

id = make_ref()
{:ok, _} = Application.ensure_all_started(:telemetry)

:ok =
  :telemetry.attach(
    id,
    [:lancedb, :operation, :stop],
    &OpenSchedulerTelemetry.handle_event/4,
    self()
  )

try do
  {:ok, remote_conn} = Connection.connect("s3://probe", options, timeout_ms: 5_000)

  {:ok, region_conn} =
    Connection.connect("s3://probe", Map.delete(options, "aws_region"), timeout_ms: 5_000)

  operations = [
    {:connect,
     fn opts -> Connection.connect("s3://probe", Map.delete(options, "aws_region"), opts) end},
    {:connect,
     fn opts ->
       Connection.connect(
         "s3://probe",
         Map.delete(options, "aws_region"),
         [index_cache_size_bytes: 0, metadata_cache_size_bytes: 0] ++ opts
       )
     end},
    {:open_table, fn opts -> Connection.open_table(remote_conn, "docs", opts) end},
    {:open_table,
     fn opts ->
       Connection.open_table(region_conn, "docs", [index_cache_size_bytes: 0] ++ opts)
     end}
  ]

  flush = fn flush ->
    receive do
      {^server, :blocked, _key} -> flush.(flush)
      {[:lancedb, :operation, :stop], _, _} -> flush.(flush)
    after
      0 -> :ok
    end
  end

  for {operation, call} <- operations do
    flush.(flush)
    :ok = ObjectStoreFixture.hold(server, :all)
    pending = Task.async(fn -> call.(timeout_ms: 5_000) end)
    assert_receive {^server, :blocked, _key}, 5_000

    cpu = Task.async(fn -> Batch.to_rows(batch) end)
    assert {:ok, {:ok, [%{"id" => 7}]}} = Task.yield(cpu, 1_000)
    io = Task.async(fn -> File.read(__ENV__.file) end)
    assert {:ok, {:ok, _}} = Task.yield(io, 1_000)
    timed = Task.async(fn -> call.(timeout_ms: 50) end)

    assert {:ok, {:error, %Error{kind: :timeout, operation: ^operation}}} =
             Task.yield(timed, 2_000)

    assert_receive {[:lancedb, :operation, :stop], _, %{operation: ^operation, status: :error}},
                   1_000

    assert Task.yield(pending, 0) == nil
    :ok = ObjectStoreFixture.release(server)
    assert {:ok, _} = Task.await(pending, 5_000)

    assert_receive {[:lancedb, :operation, :stop], _, %{operation: ^operation, status: :ok}},
                   1_000

    assert {:ok, :ok} = Execution.configure(reads: 1)
    flush.(flush)
    :ok = ObjectStoreFixture.hold(server, :all)
    timed = Task.async(fn -> call.(timeout_ms: 2_000) end)
    assert_receive {^server, :blocked, _key}, 5_000
    assert {:error, %Error{kind: :busy}} = Query.query(local)
    assert {:error, %Error{kind: :timeout}} = Task.await(timed, 3_000)
    # Storage is still held: success proves timeout released read admission.
    assert {:ok, [_]} = Query.query(local)
    :ok = ObjectStoreFixture.release(server)

    flush.(flush)
    :ok = ObjectStoreFixture.hold(server, :all)
    {owner, monitor} = spawn_monitor(fn -> call.([]) end)
    assert_receive {^server, :blocked, _key}, 5_000
    assert {:error, %Error{kind: :busy}} = Query.query(local)
    Process.exit(owner, :kill)
    assert_receive {:DOWN, ^monitor, :process, ^owner, :killed}, 5_000
    LanceDB.TestSupport.assert_eventually(fn -> match?({:ok, [_]}, Query.query(local)) end)
    :ok = ObjectStoreFixture.release(server)
    assert {:ok, :ok} = Execution.configure(reads: 64)
  end

  IO.puts("connect and open yield schedulers; deadlines and caller death release admission")
after
  :telemetry.detach(id)
  ObjectStoreFixture.release(server)
  GenServer.stop(server)
end
