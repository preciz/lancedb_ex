import ExUnit.Assertions
alias LanceDB.{Connection, Error, Index, Job, ObjectStoreFixture}

[database] = System.argv()
assert :erlang.system_info(:dirty_io_schedulers) == 1
{:ok, conn} = LanceDB.connect(database)
{:ok, _table} = Connection.create_table(conn, "docs", Enum.map(1..1_000, &%{id: &1}))
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
  :ok = ObjectStoreFixture.hold(server)
  {:ok, job} = Index.create_async(remote, :id, type: :btree, name: "ids")
  assert_receive {^server, :blocked, _key}, 5_000

  waiters = for _ <- 1..3, do: Task.async(fn -> Job.wait(job) end)
  for waiter <- waiters, do: assert(Task.yield(waiter, 25) == nil)

  status = Task.async(fn -> Job.status(job) end)
  assert {:ok, {:ok, "running"}} = Task.yield(status, 2_000)

  timed = Task.async(fn -> Job.wait(job, timeout_ms: 25) end)
  assert {:ok, {:error, %Error{kind: :timeout}}} = Task.yield(timed, 2_000)
  assert {:ok, "running"} = Job.status(job)

  owner = self()

  {caller, monitor} =
    spawn_monitor(fn ->
      reference = make_ref()
      assert {:ok, :ok} = LanceDB.Native.job_wait(job.resource, nil, reference)
      send(owner, {:waiting, self()})

      receive do
        {^reference, result} -> result
      end
    end)

  assert_receive {:waiting, ^caller}, 2_000
  Process.exit(caller, :kill)
  assert_receive {:DOWN, ^monitor, :process, ^caller, :killed}, 2_000
  assert {:ok, "running"} = Job.status(job)

  cancelling = Task.async(fn -> Job.cancel(job) end)
  assert {:ok, {:ok, :ok}} = Task.yield(cancelling, 2_000)

  for waiter <- waiters do
    assert {:error, %Error{kind: :cancelled}} = Task.await(waiter, 2_000)
  end

  assert {:error, %Error{kind: :cancelled}} = Job.wait(job)
  assert {:ok, "cancelled"} = Job.status(job)
  IO.puts("job waiters yield schedulers; deadlines and cancellation remain available")
after
  ObjectStoreFixture.release(server)
  GenServer.stop(server)
end
