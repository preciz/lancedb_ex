defmodule LanceDB.IndexReadinessTest do
  use LanceDB.DataCase, async: false
  alias LanceDB.{Error, Execution, Index, Job, Maintenance, Query, Table}
  alias LanceDB.Query.{Executor, Request}

  setup %{path: path} do
    {:ok, conn} = LanceDB.connect(path)
    {:ok, table} = LanceDB.Connection.create_table(conn, "rows", [%{id: 1, value: "one"}])
    %{table: table}
  end

  test "named readiness waits for asynchronous builds and supports multiple names", %{
    table: table
  } do
    assert {:ok, job} = Index.create_async(table, :id, type: :btree, name: "ids")
    assert {:ok, :ready} = Index.wait(table, :ids, timeout_ms: 5_000, poll_interval_ms: 10)
    assert {:ok, :created} = Job.wait(job)
    assert {:ok, :created} = Index.create(table, :value, type: :bitmap, name: "values")
    assert {:ok, :ready} = Index.wait(table, [:ids, "values"])
  end

  test "missing and partially indexed names time out without performing maintenance", %{
    table: table
  } do
    assert {:ok, :created} = Index.create(table, :id, name: "ids")

    assert {:error,
            %Error{
              kind: :timeout,
              operation: :wait_for_index,
              details: %{pending_indexes: ["missing"], phase: :waiting}
            }} = Index.wait(table, [:ids, :missing], timeout_ms: 30, poll_interval_ms: 5)

    assert {:ok, %{inserted: 1}} = Table.add(table, [%{id: 2, value: "two"}])
    assert {:error, %Error{kind: :timeout}} = Index.wait(table, :ids, timeout_ms: 20)
    assert {:ok, %{"num_unindexed_rows" => 1}} = Index.stats(table, :ids)
    assert {:ok, :optimized} = Maintenance.optimize_indices(table)
    assert {:ok, :ready} = Index.wait(table, :ids)
  end

  test "a missing index can become ready during polling", %{table: table} do
    waiter =
      Task.async(fn -> Index.wait(table, :later, timeout_ms: 5_000, poll_interval_ms: 10) end)

    assert {:ok, job} = Index.create_async(table, :value, type: :bitmap, name: "later")
    assert {:ok, :ready} = Task.await(waiter)
    assert {:ok, :created} = Job.wait(job)
  end

  test "inspection failures propagate and every wait emits one operation span", %{table: table} do
    assert {:ok, :created} = Index.create(table, :id, name: "ids")
    handler = make_ref()

    :ok =
      :telemetry.attach_many(
        handler,
        [[:lancedb, :operation, :start], [:lancedb, :operation, :stop]],
        &__MODULE__.event/4,
        self()
      )

    on_exit(fn -> :telemetry.detach(handler) end)
    assert {:ok, :ready} = Index.wait(table, :ids)
    assert_receive {:event, :start, :wait_for_index}
    assert_receive {:event, :stop, :wait_for_index}
    refute_receive {:event, _, _}
    assert {:ok, _} = Table.add(table, Enum.map(2..100, &%{id: &1, value: "more"}))
    Execution.configure(reads: 1)
    on_exit(fn -> Execution.configure([]) end)
    query = Query.new(table)

    {:ok, cursor} =
      Executor.open(table, Request.compile(query.definition), 1)

    assert {:ok, _batch} = Executor.next_batch(cursor)

    try do
      assert {:error, %Error{kind: :busy, operation: :wait_for_index}} =
               Index.wait(table, :ids, timeout_ms: 1_000)
    after
      Executor.close(cursor)
    end
  end

  test "invalid wait arguments fail immediately", %{table: table} do
    for opts <- [
          [timeout_ms: 0],
          [timeout_ms: 7_200_001],
          [timeout_ms: nil],
          [poll_interval_ms: 0],
          [poll_interval_ms: 60_001],
          [unknown: true]
        ] do
      assert_raise ArgumentError, fn -> Index.wait(table, :ids, opts) end
    end

    for names <- [[], [:ids, "ids"], [""]],
        do: assert_raise(ArgumentError, fn -> Index.wait(table, names) end)
  end

  def event([:lancedb, :operation, event], _, metadata, pid),
    do: send(pid, {:event, event, metadata.operation})
end
