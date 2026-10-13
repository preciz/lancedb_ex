defmodule LanceDB.JobTest do
  use LanceDB.DataCase, async: true
  alias LanceDB.{Index, Job}

  setup %{path: path} do
    {:ok, db} = LanceDB.connect(path)

    {:ok, table} =
      LanceDB.Connection.create_table(db, "rows", Enum.map(1..100, &%{id: &1, vector: [&1, 1]}))

    %{table: table}
  end

  test("native jobs are shareable and retain completion for repeated waits", %{table: table}) do
    assert {:ok, %Job{} = job} = Index.create_async(table, :id, type: :btree, name: "ids")
    assert {:ok, status} = Job.status(job)
    assert status in ["running", "finished"]

    assert {:ok, :created} =
             Task.async(fn -> Job.wait(job, timeout_ms: 10_000) end) |> Task.await()

    assert {:ok, :created} = Job.wait(job)
    assert {:ok, "finished"} = Job.status(job)
    assert {:ok, [%LanceDB.Index.Description{name: "ids"}]} = Index.list(table)
    assert {:ok, :ok} = Job.cancel(job)
    assert {:ok, :created} = Job.wait(job)
    assert_raise ArgumentError, fn -> Job.wait(job, timeout_ms: -1) end
  end

  test("untrained scalar indexes and invalid jobs", %{table: table}) do
    assert {:ok, :created} = Index.create(table, :id, type: :btree, train: false)
    assert {:ok, %{"num_indexed_rows" => 0}} = Index.stats(table, "id_idx")
    assert {:error, _} = Index.create_async(table, :missing, type: :btree)

    assert_raise ArgumentError, fn ->
      Index.create_async(table, :vector, type: :ivf_flat, train: false)
    end

    assert_raise ArgumentError, fn -> Index.create(table, :id, train: :no) end
  end

  test("asynchronous build failures remain available to every waiter", %{table: table}) do
    assert {:ok, :created} = Index.create(table, :id, type: :btree, name: "existing")

    assert {:ok, job} =
             Index.create_async(table, :id, type: :btree, name: "existing")

    assert {:error, error} = Job.wait(job, timeout_ms: 10_000)
    assert error.kind == :execution
    assert {:ok, "failed"} = Job.status(job)
    assert {:error, ^error} = Job.wait(job)
  end

  test("wait deadlines leave jobs available for cancellation and settlement", %{table: table}) do
    {:ok, job} = Index.create_async(table, :vector, type: :ivf_flat, num_partitions: 1)

    case Job.wait(job, timeout_ms: 0) do
      {:ok, :created} -> :ok
      {:error, error} -> assert error.kind == :timeout
    end

    assert {:ok, :ok} = Job.cancel(job)

    case Job.wait(job, timeout_ms: 10_000) do
      {:ok, :created} ->
        assert {:ok, "finished"} = Job.status(job)

      {:error, error} ->
        assert error.kind == :cancelled
        assert {:ok, "cancelled"} = Job.status(job)
    end
  end
end
