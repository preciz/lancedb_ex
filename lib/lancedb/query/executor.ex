defmodule LanceDB.Query.Executor do
  alias LanceDB.Backend.SchemaCodec
  @moduledoc false
  alias LanceDB.Backend
  alias LanceDB.Query.{Callback, Deadline, Plan}

  @spec execute(LanceDB.Table.t(), Plan.t(), :execute | LanceDB.Native.query_operation()) ::
          LanceDB.result([map()] | String.t() | LanceDB.ArrowSchema.t())
  def execute(table, plan, operation) do
    context = %{execution_id: make_ref(), operation_id: make_ref()}

    LanceDB.Execution.run(
      if(operation == :execute, do: :query, else: operation),
      fn ->
        do_execute(table, plan, operation, %{
          execution_id: context.execution_id,
          parent_operation_id: context.operation_id
        })
      end,
      Map.put(context, :query_kind, plan.request.kind)
    )
  end

  defp do_execute(table, plan, :execute, context) do
    {:ok, table |> LanceDB.Stream.from_plan(plan, 1024, :rows, context) |> Enum.to_list()}
  rescue
    error in LanceDB.Error -> {:error, error}
  end

  defp do_execute(table, plan, operation, _context) do
    if Plan.custom?(plan) and operation == :analyze do
      custom(table, plan, operation, 1024)
    else
      case Backend.query_execute(table, plan, operation) do
        {:ok, json} when operation == :schema -> SchemaCodec.decode(json, :arrow)
        result -> result
      end
    end
  end

  @spec open(LanceDB.Table.t(), Plan.t(), pos_integer(), map()) :: LanceDB.result(reference())
  def open(table, plan, batch_size, context \\ %{}) do
    LanceDB.Execution.run(
      :stream_open,
      fn -> do_open(table, plan, batch_size) end,
      Map.put(context, :query_kind, plan.request.kind)
    )
  end

  defp do_open(table, plan, batch_size) do
    if Plan.custom?(plan),
      do: custom(table, plan, :stream, batch_size),
      else: Backend.stream_open(table, plan, batch_size)
  end

  @spec next_batch(reference(), map()) :: LanceDB.result(LanceDB.Batch.t() | nil)
  def next_batch(cursor, context \\ %{}),
    do:
      LanceDB.Execution.run(
        :stream_next_batch,
        fn -> Backend.stream_next_batch(cursor) end,
        context
      )

  @spec close(reference(), map()) :: :ok
  def close(cursor, context \\ %{}),
    do: LanceDB.Execution.run(:stream_close, fn -> Backend.stream_close(cursor) end, context)

  defp custom(table, plan, operation, batch_size) do
    deadline = Deadline.start(plan.request.execution.timeout_ms)

    with {:ok, {resource, vector, text, analysis}} <-
           Backend.rerank_candidates(table, plan, operation == :analyze),
         {:ok, scores} <- Callback.run(plan.reranker, vector, text, Deadline.remaining(deadline)),
         {:ok, result} <-
           Backend.rerank_finish(resource, scores, operation, batch_size) do
      custom_result(result, operation, plan, analysis, deadline)
    end
  end

  defp custom_result(cursor, :stream, _, _, _), do: {:ok, cursor}

  defp custom_result(count, :analyze, plan, analysis, deadline) do
    {:ok,
     analysis <>
       "Reranker: custom Elixir callback, normalization=#{plan.request.controls.hybrid.normalize}\nTotal execution: elapsed_ms=#{Deadline.elapsed(deadline)}\nOutput: rows=#{count}\n"}
  end
end
