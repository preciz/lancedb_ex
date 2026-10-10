defmodule LanceDB.Stream do
  @moduledoc false
  alias LanceDB.Query.Executor

  def from_plan(table, plan, batch_size, mode, parent \\ %{}) do
    Stream.resource(
      fn ->
        context = Map.put_new(parent, :execution_id, make_ref())
        {unwrap!(Executor.open(table, plan, batch_size, context)), context}
      end,
      fn {cursor, context} = state ->
        case unwrap!(Executor.next_batch(cursor, context)) do
          nil -> {:halt, state}
          batch -> {items(batch, mode), state}
        end
      end,
      fn {cursor, context} -> Executor.close(cursor, context) end
    )
  end

  defp items(batch, :batches), do: [batch]

  defp items(batch, :rows),
    do: batch |> LanceDB.Backend.batch_rows() |> unwrap!() |> LanceDB.Result.rows!()

  defp unwrap!({:ok, value}), do: value
  defp unwrap!({:error, error}), do: raise(LanceDB.Error.normalize(error, :stream_next_batch))
end
