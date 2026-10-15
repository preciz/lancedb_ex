defmodule LanceDB.Stream do
  @moduledoc false
  alias LanceDB.Query.Executor

  @spec from_plan(
          LanceDB.Table.t(),
          LanceDB.Query.Plan.t(),
          pos_integer(),
          :rows | :batches,
          map()
        ) :: Enumerable.t()
  def from_plan(table, plan, batch_size, mode, parent \\ %{}) do
    Stream.resource(
      fn ->
        context = Map.put_new(parent, :execution_id, make_ref())
        {unwrap!(Executor.open(table, plan, batch_size, context), :stream_open), context}
      end,
      fn {cursor, context} = state ->
        case unwrap!(Executor.next_batch(cursor, context), :stream_next_batch) do
          nil -> {:halt, state}
          batch -> {items(batch, mode), state}
        end
      end,
      fn {cursor, context} -> Executor.close(cursor, context) end
    )
  end

  defp items(batch, :batches), do: [batch]

  defp items(batch, :rows),
    do: batch |> LanceDB.Backend.batch_rows() |> unwrap!(:batch_rows) |> LanceDB.Result.rows!()

  defp unwrap!({:ok, value}, _operation), do: value

  defp unwrap!({:error, %LanceDB.Error{operation: operation} = error}, _fallback)
       when not is_nil(operation),
       do: raise(error)

  defp unwrap!({:error, error}, operation), do: raise(LanceDB.Error.normalize(error, operation))
end
