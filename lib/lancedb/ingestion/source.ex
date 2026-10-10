defmodule LanceDB.Ingestion.Source do
  @moduledoc false

  # Own the suspended continuation so a failing finalizer cannot replace the
  # consumer's error or confirmed commits. Producers still run in their caller.
  def reduce_while(enumerable, accumulator, reducer),
    do: reduce({:start, enumerable}, accumulator, reducer)

  defp reduce(state, accumulator, reducer) do
    case next(state) do
      {:done, _} ->
        {accumulator, nil}

      {item, continuation} ->
        case step(reducer, item, accumulator, continuation) do
          {:cont, accumulator} -> reduce(continuation, accumulator, reducer)
          {:halt, accumulator} -> {accumulator, close(continuation)}
        end
    end
  end

  defp step(reducer, item, accumulator, continuation) do
    reducer.(item, accumulator)
  catch
    kind, reason ->
      stacktrace = __STACKTRACE__
      close(continuation)
      :erlang.raise(kind, reason, stacktrace)
  end

  defp next(:done), do: {:done, :done}

  defp next(state) do
    case pull(state) do
      {:suspended, batch, continuation} -> {{:ok, batch}, {:next, continuation}}
      {status, _} when status in [:done, :halted] -> {:done, :done}
    end
  rescue
    error -> {{:error, source_error(Exception.message(error), :input)}, :done}
  catch
    kind, reason -> {{:error, source_error("#{kind}: #{inspect(reason)}", :input)}, :done}
  end

  defp pull({:start, enumerable}),
    do: Enumerable.reduce(enumerable, {:cont, nil}, fn batch, _ -> {:suspend, batch} end)

  defp pull({:next, continuation}), do: continuation.({:cont, nil})

  defp close({:next, continuation}) do
    continuation.({:halt, nil})
    nil
  rescue
    error -> source_error(Exception.message(error), :cleanup)
  catch
    kind, reason -> source_error("#{kind}: #{inspect(reason)}", :cleanup)
  end

  defp close(_), do: nil

  defp source_error(message, phase),
    do: %LanceDB.Error{
      kind: :source,
      message: message,
      details: %{phase: phase, commit_outcome: :unknown}
    }
end
