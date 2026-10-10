defmodule LanceDB.Index.Readiness do
  @moduledoc false
  alias LanceDB.{Backend, Error, Options}

  def wait(table, names, opts) do
    opts = Keyword.validate!(opts, [:timeout_ms, :poll_interval_ms])
    names = names |> List.wrap() |> Options.column_names!()
    timeout = Keyword.get(opts, :timeout_ms, 60_000)
    interval = Keyword.get(opts, :poll_interval_ms, 1_000)

    if !(is_integer(timeout) and timeout > 0 and timeout <= 7_200_000),
      do: raise(ArgumentError, "timeout_ms must be positive and at most two hours (7_200_000)")

    if !(is_integer(interval) and interval > 0 and interval <= 60_000),
      do: raise(ArgumentError, "poll_interval_ms must be positive and at most 60_000")

    poll(table, names, now() + timeout, interval)
  end

  defp poll(table, names, deadline, interval) do
    case deadline - now() do
      remaining when remaining > 0 ->
        case Backend.index_readiness(table, names, remaining) do
          {:ok, []} -> {:ok, :ready}
          {:ok, pending} -> pause(table, pending, deadline, interval)
          {:error, %{kind: :timeout}} -> timeout(names)
          error -> error
        end

      _ ->
        timeout(names)
    end
  end

  defp pause(table, pending, deadline, interval) do
    Process.sleep(max(0, min(interval, deadline - now())))
    poll(table, pending, deadline, interval)
  end

  defp timeout(names),
    do:
      {:error,
       %Error{
         kind: :timeout,
         message: "Timed out waiting for indexes: #{Enum.join(names, ", ")}",
         details: %{pending_indexes: names, phase: :waiting}
       }}

  defp now, do: System.monotonic_time(:millisecond)
end
