defmodule LanceDB.Query.Callback do
  @moduledoc false
  alias LanceDB.Result

  @spec run(
          ([map()], [map()] -> [map()] | {:ok, [map()]} | {:error, term()}),
          String.t(),
          String.t(),
          timeout()
        ) :: {:ok, [map()]} | {:error, String.t() | LanceDB.Error.t()}
  def run(callback, vector, text, timeout) do
    owner = self()
    tag = make_ref()

    {pid, monitor} =
      spawn_monitor(fn ->
        watch_owner(owner, self())
        send(owner, {tag, safely_invoke(callback, vector, text)})
      end)

    receive do
      {^tag, result} ->
        Process.demonitor(monitor, [:flush])
        result

      {:DOWN, ^monitor, :process, ^pid, reason} ->
        {:error, "Reranker exited: #{inspect(reason)}"}
    after
      timeout ->
        Process.exit(pid, :kill)

        receive do
          {:DOWN, ^monitor, :process, ^pid, _} -> :ok
        end

        receive do
          {^tag, _} -> :ok
        after
          0 -> :ok
        end

        {:error,
         %LanceDB.Error{kind: :timeout, message: "Query timed out during custom reranking"}}
    end
  end

  # Cancel an unlinked callback if its caller dies, and stop the watcher when
  # the callback exits. This also isolates untrappable callback exits (:kill).
  defp watch_owner(owner, worker) do
    spawn(fn ->
      owner_monitor = Process.monitor(owner)
      worker_monitor = Process.monitor(worker)

      receive do
        {:DOWN, ^owner_monitor, :process, ^owner, _} -> Process.exit(worker, :kill)
        {:DOWN, ^worker_monitor, :process, ^worker, _} -> :ok
      end
    end)
  end

  defp safely_invoke(callback, vector, text) do
    callback.(Result.rows!(vector), Result.rows!(text)) |> scores!()
  rescue
    exception -> {:error, "Reranker failed: #{Exception.message(exception)}"}
  catch
    kind, reason -> {:error, "Reranker #{kind}: #{inspect(reason)}"}
  end

  defp scores!({:ok, rows}), do: scores!(rows)
  defp scores!({:error, reason}), do: {:error, "Reranker failed: #{inspect(reason)}"}

  defp scores!(rows) when is_list(rows) do
    {:ok, Enum.map(rows, &score!/1)}
  end

  defp scores!(_), do: raise(ArgumentError, "reranker must return a list of scored row IDs")

  defp score!(row) when is_map(row) and not is_struct(row) do
    id = Map.get(row, "_rowid", Map.get(row, :_rowid))
    score = Map.get(row, "_relevance_score", Map.get(row, :_relevance_score))

    if !(is_integer(id) and id >= 0 and id <= 18_446_744_073_709_551_615 and
           is_number(score) and abs(score) <= 3.402_823_466_385_288_6e38),
       do:
         raise(
           ArgumentError,
           "reranker rows require a uint64 _rowid and finite float32 _relevance_score"
         )

    %{_rowid: id, _relevance_score: score / 1}
  end

  defp score!(_), do: raise(ArgumentError, "reranker rows must be maps")
end
