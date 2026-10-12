defmodule LanceDB.TableCache.Entries do
  @moduledoc false
  alias LanceDB.{Error, Table}
  alias LanceDB.TableCache.Warming

  # Entry policy has no process effects. The coordinator supplies time and
  # handles discarded entries' callers, monitors, and telemetry.
  @type entry :: %{
          generation: reference(),
          table: Table.t() | nil,
          opening: :queued | :running | nil,
          waiters: %{reference() => {GenServer.from(), reference()}},
          warming: %{String.t() => Warming.t()},
          touched: integer(),
          order: pos_integer()
        }
  @type t :: %{String.t() => entry()}
  @type discarded :: {String.t(), entry()} | nil

  @spec admit(t(), non_neg_integer(), pos_integer()) :: :ok | {:error, Error.t()}
  def admit(entries, extra, max_requests) do
    used =
      Enum.reduce(entries, 0, fn {_name, entry}, count ->
        count + map_size(entry.waiters) +
          Enum.count(entry.warming, fn {_name, warm} -> warm.status in [:queued, :running] end)
      end)

    if used + extra <= max_requests,
      do: :ok,
      else: {:error, busy("Table cache request capacity reached")}
  end

  @spec trim_warming(t(), pos_integer()) :: t()
  def trim_warming(entries, max_records) do
    retained =
      Enum.reduce(entries, 0, fn {_name, entry}, count -> count + map_size(entry.warming) end)

    excess = retained - max_records

    if excess > 0 do
      finished =
        for {name, entry} <- entries,
            {index, warm} <- entry.warming,
            warm.status in [:completed, :failed],
            do: {entry.order, name, index}

      finished
      |> Enum.sort()
      |> Enum.take(excess)
      |> Enum.reduce(entries, fn {_order, name, index}, entries ->
        update_in(entries, [name, :warming], &Map.delete(&1, index))
      end)
    else
      entries
    end
  end

  @spec reserve(t(), String.t(), pos_integer(), non_neg_integer(), integer()) ::
          {:ok, t(), pos_integer(), discarded()} | {:error, Error.t()}
  def reserve(entries, name, max_tables, tick, now) do
    if Map.has_key?(entries, name) do
      {entries, tick} = touch(entries, name, tick, now)
      {:ok, entries, tick, nil}
    else
      with {:ok, entries, discarded} <- make_room(entries, max_tables) do
        tick = tick + 1

        entry = %{
          generation: make_ref(),
          table: nil,
          opening: :queued,
          waiters: %{},
          warming: %{},
          touched: now,
          order: tick
        }

        {:ok, Map.put(entries, name, entry), tick, discarded}
      end
    end
  end

  defp make_room(entries, max_tables) do
    if map_size(entries) < max_tables do
      {:ok, entries, nil}
    else
      candidate =
        entries
        |> Enum.reject(fn {_name, entry} -> map_size(entry.waiters) > 0 end)
        |> Enum.min_by(fn {_name, entry} -> entry.order end, fn -> nil end)

      case candidate do
        nil ->
          {:error, busy("All table cache entries have pending fetch callers")}

        {name, _} = discarded ->
          {:ok, Map.delete(entries, name), discarded}
      end
    end
  end

  @spec touch(t(), String.t(), non_neg_integer(), integer()) :: {t(), pos_integer()}
  def touch(entries, name, tick, now) do
    tick = tick + 1
    entry = %{Map.fetch!(entries, name) | touched: now, order: tick}
    {Map.put(entries, name, entry), tick}
  end

  @spec expired(t(), pos_integer() | nil, integer()) :: [String.t()]
  def expired(_entries, nil, _now), do: []

  def expired(entries, idle_timeout_ms, now) do
    cutoff = now - idle_timeout_ms

    for {name, entry} <- entries,
        entry.touched <= cutoff and map_size(entry.waiters) == 0,
        do: name
  end

  defp busy(message), do: %Error{kind: :busy, message: message, details: %{phase: :waiting}}
end
