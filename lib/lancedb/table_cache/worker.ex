defmodule LanceDB.TableCache.Worker do
  @moduledoc false
  alias LanceDB.{Connection, Error, History, Index, Table}
  alias LanceDB.TableCache.{Options, Warming}

  @spec connect(Options.t()) :: LanceDB.result(Connection.t())
  def connect(options),
    do: Connection.connect(options.uri, options.storage_options, options.connect_options)

  @spec open(Connection.t(), String.t(), Options.t()) :: LanceDB.result(Table.t())
  def open(conn, name, options), do: Connection.open_table(conn, name, options.open_options)

  @spec warm(Table.t(), %{String.t() => Warming.t()}) ::
          LanceDB.result(%{String.t() => Warming.t()})
  def warm(table, requests) do
    with {:ok, version} <- History.version(table),
         {:ok, snapshot} <- History.snapshot(table, version),
         {:ok, indices} <- Index.list(snapshot) do
      indices = Map.new(indices, &{&1.name, &1})

      results =
        Map.new(requests, fn {name, previous} ->
          {name, warm_index(snapshot, name, previous, indices[name], version)}
        end)

      # A live handle may change while the pinned snapshot is warming. Never
      # label that work as completion for the replacement index/table version.
      case History.version(table) do
        {:ok, ^version} ->
          {:ok, results}

        {:ok, _} ->
          {:error,
           %Error{
             kind: :conflict,
             message: "Table changed while warming; request prewarming again"
           }}

        error ->
          error
      end
    end
  end

  defp warm_index(_table, name, _previous, nil, _version),
    do: %Warming{
      status: :failed,
      duration_ms: 0,
      error: %Error{
        kind: :not_found,
        operation: :cache_warm,
        message: "Index #{name} does not exist"
      }
    }

  defp warm_index(table, name, previous, index, version) do
    started = System.monotonic_time(:millisecond)

    result =
      if previous.version == version and previous.index_uuid != nil and
           previous.index_uuid == index.index_uuid,
         do: {:ok, :prewarmed},
         else: Index.prewarm(table, name)

    duration = System.monotonic_time(:millisecond) - started

    case result do
      {:ok, :prewarmed} ->
        %Warming{
          status: :completed,
          version: version,
          index_uuid: index.index_uuid,
          duration_ms: duration
        }

      {:error, error} ->
        %Warming{status: :failed, duration_ms: duration, error: error}
    end
  end
end
