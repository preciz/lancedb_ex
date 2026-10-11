defmodule LanceDB.Telemetry do
  @moduledoc """
  Operation timing events and opt-in native storage metrics.

  Public database operations, direct query execution and individual query
  stream open/next/close calls emit `[:lancedb, :operation, :start | :stop |
  :exception]`. Metadata includes `:operation`, `:operation_id`, and `:execution_id`. A query enumeration shares its execution ID across open/next/close events; eager queries also provide `:parent_operation_id` on child events. Independent callback operations receive independent IDs; stop metadata includes `:status`
  (`:ok` or `:error`) and `:error_kind`. Direct query execution also identifies
  `:query_kind`. Public calls made inside callbacks emit their own spans. Internal adapter calls do not emit spans. Durations use Erlang native time units. Records, SQL,
  connection credentials, and table handles are not included. Exception events
  use the standard telemetry exception metadata, which can contain user data in
  the exception reason or stacktrace.

  Call `enable_native_metrics/0` before opening connections to collect native
  metrics. The recorder is process-global, cannot be disabled, and reports
  cumulative values across all connections in this NIF. It does not reset when
  read. An incompatible existing native recorder produces an error.

  For session-local index and metadata cache statistics, use
  `LanceDB.Table.cache_stats/1`; that API needs no global recorder and reports
  the table's actual session, including any other handles sharing its caches.

  `poll/0` emits one `[:lancedb, :native, :metric]` event per metric series. Scalar
  measurements contain `:value`; histogram measurements contain `:count` and
  `:sum`. Metadata contains `:name`, `:kind`, `:attributes`, and histogram
  `:buckets` (cumulative pairs of upper-bound strings and counts).

  These are cumulative snapshots, not individual observations: do not sum
  successive counter snapshots or feed histogram sums into a distribution.
  Metric names and attributes stay strings and depend on the upstream SDK.
  Native attributes may include storage paths; filter them before export when
  necessary. Call `poll/0` from your own scheduler or telemetry_poller; this
  package starts no background polling process.
  """
  alias LanceDB.Backend

  @doc "Installs the native recorder, returning `:ok` even if already installed."
  @spec enable_native_metrics() :: :ok | {:error, LanceDB.Error.t()}
  def enable_native_metrics do
    case Backend.metrics_enable() do
      {:ok, true} -> :ok
      error -> error
    end
  end

  @doc "Returns metric descriptions with string keys; empty until metrics are described."
  @spec catalog() :: [map()]
  def catalog, do: Backend.metrics_catalog() |> JSON.decode!()

  @doc "Returns cumulative metric series with string keys, without emitting events."
  @spec snapshot() :: [map()]
  def snapshot, do: Backend.metrics_snapshot() |> JSON.decode!()

  @doc "Emits native metric snapshots as telemetry events."
  @spec poll() :: :ok
  def poll do
    Enum.each(snapshot(), &emit_metric/1)
  end

  defp emit_metric(metric) do
    metadata = %{name: metric["name"], kind: metric["kind"], attributes: metric["attributes"]}
    values = metric["measurements"]

    {measurements, metadata} =
      case values do
        %{"value" => value} ->
          {%{value: value}, metadata}

        %{"count" => count, "sum" => sum, "buckets" => buckets} ->
          {%{count: count, sum: sum}, Map.put(metadata, :buckets, buckets)}
      end

    :telemetry.execute([:lancedb, :native, :metric], measurements, metadata)
  end

  @doc false
  def span(operation, fun, metadata \\ %{}) do
    metadata =
      metadata
      |> Map.put(:operation, operation)
      |> Map.put_new(:operation_id, make_ref())
      |> Map.put_new(:execution_id, make_ref())

    :telemetry.span([:lancedb, :operation], metadata, fn ->
      result = fun.()
      status = if match?({:error, _}, result), do: :error, else: :ok

      error_kind =
        case result do
          {:error, %LanceDB.Error{kind: kind}} -> kind
          _ -> nil
        end

      {result, Map.merge(metadata, %{status: status, error_kind: error_kind})}
    end)
  end
end
