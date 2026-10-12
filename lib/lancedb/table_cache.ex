defmodule LanceDB.TableCache do
  @moduledoc """
  Opt-in retention of table handles and supervised background index warming.

  Add `{LanceDB.TableCache, name: MyApp.Cache, uri: uri, ...}` to your supervision
  tree. Each instance owns one connection/session with immutable storage,
  consistency and cache settings. Storage runs in supervised tasks, never in
  the coordinator. Separate instances keep incompatible settings independent.

  Options:

    * `:uri` (required), `:storage_options` (default `%{}`), and optional `:name`.
      Unnamed instances receive distinct supervisor child IDs. Use
      `Supervisor.child_spec/2` to supply a stable application-specific ID.
    * `:index_cache_size_bytes`, `:metadata_cache_size_bytes`, and
      `:read_consistency_interval_ms` follow `LanceDB.Connection.connect/3`.
    * `:connect_timeout_ms` and `:open_timeout_ms` default to 5_000; nil disables
      that native deadline. There is no native prewarming deadline.
    * `:max_tables` (default 64) bounds entries, including pending opens.
      Least recently fetched/prewarmed entries are evicted first. Pending fetch
      callers protect their entries from eviction until they finish or time out.
    * `:idle_timeout_ms` (default 3_600_000, nil disables expiry) measures time
      since fetch/prewarm requests. Queries on returned handles are not observed.
    * `:max_opens` (default 4), `:max_warmers` (default 2) bound concurrent work.
      One warmer processes a table's requested indexes sequentially.
    * `:max_requests` (default 256) bounds waiting fetch callers plus remembered
      index warming records across the instance, including completed/failed ones.
      Invalidation and eviction discard these records. Saturation returns `:busy`.

  Limits and idle intervals are positive uint32 integers. Native memory is
  controlled separately by session cache budgets. Eviction releases this
  component's handle; it cannot revoke caller-held handles or flush a shared
  session. Warming records describe completed work, not guaranteed residency.

  The application owns table names, schemas, credentials, index selection and
  when to warm. Direct connection/table APIs remain available. Each node owns
  its own cache. Cached handles are live main-branch tables; use the direct API
  for custom locations, branches and historical checkouts.
  """
  use GenServer
  alias LanceDB.{Error, Execution, Options}
  alias LanceDB.TableCache.{Entries, Warming, Worker}
  alias LanceDB.TableCache.Options, as: CacheOptions
  @typep state :: map()

  @doc "Starts a cache and its linked task supervisor without opening storage until needed."
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) do
    options = CacheOptions.new(opts)
    server_opts = if is_nil(opts[:name]), do: [], else: [name: opts[:name]]
    GenServer.start_link(__MODULE__, options, server_opts)
  end

  @doc false
  @spec child_spec(keyword()) :: Supervisor.child_spec()
  def child_spec(opts) do
    id =
      case Keyword.get(opts, :name) do
        nil -> make_ref()
        name -> name
      end

    %{id: id, start: {__MODULE__, :start_link, [opts]}}
  end

  @doc """
  Returns the retained handle, sharing concurrent opens for the same name.
  `:timeout_ms` defaults to 15_000; nil waits indefinitely. It covers queueing,
  connection and open waits for this caller. Timeout/caller death removes the
  waiter but does not cancel an open shared with the cache or other callers.
  Failed opens are retryable. Native admission saturation returns `:busy`.
  """
  @spec fetch(GenServer.server(), String.t(), keyword()) :: LanceDB.result(LanceDB.Table.t())
  def fetch(server, name, opts \\ []) do
    name = Options.string!(name, "table name")
    opts = Keyword.validate!(opts, [:timeout_ms])
    timeout = Options.timeout_ms!(Keyword.get(opts, :timeout_ms, 15_000)) || :infinity
    request = make_ref()

    Execution.run(:cache_fetch, fn ->
      try do
        GenServer.call(server, {:fetch, name, request}, timeout)
      catch
        :exit, {:timeout, _} ->
          GenServer.cast(server, {:forget, request})
          {:error, failure(:timeout, "Table cache fetch timed out")}
      end
    end)
  end

  @doc """
  Schedules opening and warming, returning `{:ok, :scheduled}` promptly.
  Requires `indexes: [name, ...]` with distinct string/atom names. Concurrent
  requests share work. Completed work is skipped only after rechecking table
  version and index UUID. Warming uses a snapshot sharing the fetched session;
  a table change detected during warming fails the attempt for retry.

  Failures/crashes, including native `:busy`, are visible through `status/2`
  and can be retried with another request. Invalidation/eviction discard late
  results. Running native prewarming cannot be forcibly interrupted.
  """
  @spec prewarm(GenServer.server(), String.t(), keyword()) :: LanceDB.result(:scheduled)
  def prewarm(server, name, opts) do
    name = Options.string!(name, "table name")
    opts = Keyword.validate!(opts, [:indexes])
    indexes = Options.column_names!(opts[:indexes])
    Execution.run(:cache_prewarm, fn -> GenServer.call(server, {:prewarm, name, indexes}) end)
  end

  @doc "Returns string-keyed index names mapped to Warming structs; unretained tables return :not_found."
  @spec status(GenServer.server(), String.t()) :: LanceDB.result(%{String.t() => Warming.t()})
  def status(server, name) do
    name = Options.string!(name, "table name")
    Execution.run(:cache_status, fn -> GenServer.call(server, {:status, name}) end)
  end

  @doc """
  Discards an entry and its warming records, returning :ok even if absent.
  Pending fetches receive :cancelled. Later results cannot recreate the entry.
  Existing caller-held handles remain usable; explicitly invalidate after
  dropping/recreating a table. Running work stays counted until it settles.
  """
  @spec invalidate(GenServer.server(), String.t()) :: :ok
  def invalidate(server, name),
    do: GenServer.call(server, {:invalidate, Options.string!(name, "table name")})

  @impl true
  @spec init(CacheOptions.t()) :: {:ok, state()}
  def init(options) do
    {:ok, supervisor} = Task.Supervisor.start_link()

    state = %{
      options: options,
      supervisor: supervisor,
      conn: nil,
      connecting: false,
      entries: %{},
      tasks: %{},
      tick: 0,
      cache_id: make_ref()
    }

    schedule_expiry(state)
    {:ok, state}
  end

  @impl true
  @spec handle_call(term(), GenServer.from(), state()) ::
          {:reply, term(), state()} | {:noreply, state()}
  def handle_call({:fetch, name, request}, from, state) do
    state = expire(state)

    case state.entries[name] do
      %{table: %LanceDB.Table{} = table} -> {:reply, {:ok, table}, touch(state, name)}
      _ -> fetch_pending(state, name, request, from)
    end
  end

  def handle_call({:prewarm, name, names}, _from, state) do
    state = expire(state)
    previous = get_in(state, [:entries, name, :warming]) || %{}
    extra = Enum.count(names, &(not Map.has_key?(previous, &1)))

    with :ok <- Entries.admit(state.entries, extra, state.options.max_requests),
         {:ok, state} <- reserve(state, name) do
      entry = state.entries[name]

      warming = Enum.reduce(names, entry.warming, &enqueue_index/2)

      entry = %{
        entry
        | warming: warming,
          opening: if(entry.table == nil, do: entry.opening || :queued, else: nil)
      }

      {:reply, {:ok, :scheduled}, state |> put_entry(name, entry) |> dispatch()}
    else
      {:error, error} -> {:reply, {:error, error}, state}
    end
  end

  def handle_call({:status, name}, _from, state) do
    state = expire(state)

    result =
      case state.entries[name] do
        nil -> {:error, failure(:not_found, "Table is not retained by this cache")}
        entry -> {:ok, entry.warming}
      end

    {:reply, result, state}
  end

  def handle_call({:invalidate, name}, _from, state),
    do: {:reply, :ok, discard(state, name, :invalidated)}

  @impl true
  @spec handle_cast(term(), state()) :: {:noreply, state()}
  def handle_cast({:forget, request}, state),
    do: {:noreply, forget(state, fn id, _monitor -> id == request end)}

  @impl true
  @spec handle_info(term(), state()) :: {:noreply, state()}
  def handle_info({reference, result}, state) when is_reference(reference) do
    case Map.pop(state.tasks, reference) do
      {nil, _} ->
        {:noreply, state}

      {job, tasks} ->
        Process.demonitor(reference, [:flush])
        state = finish(%{state | tasks: tasks}, job, result)
        {:noreply, dispatch(state)}
    end
  end

  def handle_info({:DOWN, reference, :process, _pid, reason}, state) do
    case Map.pop(state.tasks, reference) do
      {nil, _} ->
        {:noreply, forget(state, fn _request, monitor -> monitor == reference end)}

      {job, tasks} ->
        error = failure(:execution, "Table cache worker exited: #{inspect(reason)}")
        state = finish(%{state | tasks: tasks}, job, {:error, error})
        {:noreply, dispatch(state)}
    end
  end

  def handle_info(:expire, state) do
    state = expire(state)
    schedule_expiry(state)
    {:noreply, dispatch(state)}
  end

  @impl true
  @spec code_change(term(), state(), term()) :: {:ok, state()}
  def code_change(_old_version, state, _extra), do: {:ok, state}

  @impl true
  @spec terminate(term(), state()) :: :ok
  def terminate(_reason, state) do
    Process.exit(state.supervisor, :shutdown)
    :ok
  end

  defp fetch_pending(state, name, request, from) do
    with :ok <- Entries.admit(state.entries, 1, state.options.max_requests),
         {:ok, updated} <- reserve(state, name) do
      entry = updated.entries[name]
      monitor = Process.monitor(elem(from, 0))

      entry = %{
        entry
        | opening: entry.opening || :queued,
          waiters: Map.put(entry.waiters, request, {from, monitor})
      }

      {:noreply, updated |> put_entry(name, entry) |> dispatch()}
    else
      {:error, error} -> {:reply, {:error, error}, state}
    end
  end

  defp enqueue_index(name, warming) do
    case warming[name] do
      %Warming{status: status} when status in [:queued, :running] ->
        warming

      previous ->
        Map.put(warming, name, %{
          (previous || %Warming{})
          | status: :queued,
            duration_ms: nil,
            error: nil
        })
    end
  end

  defp reserve(state, name) do
    with {:ok, entries, tick, discarded} <-
           Entries.reserve(state.entries, name, state.options.max_tables, state.tick, now()) do
      discard_entry(state, discarded, :capacity)
      {:ok, %{state | entries: entries, tick: tick}}
    end
  end

  defp touch(state, name) do
    {entries, tick} = Entries.touch(state.entries, name, state.tick, now())
    %{state | entries: entries, tick: tick}
  end

  defp put_entry(state, name, entry), do: %{state | entries: Map.put(state.entries, name, entry)}

  defp discard(state, name, reason) do
    {entry, entries} = Map.pop(state.entries, name)
    discard_entry(state, if(entry, do: {name, entry}), reason)
    %{state | entries: entries}
  end

  defp discard_entry(_state, nil, _reason), do: :ok

  defp discard_entry(state, {name, entry}, reason) do
    reply_waiters(entry, {:error, failure(:cancelled, "Table cache entry was discarded")})

    :telemetry.execute([:lancedb, :table_cache, :evict], %{count: 1}, %{
      cache_id: state.cache_id,
      table_name: name,
      reason: reason
    })
  end

  defp forget(state, predicate) do
    entries =
      Map.new(state.entries, fn {name, entry} ->
        waiters = Map.reject(entry.waiters, &forget_waiter(&1, predicate))

        {name, %{entry | waiters: waiters}}
      end)

    %{state | entries: entries}
  end

  defp forget_waiter({request, {_from, monitor}}, predicate) do
    if predicate.(request, monitor) do
      Process.demonitor(monitor, [:flush])
      true
    else
      false
    end
  end

  defp dispatch(state) do
    pending_open = Enum.any?(state.entries, fn {_name, entry} -> entry.opening == :queued end)

    cond do
      state.conn == nil and pending_open and not state.connecting ->
        options = state.options
        state = start_job(state, :connect, nil, nil, [], fn -> Worker.connect(options) end)
        %{state | connecting: true}

      state.conn != nil ->
        state |> start_opens() |> start_warmers()

      true ->
        state
    end
  end

  defp start_opens(state) do
    entry = Enum.find(state.entries, fn {_name, entry} -> entry.opening == :queued end)

    if entry != nil and active(state, :open) < state.options.max_opens do
      {name, entry} = entry
      state = put_entry(state, name, %{entry | opening: :running})
      conn = state.conn
      options = state.options

      state =
        start_job(state, :open, name, entry.generation, [], fn ->
          Worker.open(conn, name, options)
        end)

      start_opens(state)
    else
      state
    end
  end

  defp start_warmers(state) do
    entry =
      Enum.find(state.entries, fn {name, entry} ->
        entry.table != nil and
          Enum.any?(entry.warming, fn {_name, warm} -> warm.status == :queued end) and
          not Enum.any?(state.tasks, fn {_ref, job} ->
            job.kind == :warm and job.name == name and job.generation == entry.generation
          end)
      end)

    if entry != nil and active(state, :warm) < state.options.max_warmers do
      {name, entry} = entry
      requests = Map.filter(entry.warming, fn {_name, warm} -> warm.status == :queued end)

      warming =
        Enum.reduce(requests, entry.warming, fn {name, warm}, all ->
          Map.put(all, name, %{warm | status: :running})
        end)

      state = put_entry(state, name, %{entry | warming: warming})
      table = entry.table

      state =
        start_job(state, :warm, name, entry.generation, Map.keys(requests), fn ->
          Worker.warm(table, requests)
        end)

      start_warmers(state)
    else
      state
    end
  end

  defp active(state, kind), do: Enum.count(state.tasks, fn {_ref, job} -> job.kind == kind end)

  defp start_job(state, kind, name, generation, indexes, fun) do
    metadata = %{
      cache_id: state.cache_id,
      table_name: name,
      generation: generation,
      index_names: indexes
    }

    operation =
      case kind do
        :connect -> :cache_connect
        :open -> :cache_open
        :warm -> :cache_warm
      end

    task =
      Task.Supervisor.async_nolink(
        state.supervisor,
        fn -> Execution.run(operation, fun, metadata) end
      )

    job = %{kind: kind, name: name, generation: generation, indexes: indexes, started: now()}
    %{state | tasks: Map.put(state.tasks, task.ref, job)}
  end

  defp finish(state, %{kind: :connect}, {:ok, conn}),
    do: %{state | conn: conn, connecting: false}

  defp finish(state, %{kind: :connect}, {:error, error}) do
    state = %{state | connecting: false}

    Enum.reduce(state.entries, state, fn {name, entry}, state ->
      if entry.table == nil, do: open_failed(state, name, entry, error), else: state
    end)
  end

  defp finish(state, job, result) do
    case state.entries[job.name] do
      %{generation: generation} = entry when generation == job.generation ->
        finish_entry(state, job, entry, result)

      _ ->
        state
    end
  end

  defp finish_entry(state, %{kind: :open, name: name}, entry, {:ok, table}) do
    reply_waiters(entry, {:ok, table})
    put_entry(state, name, %{entry | table: table, opening: nil, waiters: %{}})
  end

  defp finish_entry(state, %{kind: :open, name: name}, entry, {:error, error}),
    do: open_failed(state, name, entry, error)

  defp finish_entry(state, %{kind: :warm} = job, entry, result) do
    results =
      case result do
        {:ok, results} ->
          results

        {:error, error} ->
          Map.new(
            job.indexes,
            &{&1, %Warming{status: :failed, error: error, duration_ms: now() - job.started}}
          )
      end

    Enum.each(results, fn {name, warm} ->
      :telemetry.execute([:lancedb, :table_cache, :warm], %{duration_ms: warm.duration_ms}, %{
        cache_id: state.cache_id,
        table_name: job.name,
        index_name: name,
        status: warm.status,
        version: warm.version,
        index_uuid: warm.index_uuid,
        error_kind: if(warm.error, do: warm.error.kind)
      })
    end)

    put_entry(state, job.name, %{entry | warming: Map.merge(entry.warming, results)})
  end

  defp open_failed(state, name, entry, error) do
    reply_waiters(entry, {:error, error})

    if map_size(entry.warming) == 0 do
      %{state | entries: Map.delete(state.entries, name)}
    else
      warming =
        Map.new(entry.warming, fn {name, _} ->
          {name, %Warming{status: :failed, error: error}}
        end)

      put_entry(state, name, %{entry | opening: nil, waiters: %{}, warming: warming})
    end
  end

  defp reply_waiters(entry, result) do
    Enum.each(entry.waiters, fn {_request, {from, monitor}} ->
      Process.demonitor(monitor, [:flush])
      GenServer.reply(from, result)
    end)
  end

  defp expire(state) do
    state.entries
    |> Entries.expired(state.options.idle_timeout_ms, now())
    |> Enum.reduce(state, fn name, state -> discard(state, name, :idle) end)
  end

  defp schedule_expiry(state) do
    if state.options.idle_timeout_ms,
      do: Process.send_after(self(), :expire, min(state.options.idle_timeout_ms, 60_000))
  end

  defp failure(kind, message),
    do: %Error{
      kind: kind,
      message: message,
      details: %{phase: if(kind == :execution, do: :execution, else: :waiting)}
    }

  defp now, do: System.monotonic_time(:millisecond)
end
