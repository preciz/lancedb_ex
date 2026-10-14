defmodule LanceDB.TableCacheTest do
  use LanceDB.DataCase, async: false

  alias LanceDB.{
    Connection,
    Error,
    Execution,
    Index,
    Maintenance,
    Query,
    Table,
    TableCache
  }

  alias LanceDB.TableCache.Warming

  setup %{path: path} do
    {:ok, conn} = Connection.connect(path, %{}, read_consistency_interval_ms: 0)
    rows = for id <- 1..100, do: %{id: id, text: "document #{id}", title: "title #{id}"}
    {:ok, table} = Connection.create_table(conn, "docs", rows)
    {:ok, :created} = Index.create(table, :text, type: :fts, name: "words")
    {:ok, :created} = Index.create(table, :title, type: :fts, name: "titles")
    for name <- ["other", "third"], do: Connection.create_table(conn, name, rows)
    %{conn: conn, table: table}
  end

  test "connection access is lazy, works on empty storage and creates no table entry", %{
    path: path
  } do
    cache = cache(Path.join(path, "empty"), max_tables: 1, max_requests: 1)
    assert :sys.get_state(cache).conn == nil
    assert :sys.get_state(cache).tasks == %{}
    assert {:ok, conn} = TableCache.connection(cache)
    assert {:ok, ^conn} = TableCache.connection(cache, timeout_ms: nil)
    assert {:ok, []} = Connection.table_names(conn)
    assert :sys.get_state(cache).entries == %{}
    assert :sys.get_state(cache).connection_waiters == %{}

    assert {:ok, _} = Connection.create_table(conn, "created", [%{id: 1}])
    assert :sys.get_state(cache).entries == %{}
    assert {:ok, table} = TableCache.fetch(cache, "created")
    assert {:ok, [%{"id" => 1}]} = Query.query(table)
  end

  test "connection, fetch and warming callers share a single supervised attempt", %{path: path} do
    cache = cache(path, max_requests: 5)
    handler = block(cache, :cache_connect, nil)
    connections = for _ <- 1..3, do: Task.async(fn -> TableCache.connection(cache) end)
    assert_receive {:blocked, worker}, 2_000
    assert_eventually(fn -> map_size(:sys.get_state(cache).connection_waiters) == 3 end)
    fetched = Task.async(fn -> TableCache.fetch(cache, "docs") end)

    assert_eventually(fn ->
      map_size(get_in(:sys.get_state(cache), [:entries, "docs", :waiters]) || %{}) == 1
    end)

    assert {:ok, :scheduled} = TableCache.prewarm(cache, "docs", indexes: [:words])
    before = :sys.get_state(cache)
    assert {:error, %Error{kind: :busy}} = TableCache.connection(cache)
    assert {:error, %Error{kind: :busy}} = TableCache.fetch(cache, "other")
    assert {:error, %Error{kind: :busy}} = TableCache.prewarm(cache, "docs", indexes: [:titles])
    assert :sys.get_state(cache).entries == before.entries
    assert map_size(:sys.get_state(cache).tasks) == 1
    refute_receive {:blocked, _}, 50
    :telemetry.detach(handler)
    send(worker, :continue)

    [conn] =
      Enum.uniq(
        Enum.map(connections, fn task ->
          assert {:ok, conn} = Task.await(task)
          conn
        end)
      )

    assert {:ok, ^conn} = TableCache.connection(cache)
    assert {:ok, _} = Task.await(fetched)
    await_warmed(cache, "docs", ["words"])
    assert :sys.get_state(cache).connection_waiters == %{}
  end

  test "connection timeout and caller death release capacity without cancelling shared work", %{
    path: path
  } do
    cache = cache(path, max_requests: 2)
    handler = block(cache, :cache_connect, nil)
    timed = Task.async(fn -> TableCache.connection(cache, timeout_ms: 100) end)
    assert_receive {:blocked, worker}, 2_000
    survivor = Task.async(fn -> TableCache.connection(cache, timeout_ms: nil) end)
    assert_eventually(fn -> map_size(:sys.get_state(cache).connection_waiters) == 2 end)
    assert {:error, %Error{kind: :busy}} = TableCache.connection(cache)
    assert {:error, %Error{kind: :timeout, operation: :cache_connection}} = Task.await(timed)
    assert_eventually(fn -> map_size(:sys.get_state(cache).connection_waiters) == 1 end)
    dead = spawn(fn -> TableCache.connection(cache, timeout_ms: nil) end)
    assert_eventually(fn -> map_size(:sys.get_state(cache).connection_waiters) == 2 end)
    Process.exit(dead, :kill)
    assert_eventually(fn -> map_size(:sys.get_state(cache).connection_waiters) == 1 end)
    assert {:ok, :scheduled} = TableCache.prewarm(cache, "docs", indexes: [:words])
    assert {:error, %Error{kind: :busy}} = TableCache.fetch(cache, "docs")
    assert Process.alive?(worker)
    :telemetry.detach(handler)
    send(worker, :continue)
    assert {:ok, conn} = Task.await(survivor)
    assert {:ok, ^conn} = TableCache.connection(cache)
    await_warmed(cache, "docs", ["words"])
    assert :sys.get_state(cache).connection_waiters == %{}
  end

  test "an abandoned connection attempt settles once and retains its result", %{path: path} do
    cache = cache(path, max_requests: 1)
    handler = block(cache, :cache_connect, nil)
    timed = Task.async(fn -> TableCache.connection(cache, timeout_ms: 50) end)
    assert_receive {:blocked, worker}, 2_000
    assert {:error, %Error{kind: :timeout}} = Task.await(timed)
    assert_eventually(fn -> :sys.get_state(cache).connection_waiters == %{} end)
    assert :sys.get_state(cache).entries == %{}
    :telemetry.detach(handler)
    send(worker, :continue)
    assert_eventually(fn -> :sys.get_state(cache).conn != nil end)
    conn = :sys.get_state(cache).conn
    assert {:ok, ^conn} = TableCache.connection(cache)
    assert :sys.get_state(cache).tasks == %{}
  end

  test "connection worker crashes fail all affected requests and allow retry", %{path: path} do
    cache = cache(path)
    handler = block(cache, :cache_connect, nil)
    connections = for _ <- 1..3, do: Task.async(fn -> TableCache.connection(cache) end)
    assert_receive {:blocked, worker}, 2_000
    fetched = Task.async(fn -> TableCache.fetch(cache, "docs") end)
    assert_eventually(fn -> map_size(:sys.get_state(cache).connection_waiters) == 3 end)

    assert_eventually(fn ->
      map_size(get_in(:sys.get_state(cache), [:entries, "docs", :waiters]) || %{}) == 1
    end)

    assert {:ok, :scheduled} = TableCache.prewarm(cache, "docs", indexes: [:words])
    :telemetry.detach(handler)
    Process.exit(worker, :kill)

    for task <- [fetched | connections],
        do: assert({:error, %Error{kind: :execution}} = Task.await(task))

    assert {:ok, %{"words" => %Warming{status: :failed}}} = TableCache.status(cache, "docs")
    assert :sys.get_state(cache).connection_waiters == %{}
    assert {:ok, _} = TableCache.connection(cache)
    assert {:ok, _} = TableCache.fetch(cache, "docs")
    assert {:ok, :scheduled} = TableCache.prewarm(cache, "docs", indexes: [:words])
    await_warmed(cache, "docs", ["words"])
  end

  test "native connection failures reach all waiters and the next request retries", %{path: path} do
    uri = Path.join(path, "blocked_database")
    File.write!(uri, "a file cannot be a database directory")
    cache = cache(uri)
    handler = block(cache, :cache_connect, nil)
    callers = for _ <- 1..3, do: Task.async(fn -> TableCache.connection(cache) end)
    assert_receive {:blocked, worker}, 2_000
    assert_eventually(fn -> map_size(:sys.get_state(cache).connection_waiters) == 3 end)
    fetched = Task.async(fn -> TableCache.fetch(cache, "created") end)

    assert_eventually(fn ->
      map_size(get_in(:sys.get_state(cache), [:entries, "created", :waiters]) || %{}) == 1
    end)

    assert {:ok, :scheduled} = TableCache.prewarm(cache, "created", indexes: [:words])
    :telemetry.detach(handler)
    send(worker, :continue)
    results = Enum.map(callers, &Task.await/1)
    assert [{:error, %Error{kind: kind, operation: :cache_connection}}] = Enum.uniq(results)
    assert {:error, %Error{kind: ^kind, operation: :cache_fetch}} = Task.await(fetched)

    assert {:ok, %{"words" => %Warming{status: :failed, error: %Error{kind: ^kind}}}} =
             TableCache.status(cache, "created")

    assert :sys.get_state(cache).connection_waiters == %{}
    assert :sys.get_state(cache).conn == nil
    File.rm!(uri)
    assert {:ok, conn} = TableCache.connection(cache)
    assert {:ok, []} = Connection.table_names(conn)
    assert {:ok, _} = Connection.create_table(conn, "created", [%{id: 1, text: "created"}])
    assert {:ok, _} = TableCache.fetch(cache, "created")
  end

  test "direct and cached handles share configured caches without changing cache entries", %{
    path: path
  } do
    cache =
      cache(path,
        max_tables: 1,
        index_cache_size_bytes: 16 * 1024 * 1024,
        metadata_cache_size_bytes: 0
      )

    {:ok, cached} = TableCache.fetch(cache, "docs")
    assert {:ok, :scheduled} = TableCache.prewarm(cache, "docs", indexes: [:words])
    await_warmed(cache, "docs", ["words"])
    before = :sys.get_state(cache)
    {:ok, conn} = TableCache.connection(cache)
    assert :sys.get_state(cache) == before
    {:ok, direct} = Connection.open_table(conn, "docs")
    {:ok, created} = Connection.create_table(conn, "outside", [%{id: 1, text: "outside"}])
    {:ok, :created} = Index.create(created, :text, type: :fts, name: "outside_words")
    {:ok, _} = Query.full_text_search(created, "outside", columns: [:id])
    {:ok, stats} = Table.cache_stats(cached)
    assert stats["index"]["num_entries"] > 0
    assert stats["metadata"]["num_entries"] == 0
    assert {:ok, ^stats} = Table.cache_stats(direct)
    assert {:ok, ^stats} = Table.cache_stats(created)
    assert :sys.get_state(cache) == before
    assert {:ok, ^cached} = TableCache.fetch(cache, "docs")
  end

  test "held connection survives restart and direct replacement requires explicit invalidation",
       %{
         path: path
       } do
    id = make_ref()
    opts = [uri: path, read_consistency_interval_ms: 0]
    cache = start_supervised!(Supervisor.child_spec({TableCache, opts}, id: id))
    {:ok, conn} = TableCache.connection(cache)
    {:ok, table} = TableCache.fetch(cache, "docs")
    assert :ok = stop_supervised(id)
    assert {:ok, _} = Connection.table_names(conn)
    assert {:ok, 100} = Table.count_rows(table)
    replacement = start_supervised!(Supervisor.child_spec({TableCache, opts}, id: id))
    {:ok, fresh} = TableCache.connection(replacement)
    assert fresh != conn
    {:ok, cached} = TableCache.fetch(replacement, "docs")
    assert {:ok, :dropped} = Connection.drop_table(fresh, "docs")
    assert {:ok, _} = Connection.create_table(fresh, "docs", [%{id: 999}])
    assert :sys.get_state(replacement).entries["docs"].table == cached
    assert :ok = TableCache.invalidate(replacement, "docs")
    assert {:ok, reopened} = TableCache.fetch(replacement, "docs")
    assert reopened != cached
    assert {:ok, [%{"id" => 999}]} = Query.query(reopened)
  end

  test "connection options are validated without contacting the server" do
    for opts <- [
          [timeout_ms: 0],
          [timeout_ms: -1],
          [timeout_ms: "5"],
          [timeout_ms: 4_294_967_296],
          [index_cache_size_bytes: 0],
          [storage_options: %{}]
        ] do
      assert_raise ArgumentError, fn -> TableCache.connection(:missing, opts) end
    end
  end

  test "shares concurrent opens and a slow table does not block another table", %{path: path} do
    cache = cache(path)
    handler = block(cache, :cache_open, "docs")
    callers = for _ <- 1..20, do: Task.async(fn -> TableCache.fetch(cache, "docs") end)
    assert_receive {:blocked, worker}, 2_000
    assert_eventually(fn -> map_size(:sys.get_state(cache).entries["docs"].waiters) == 20 end)
    assert {:ok, other} = TableCache.fetch(cache, "other")
    assert {:ok, 100} = Table.count_rows(other)
    refute_receive {:blocked, _}, 50
    :telemetry.detach(handler)
    send(worker, :continue)

    handles =
      Enum.map(callers, fn caller ->
        {:ok, table} = Task.await(caller)
        table
      end)

    assert Enum.uniq(handles) == [hd(handles)]
    assert {:ok, table} = TableCache.fetch(cache, "docs")
    assert table == hd(handles)
  end

  test "LRU and idle expiry release entries without revoking caller handles", %{path: path} do
    cache = cache(path, max_tables: 2)
    {:ok, old} = TableCache.fetch(cache, "docs")
    {:ok, _} = TableCache.fetch(cache, "other")
    {:ok, ^old} = TableCache.fetch(cache, "docs")
    {:ok, _} = TableCache.fetch(cache, "third")
    assert {:error, %Error{kind: :not_found}} = TableCache.status(cache, "other")
    assert {:ok, ^old} = TableCache.fetch(cache, "docs")
    assert {:ok, 100} = Table.count_rows(old)

    idle = cache(path, idle_timeout_ms: 30)
    {:ok, first} = TableCache.fetch(idle, "docs")

    assert_eventually(fn ->
      match?({:error, %Error{kind: :not_found}}, TableCache.status(idle, "docs"))
    end)

    assert {:ok, second} = TableCache.fetch(idle, "docs")
    assert second != first
    assert {:ok, 100} = Table.count_rows(first)
  end

  test "open concurrency remains bounded when an in-flight entry is invalidated", %{path: path} do
    cache = cache(path, max_tables: 1, max_opens: 1)
    handler = block(cache, :cache_open, :any)
    first = Task.async(fn -> TableCache.fetch(cache, "docs") end)
    assert_receive {:blocked, worker}, 2_000
    assert {:error, %Error{kind: :busy}} = TableCache.fetch(cache, "other")
    :ok = TableCache.invalidate(cache, "docs")
    assert {:error, %Error{kind: :cancelled}} = Task.await(first)
    second = Task.async(fn -> TableCache.fetch(cache, "other") end)

    assert_eventually(fn ->
      match?(%{opening: :queued}, :sys.get_state(cache).entries["other"])
    end)

    refute_receive {:blocked, _}, 50
    send(worker, :continue)
    assert_receive {:blocked, second_worker}, 2_000
    :telemetry.detach(handler)
    send(second_worker, :continue)
    assert {:ok, _} = Task.await(second)
    assert {:error, %Error{kind: :not_found}} = TableCache.status(cache, "docs")
  end

  test "failed opens and crashed connection/open tasks can be retried", %{path: path, conn: conn} do
    cache = cache(path)
    handler = block(cache, :cache_connect, nil)
    caller = Task.async(fn -> TableCache.fetch(cache, "docs") end)
    assert_receive {:blocked, worker}, 2_000
    :telemetry.detach(handler)
    Process.exit(worker, :kill)
    assert {:error, %Error{kind: :execution}} = Task.await(caller)
    assert Process.alive?(cache)

    assert {:error, %Error{kind: :not_found}} = TableCache.fetch(cache, "missing")
    {:ok, _} = Connection.create_table(conn, "missing", [%{id: 1}])
    assert {:ok, _} = TableCache.fetch(cache, "missing")

    handler = block(cache, :cache_open, "docs")
    caller = Task.async(fn -> TableCache.fetch(cache, "docs") end)
    assert_receive {:blocked, worker}, 2_000
    :telemetry.detach(handler)
    Process.exit(worker, :kill)
    assert {:error, %Error{kind: :execution}} = Task.await(caller)
    assert {:ok, _} = TableCache.fetch(cache, "docs")
  end

  test "invalidation cannot be undone by a late open result", %{path: path} do
    cache = cache(path)
    handler = block(cache, :cache_open, "docs")
    old_caller = Task.async(fn -> TableCache.fetch(cache, "docs") end)
    assert_receive {:blocked, old_worker}, 2_000
    assert :ok = TableCache.invalidate(cache, "docs")
    assert {:error, %Error{kind: :cancelled}} = Task.await(old_caller)
    new_caller = Task.async(fn -> TableCache.fetch(cache, "docs") end)
    assert_receive {:blocked, new_worker}, 2_000
    send(old_worker, :continue)
    assert_eventually(fn -> map_size(:sys.get_state(cache).tasks) == 1 end)
    assert Task.yield(new_caller, 50) == nil
    assert :sys.get_state(cache).entries["docs"].table == nil
    :telemetry.detach(handler)
    send(new_worker, :continue)
    assert {:ok, _} = Task.await(new_caller)
  end

  test "timeouts and caller death remove waiters and request saturation stays bounded", %{
    path: path
  } do
    cache = cache(path, max_requests: 1, max_tables: 1, max_opens: 1)
    handler = block(cache, :cache_open, "docs")
    caller = Task.async(fn -> TableCache.fetch(cache, "docs", timeout_ms: 30) end)
    assert_receive {:blocked, worker}, 2_000
    assert {:error, %Error{kind: :busy}} = TableCache.fetch(cache, "other")
    assert {:error, %Error{kind: :timeout}} = Task.await(caller)
    assert_eventually(fn -> map_size(:sys.get_state(cache).entries["docs"].waiters) == 0 end)
    caller = spawn(fn -> TableCache.fetch(cache, "docs", timeout_ms: nil) end)
    assert_eventually(fn -> map_size(:sys.get_state(cache).entries["docs"].waiters) == 1 end)
    Process.exit(caller, :kill)
    assert_eventually(fn -> map_size(:sys.get_state(cache).entries["docs"].waiters) == 0 end)
    :telemetry.detach(handler)
    send(worker, :continue)
    assert {:ok, _} = TableCache.fetch(cache, "docs")
  end

  test "warming shares work, keeps lookup/query available, and reuses the fetched session", %{
    path: path
  } do
    cache = cache(path, index_cache_size_bytes: 16 * 1024 * 1024)
    handler = block(cache, :cache_warm, "docs")
    assert {:ok, :scheduled} = TableCache.prewarm(cache, "docs", indexes: [:words, "titles"])
    assert_receive {:blocked, worker}, 2_000

    for _ <- 1..20 do
      assert {:ok, :scheduled} = TableCache.prewarm(cache, "docs", indexes: [:words, :titles])
    end

    assert {:ok, table} = TableCache.fetch(cache, "docs")
    assert {:ok, [%{"id" => 1}]} = Query.query(table, columns: [:id], limit: 1)

    assert {:ok, %{"words" => %Warming{status: :running}, "titles" => %Warming{status: :running}}} =
             TableCache.status(cache, "docs")

    refute_receive {:blocked, _}, 50
    :telemetry.detach(handler)
    send(worker, :continue)
    await_warmed(cache, "docs", ["words", "titles"])
    assert {:ok, ^table} = TableCache.fetch(cache, "docs")
    assert {:ok, stats} = Table.cache_stats(table)
    assert stats["index"]["num_entries"] > 0
    assert {:ok, rows} = Query.full_text_search(table, "document", text_column: :text)
    assert rows != []
    {:ok, after_query} = Table.cache_stats(table)
    assert after_query["index"]["hits"] > stats["index"]["hits"]
  end

  test "completed warming is skipped until replacement or extension changes its identity", %{
    path: path
  } do
    cache = cache(path)
    watch(cache)
    {:ok, table} = TableCache.fetch(cache, "docs")
    {:ok, :scheduled} = TableCache.prewarm(cache, "docs", indexes: [:words])
    await_warmed(cache, "docs", ["words"])
    assert_receive {:observed, [:lancedb, :operation, :start], _, %{operation: :prewarm_index}}
    {:ok, %{"words" => before}} = TableCache.status(cache, "docs")

    {:ok, :scheduled} = TableCache.prewarm(cache, "docs", indexes: [:words])
    await_warmed(cache, "docs", ["words"])

    refute_receive {:observed, [:lancedb, :operation, :start], _, %{operation: :prewarm_index}},
                   50

    {:ok, _} = Table.add(table, [%{id: 101, text: "new document", title: "new title"}])
    {:ok, :optimized} = Maintenance.optimize_indices(table, num_indices_to_merge: 0)
    {:ok, :scheduled} = TableCache.prewarm(cache, "docs", indexes: [:words])
    await_warmed(cache, "docs", ["words"])
    assert_receive {:observed, [:lancedb, :operation, :start], _, %{operation: :prewarm_index}}
    {:ok, %{"words" => extended}} = TableCache.status(cache, "docs")
    assert extended.index_uuid == before.index_uuid
    assert extended.version > before.version

    {:ok, :created} = Index.create(table, :text, type: :fts, name: "words", replace: true)
    {:ok, :scheduled} = TableCache.prewarm(cache, "docs", indexes: [:words])
    await_warmed(cache, "docs", ["words"])
    assert_receive {:observed, [:lancedb, :operation, :start], _, %{operation: :prewarm_index}}
    {:ok, %{"words" => replacement}} = TableCache.status(cache, "docs")
    assert replacement.index_uuid != extended.index_uuid
  end

  test "indexes added to a running batch queue once and failed table opens are retryable", %{
    path: path,
    conn: conn
  } do
    cache = cache(path)
    {:ok, :scheduled} = TableCache.prewarm(cache, "missing", indexes: [:words])

    assert_eventually(fn ->
      match?(
        {:ok, %{"words" => %Warming{status: :failed, error: %Error{kind: :not_found}}}},
        TableCache.status(cache, "missing")
      )
    end)

    {:ok, table} = Connection.create_table(conn, "missing", [%{text: "document"}])
    {:ok, :created} = Index.create(table, :text, type: :fts, name: "words")
    {:ok, :scheduled} = TableCache.prewarm(cache, "missing", indexes: [:words])
    await_warmed(cache, "missing", ["words"])

    handler = block(cache, :cache_warm, "docs")
    {:ok, :scheduled} = TableCache.prewarm(cache, "docs", indexes: [:words])
    assert_receive {:blocked, first}, 2_000
    {:ok, :scheduled} = TableCache.prewarm(cache, "docs", indexes: [:words, :titles])

    assert {:ok, %{"words" => %Warming{status: :running}, "titles" => %Warming{status: :queued}}} =
             TableCache.status(cache, "docs")

    refute_receive {:blocked, _}, 50
    send(first, :continue)
    assert_receive {:blocked, second}, 2_000
    assert {:ok, %{"words" => %Warming{status: :completed}}} = TableCache.status(cache, "docs")
    :telemetry.detach(handler)
    send(second, :continue)
    await_warmed(cache, "docs", ["words", "titles"])
  end

  test "failed and crashed warmers release attempts for retry and emit failure telemetry", %{
    path: path
  } do
    cache = cache(path)
    watch(cache)
    {:ok, table} = TableCache.fetch(cache, "docs")
    {:ok, :scheduled} = TableCache.prewarm(cache, "docs", indexes: [:missing, :words])
    await_warmed(cache, "docs", ["words"])

    assert {:ok, %{"missing" => %Warming{status: :failed, error: %Error{kind: :not_found}}}} =
             TableCache.status(cache, "docs")

    assert_receive {:observed, [:lancedb, :table_cache, :warm], _,
                    %{index_name: "missing", status: :failed, error_kind: :not_found}}

    {:ok, :created} = Index.create(table, :title, type: :fts, name: "missing")
    {:ok, :scheduled} = TableCache.prewarm(cache, "docs", indexes: [:missing])
    await_warmed(cache, "docs", ["missing"])

    handler = block(cache, :cache_warm, "docs")
    {:ok, :scheduled} = TableCache.prewarm(cache, "docs", indexes: [:words])
    assert_receive {:blocked, worker}, 2_000
    :telemetry.detach(handler)
    Process.exit(worker, :kill)

    assert_eventually(fn ->
      match?(
        {:ok, %{"words" => %Warming{status: :failed, error: %Error{kind: :execution}}}},
        TableCache.status(cache, "docs")
      )
    end)

    assert Process.alive?(cache)
    {:ok, :scheduled} = TableCache.prewarm(cache, "docs", indexes: [:words])
    await_warmed(cache, "docs", ["words"])
  end

  test "warming concurrency and pending requests stay bounded", %{path: path, conn: conn} do
    {:ok, other} = Connection.open_table(conn, "other")
    {:ok, :created} = Index.create(other, :text, type: :fts, name: "words")
    cache = cache(path, max_warmers: 1, max_requests: 2)
    handler = block(cache, :cache_warm, :any)
    {:ok, :scheduled} = TableCache.prewarm(cache, "docs", indexes: [:words])
    assert_receive {:blocked, first}, 2_000
    {:ok, :scheduled} = TableCache.prewarm(cache, "other", indexes: [:words])
    assert {:error, %Error{kind: :busy}} = TableCache.prewarm(cache, "third", indexes: [:words])
    refute_receive {:blocked, _}, 50
    send(first, :continue)
    assert_receive {:blocked, second}, 2_000
    :telemetry.detach(handler)
    send(second, :continue)
    await_warmed(cache, "other", ["words"])
    assert :ok = TableCache.invalidate(cache, "other")
    assert {:ok, :scheduled} = TableCache.prewarm(cache, "docs", indexes: [:titles])
    await_warmed(cache, "docs", ["titles"])
  end

  test "finished warming releases request capacity while status history stays bounded", %{
    path: path
  } do
    cache = cache(path, max_requests: 1)
    assert {:ok, :scheduled} = TableCache.prewarm(cache, "docs", indexes: [:words])
    await_warmed(cache, "docs", ["words"])
    assert {:ok, _} = TableCache.fetch(cache, "other")
    assert {:ok, %{"words" => %Warming{status: :completed}}} = TableCache.status(cache, "docs")

    assert {:ok, :scheduled} = TableCache.prewarm(cache, "docs", indexes: [:titles])
    await_warmed(cache, "docs", ["titles"])

    assert {:ok, %{"titles" => %Warming{status: :completed}} = statuses} =
             TableCache.status(cache, "docs")

    assert map_size(statuses) == 1

    for name <- ["missing-one", "missing-two"] do
      assert {:ok, :scheduled} = TableCache.prewarm(cache, "docs", indexes: [name])

      assert_eventually(fn ->
        match?({:ok, %{^name => %Warming{status: :failed}}}, TableCache.status(cache, "docs"))
      end)

      assert {:ok, statuses} = TableCache.status(cache, "docs")
      assert Map.keys(statuses) == [name]
      assert {:ok, _} = TableCache.fetch(cache, "third")
    end
  end

  test "status pruning protects running and queued requests and their shared work", %{path: path} do
    cache = cache(path, max_requests: 2, max_warmers: 1)
    assert {:ok, :scheduled} = TableCache.prewarm(cache, "docs", indexes: [:words])
    await_warmed(cache, "docs", ["words"])
    handler = block(cache, :cache_warm, "docs")
    assert {:ok, :scheduled} = TableCache.prewarm(cache, "docs", indexes: [:titles])
    assert_receive {:blocked, worker}, 2_000
    assert {:ok, :scheduled} = TableCache.prewarm(cache, "other", indexes: [:missing])

    assert {:ok, %{"titles" => %Warming{status: :running}} = running} =
             TableCache.status(cache, "docs")

    assert map_size(running) == 1
    assert {:ok, %{"missing" => %Warming{status: :queued}}} = TableCache.status(cache, "other")
    assert {:ok, :scheduled} = TableCache.prewarm(cache, "docs", indexes: [:titles])
    assert {:error, %Error{kind: :busy}} = TableCache.prewarm(cache, "third", indexes: [:missing])
    refute_receive {:blocked, _}, 50
    :telemetry.detach(handler)
    send(worker, :continue)
    await_warmed(cache, "docs", ["titles"])

    assert_eventually(fn ->
      match?({:ok, %{"missing" => %Warming{status: :failed}}}, TableCache.status(cache, "other"))
    end)
  end

  test "capacity eviction frees pending request capacity before admission", %{path: path} do
    cache = cache(path, max_tables: 1, max_requests: 1)
    handler = block(cache, :cache_warm, "docs")
    assert {:ok, :scheduled} = TableCache.prewarm(cache, "docs", indexes: [:words])
    assert_receive {:blocked, worker}, 2_000

    assert {:ok, other} = TableCache.fetch(cache, "other")
    assert {:error, %Error{kind: :not_found}} = TableCache.status(cache, "docs")
    assert map_size(:sys.get_state(cache).tasks) == 1
    :telemetry.detach(handler)
    send(worker, :continue)
    assert_eventually(fn -> map_size(:sys.get_state(cache).tasks) == 0 end)
    assert {:ok, ^other} = TableCache.fetch(cache, "other")
  end

  test "rejected requests preserve evictable entries and retries consume pending capacity", %{
    path: path
  } do
    cache = cache(path, max_tables: 2, max_requests: 1)
    assert {:ok, :scheduled} = TableCache.prewarm(cache, "docs", indexes: [:words])
    await_warmed(cache, "docs", ["words"])
    {:ok, original} = TableCache.fetch(cache, "docs")
    handler = block(cache, :cache_open, "other")
    pending = Task.async(fn -> TableCache.fetch(cache, "other") end)
    assert_receive {:blocked, worker}, 2_000

    assert {:error, %Error{kind: :busy}} = TableCache.fetch(cache, "third")
    assert {:error, %Error{kind: :busy}} = TableCache.prewarm(cache, "docs", indexes: [:words])
    assert {:ok, ^original} = TableCache.fetch(cache, "docs")
    assert {:ok, %{"words" => %Warming{status: :completed}}} = TableCache.status(cache, "docs")
    refute Map.has_key?(:sys.get_state(cache).entries, "third")
    :telemetry.detach(handler)
    send(worker, :continue)
    assert {:ok, _} = Task.await(pending)
    assert {:ok, :scheduled} = TableCache.prewarm(cache, "docs", indexes: [:words])
    await_warmed(cache, "docs", ["words"])
  end

  test "invalidation and eviction discard late warming results", %{path: path} do
    cache = cache(path, max_tables: 1)
    handler = block(cache, :cache_warm, "docs")
    {:ok, :scheduled} = TableCache.prewarm(cache, "docs", indexes: [:words])
    assert_receive {:blocked, old_worker}, 2_000
    assert :ok = TableCache.invalidate(cache, "docs")
    {:ok, new_table} = TableCache.fetch(cache, "docs")
    send(old_worker, :continue)
    assert_eventually(fn -> map_size(:sys.get_state(cache).tasks) == 0 end)
    assert {:ok, %{}} = TableCache.status(cache, "docs")
    assert {:ok, ^new_table} = TableCache.fetch(cache, "docs")

    {:ok, :scheduled} = TableCache.prewarm(cache, "docs", indexes: [:words])
    assert_receive {:blocked, old_worker}, 2_000
    {:ok, _} = TableCache.fetch(cache, "other")
    send(old_worker, :continue)
    assert_eventually(fn -> map_size(:sys.get_state(cache).tasks) == 0 end)
    assert {:error, %Error{kind: :not_found}} = TableCache.status(cache, "docs")
    :telemetry.detach(handler)
  end

  test "a replacement during warming cannot be marked completed", %{path: path} do
    cache = cache(path)
    {:ok, table} = TableCache.fetch(cache, "docs")
    handler = block_operation(:prewarm_index)
    {:ok, :scheduled} = TableCache.prewarm(cache, "docs", indexes: [:words])
    assert_receive {:blocked, worker}, 2_000
    {:ok, :created} = Index.create(table, :text, type: :fts, name: "words", replace: true)
    :telemetry.detach(handler)
    send(worker, :continue)

    assert_eventually(fn ->
      match?(
        {:ok, %{"words" => %Warming{status: :failed, error: %Error{kind: :conflict}}}},
        TableCache.status(cache, "docs")
      )
    end)

    {:ok, :scheduled} = TableCache.prewarm(cache, "docs", indexes: [:words])
    await_warmed(cache, "docs", ["words"])
  end

  test "native admission failure is reported and retry works after capacity returns", %{
    path: path
  } do
    cache = cache(path)
    {:ok, table} = TableCache.fetch(cache, "docs")
    Execution.configure(reads: 1)
    on_exit(fn -> Execution.configure([]) end)
    stream = Query.stream(Query.new(table), batch_size: 1)

    {:suspended, _, continuation} =
      Enumerable.reduce(stream, {:cont, nil}, fn row, _ -> {:suspend, row} end)

    {:ok, :scheduled} = TableCache.prewarm(cache, "docs", indexes: [:words])

    assert_eventually(fn ->
      match?(
        {:ok, %{"words" => %Warming{status: :failed, error: %Error{kind: :busy}}}},
        TableCache.status(cache, "docs")
      )
    end)

    assert {:ok, ^table} = TableCache.fetch(cache, "docs")
    continuation.({:halt, nil})
    {:ok, :scheduled} = TableCache.prewarm(cache, "docs", indexes: [:words])
    await_warmed(cache, "docs", ["words"])
  end

  test "instances keep sessions separate and their task supervisor follows cache lifetime", %{
    path: path
  } do
    first = cache(path, name: __MODULE__, index_cache_size_bytes: 16 * 1024 * 1024)
    second = cache(path, index_cache_size_bytes: 0, metadata_cache_size_bytes: 0)
    {:ok, table} = TableCache.fetch(__MODULE__, "docs")
    {:ok, separate} = TableCache.fetch(second, "docs")
    {:ok, :scheduled} = TableCache.prewarm(first, "docs", indexes: [:words])
    await_warmed(first, "docs", ["words"])
    {:ok, stats} = Table.cache_stats(table)
    {:ok, isolated} = Table.cache_stats(separate)
    assert stats["index"]["num_entries"] > 0
    assert isolated["index"]["num_entries"] == 0
    supervisor = :sys.get_state(first).supervisor
    monitor = Process.monitor(supervisor)
    GenServer.stop(first)
    assert_receive {:DOWN, ^monitor, :process, ^supervisor, _}, 2_000
  end

  test "multiple unnamed children have distinct IDs that survive restarts", %{path: path} do
    children =
      for opts <- [[uri: path], [uri: path], [uri: path, name: nil], [uri: path, name: nil]],
          do: {TableCache, opts}

    supervisor =
      start_supervised!(%{
        id: make_ref(),
        start: {Supervisor, :start_link, [children, [strategy: :one_for_one]]},
        type: :supervisor
      })

    caches = Supervisor.which_children(supervisor)
    assert length(caches) == 4
    assert length(Enum.uniq_by(caches, &elem(&1, 0))) == 4

    for {_id, pid, :worker, _modules} <- caches do
      assert {:ok, _} = TableCache.fetch(pid, "docs")
    end

    {id, previous, :worker, _modules} = hd(caches)
    assert :ok = Supervisor.terminate_child(supervisor, id)
    assert {:ok, replacement} = Supervisor.restart_child(supervisor, id)
    assert replacement != previous
    assert {:ok, _} = TableCache.fetch(replacement, "docs")
  end

  test "validates configuration and requests before touching storage", %{path: path} do
    for opts <- [
          [uri: nil],
          [uri: path, max_tables: 0],
          [uri: path, max_requests: nil],
          [uri: path, max_warmers: -1],
          [uri: path, max_opens: 0],
          [uri: path, idle_timeout_ms: 0],
          [uri: path, connect_timeout_ms: 0],
          [uri: path, open_timeout_ms: 0],
          [uri: path, index_cache_size_bytes: nil],
          [uri: path, unknown: true]
        ] do
      assert_raise ArgumentError, fn -> TableCache.start_link(opts) end
    end

    cache = cache(path, idle_timeout_ms: nil, connect_timeout_ms: nil, open_timeout_ms: nil)

    for names <- [nil, [], [:words, "words"], [123]] do
      assert_raise ArgumentError, fn -> TableCache.prewarm(cache, "docs", indexes: names) end
    end

    assert_raise ArgumentError, fn -> TableCache.fetch(cache, "", timeout_ms: 1) end
    assert_raise ArgumentError, fn -> TableCache.fetch(cache, "docs", timeout_ms: 0) end

    assert_raise ArgumentError, fn ->
      TableCache.prewarm(cache, "docs", indexes: [:words], unknown: true)
    end

    assert :sys.get_state(cache).conn == nil
  end

  test "held S3 index reads leave cache lookup and ordinary queries available", %{path: path} do
    {fixture, options} = object_store(path)
    remote = cache("s3://probe", storage_options: options)
    {:ok, table} = TableCache.fetch(remote, "docs")
    :ok = LanceDB.ObjectStoreFixture.hold(fixture, :indexes)
    {:ok, :scheduled} = TableCache.prewarm(remote, "docs", indexes: [:words, :titles])
    assert_receive {^fixture, :blocked, _key}, 2_000
    assert {:ok, ^table} = TableCache.fetch(remote, "docs")
    assert {:ok, [%{"id" => 1}]} = Query.query(table, filter: "id = 1", columns: [:id])
    :ok = LanceDB.ObjectStoreFixture.release(fixture)
    await_warmed(remote, "docs", ["words", "titles"])
  end

  test "first S3 query after warming reuses retained index contents", %{path: path} do
    {fixture, options} = object_store(path)

    remote =
      cache("s3://probe", storage_options: options, index_cache_size_bytes: 16 * 1024 * 1024)

    {:ok, :scheduled} = TableCache.prewarm(remote, "docs", indexes: [:words, :titles])
    await_warmed(remote, "docs", ["words", "titles"])
    {:ok, table} = TableCache.fetch(remote, "docs")
    before = LanceDB.ObjectStoreFixture.requests(fixture)
    query = [text_column: :text, filter: "id = 42", columns: [:id]]
    assert {:ok, warmed_rows} = Query.full_text_search(table, "document", query)
    warm_reads = LanceDB.ObjectStoreFixture.requests(fixture) |> Enum.drop(length(before))

    {:ok, conn} =
      Connection.connect("s3://probe", options,
        index_cache_size_bytes: 16 * 1024 * 1024,
        read_consistency_interval_ms: 0
      )

    {:ok, cold} = Connection.open_table(conn, "docs")
    before = LanceDB.ObjectStoreFixture.requests(fixture)
    assert {:ok, ^warmed_rows} = Query.full_text_search(cold, "document", query)
    cold_reads = LanceDB.ObjectStoreFixture.requests(fixture) |> Enum.drop(length(before))
    assert index_reads(cold_reads) > 0
    assert index_reads(warm_reads) < index_reads(cold_reads)
  end

  defp object_store(path) do
    fixture = start_supervised!({LanceDB.ObjectStoreFixture, path})

    options = %{
      aws_region: "us-east-1",
      aws_access_key_id: "fixture",
      aws_secret_access_key: "fixture",
      allow_http: true,
      aws_virtual_hosted_style_request: false,
      aws_endpoint: "http://127.0.0.1:#{LanceDB.ObjectStoreFixture.port(fixture)}"
    }

    {fixture, options}
  end

  defp index_reads(requests),
    do:
      Enum.count(requests, fn {method, key} ->
        method == :GET and String.contains?(key, "/_indices/")
      end)

  defp cache(path, opts \\ []) do
    opts = Keyword.merge([uri: path, read_consistency_interval_ms: 0], opts)
    start_supervised!(Supervisor.child_spec({TableCache, opts}, id: make_ref()))
  end

  defp await_warmed(cache, name, indexes) do
    LanceDB.TestSupport.assert_eventually(
      fn ->
        case TableCache.status(cache, name) do
          {:ok, statuses} ->
            Enum.all?(indexes, &match?(%Warming{status: :completed}, statuses[&1]))

          _ ->
            false
        end
      end,
      5_000
    )
  end

  defp block(cache, operation, table_name) do
    id = :sys.get_state(cache).cache_id

    attach(&__MODULE__.gate/4, %{
      owner: self(),
      cache_id: id,
      operation: operation,
      table_name: table_name
    })
  end

  defp block_operation(operation),
    do:
      attach(&__MODULE__.gate/4, %{
        owner: self(),
        cache_id: :any,
        operation: operation,
        table_name: :any
      })

  defp watch(cache) do
    id = :sys.get_state(cache).cache_id
    handler = make_ref()

    :ok =
      :telemetry.attach_many(
        handler,
        [[:lancedb, :operation, :start], [:lancedb, :table_cache, :warm]],
        &__MODULE__.observe/4,
        %{owner: self(), cache_id: id}
      )

    on_exit(fn -> :telemetry.detach(handler) end)
  end

  defp attach(fun, context) do
    handler = make_ref()
    :ok = :telemetry.attach(handler, [:lancedb, :operation, :start], fun, context)
    on_exit(fn -> :telemetry.detach(handler) end)
    handler
  end

  def gate(_event, _measurements, metadata, context) do
    if metadata.operation == context.operation and
         (context.cache_id == :any or metadata[:cache_id] == context.cache_id) and
         (context.table_name == :any or metadata[:table_name] == context.table_name) do
      send(context.owner, {:blocked, self()})

      receive do
        :continue -> :ok
      after
        10_000 -> raise "cache test worker was not released"
      end
    end
  end

  def observe(event, measurements, metadata, context) do
    if metadata[:cache_id] == context.cache_id or metadata[:operation] == :prewarm_index,
      do: send(context.owner, {:observed, event, measurements, metadata})
  end
end
