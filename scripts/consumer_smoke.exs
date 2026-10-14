path =
  Path.join(
    System.tmp_dir!(),
    "lancedb-consumer-#{System.pid()}-#{System.unique_integer([:positive])}"
  )

try do
  {:ok, db} =
    LanceDB.connect(path, %{},
      index_cache_size_bytes: 0,
      metadata_cache_size_bytes: 0,
      read_consistency_interval_ms: 0,
      timeout_ms: 10_000
    )

  {:ok, table} =
    LanceDB.Connection.create_table(db, "items", [
      %{id: 1, name: "Elixir", text: "Elixir and OTP", vector: [1.0, 0.0]},
      %{id: 2, name: "Bread", text: "Sourdough bread", vector: [0.0, 1.0]}
    ])

  {:ok, 2} = LanceDB.Table.count_rows(table)
  {:ok, reopened} = LanceDB.Connection.open_table(db, "items", timeout_ms: 10_000)
  {:ok, 2} = LanceDB.Table.count_rows(reopened)

  {:ok, %{"scope" => "session", "index" => index, "metadata" => metadata}} =
    LanceDB.Table.cache_stats(reopened)

  for cache <- [index, metadata] do
    %{"num_entries" => 0, "size_bytes" => 0, "capacity_bytes" => nil} = cache
  end

  {:ok, [%{"id" => 1}]} = LanceDB.Query.search(table, [1.0, 0.0], limit: 1)
  IO.puts("Consumer NIF loaded; LanceDB write and vector query passed.")
  IO.puts("Connect/open deadlines, initial cache budgets and cache statistics passed.")

  {:ok, :created} = LanceDB.Index.create(table, :name, type: :fts, name: "names_idx")
  {:ok, :created} = LanceDB.Index.create(table, :text, type: :fts, name: "text_idx")
  cache = LanceDB.ConsumerSmoke.TableCache
  indexes = ["names_idx", "text_idx"]

  {:ok, supervisor} =
    Supervisor.start_link(
      [
        {LanceDB.TableCache,
         name: cache,
         uri: path,
         index_cache_size_bytes: 16 * 1024 * 1024,
         metadata_cache_size_bytes: 1024 * 1024,
         read_consistency_interval_ms: 0}
      ],
      strategy: :one_for_one
    )

  try do
    if function_exported?(LanceDB.TableCache, :connection, 1) do
      {:ok, conn} = apply(LanceDB.TableCache, :connection, [cache])
      {:ok, ^conn} = apply(LanceDB.TableCache, :connection, [cache, [timeout_ms: 10_000]])
      {:ok, _} = LanceDB.Connection.create_table(conn, "from_cache", [%{id: 3}])
      {:ok, created} = LanceDB.TableCache.fetch(cache, "from_cache")
      {:ok, [%{"id" => 3}]} = LanceDB.Query.query(created)
      {:ok, ["from_cache", "items"]} = LanceDB.Connection.table_names(conn)
      IO.puts("Shared cache connection, direct creation and cached fetch passed.")
    end

    {:ok, :scheduled} = LanceDB.TableCache.prewarm(cache, "items", indexes: indexes)
    {:ok, cached} = LanceDB.TableCache.fetch(cache, "items", timeout_ms: 10_000)
    deadline = System.monotonic_time(:millisecond) + 10_000

    Stream.repeatedly(fn ->
      {:ok, statuses} = LanceDB.TableCache.status(cache, "items")
      statuses
    end)
    |> Enum.find(fn statuses ->
      for {_name, %LanceDB.TableCache.Warming{status: :failed, error: error}} <- statuses,
          do: raise(error)

      cond do
        Enum.all?(indexes, &match?(%LanceDB.TableCache.Warming{status: :completed}, statuses[&1])) ->
          true

        System.monotonic_time(:millisecond) >= deadline ->
          raise "Consumer index warming timed out: #{inspect(statuses)}"

        true ->
          Process.sleep(10)
          false
      end
    end)

    {:ok, ^cached} = LanceDB.TableCache.fetch(cache, "items")
    {:ok, before_query} = LanceDB.Table.cache_stats(cached)
    true = before_query["index"]["num_entries"] > 0

    {:ok, [%{"id" => 1}]} =
      LanceDB.Query.full_text_search(
        cached,
        LanceDB.FTS.multi_match("Elixir", [:name, :text]),
        columns: [:id]
      )

    {:ok, after_query} = LanceDB.Table.cache_stats(cached)
    true = after_query["index"]["hits"] > before_query["index"]["hits"]

    IO.puts(
      "Supervised table cache, background multi-index warming and shared-cache FTS query passed."
    )

    # Older Hex packages have no preparation API; newer wrappers may still
    # select an older NIF. Only a successful native call establishes support.
    preparation_available? =
      if function_exported?(LanceDB.FTS, :prepare, 1) do
        try do
          {:ok, _} = apply(LanceDB.FTS, :prepare, [""])
          true
        catch
          :error, :nif_not_loaded -> false
        end
      else
        false
      end

    if preparation_available? do
      {:ok, prepared} = apply(LanceDB.FTS, :prepare, [cached, "ELIXR", [index: "text_idx"]])
      1 = apply(LanceDB.FTS, :term_count, [prepared])

      {:ok, [%{"id" => 1}]} =
        LanceDB.Query.full_text_search(
          cached,
          LanceDB.FTS.match(prepared, column: :text, fuzziness: 1),
          columns: [:id]
        )

      {:ok, explicit} = apply(LanceDB.FTS, :prepare, ["ELIXR"])
      ^prepared = explicit
      IO.puts("Explicit and saved-analyzer FTS preparation and normalized fuzzy query passed.")
    else
      IO.puts("FTS preparation checks skipped: unavailable in this native build.")
    end
  after
    Supervisor.stop(supervisor)
  end
after
  File.rm_rf!(path)
end
