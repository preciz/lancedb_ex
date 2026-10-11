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
      %{id: 1, vector: [1.0, 0.0]},
      %{id: 2, vector: [0.0, 1.0]}
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
after
  File.rm_rf!(path)
end
