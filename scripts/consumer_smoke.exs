path =
  Path.join(
    System.tmp_dir!(),
    "lancedb-consumer-#{System.pid()}-#{System.unique_integer([:positive])}"
  )

try do
  {:ok, db} = LanceDB.connect(path)

  {:ok, table} =
    LanceDB.Connection.create_table(db, "items", [
      %{id: 1, vector: [1.0, 0.0]},
      %{id: 2, vector: [0.0, 1.0]}
    ])

  {:ok, 2} = LanceDB.Table.count_rows(table)
  {:ok, [%{"id" => 1}]} = LanceDB.Query.search(table, [1.0, 0.0], limit: 1)
  IO.puts("Consumer NIF loaded; LanceDB write and vector query passed.")
after
  File.rm_rf!(path)
end
