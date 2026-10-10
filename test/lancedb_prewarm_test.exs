defmodule LanceDB.PrewarmTest do
  use LanceDB.DataCase, async: true

  test "prewarms reopened scalar, full-text and vector indexes without changing data", %{
    path: path
  } do
    {:ok, conn} = LanceDB.connect(path)
    rows = for id <- 1..256, do: %{id: id, text: "document #{id}", vector: [id * 1.0, 1.0]}
    {:ok, table} = LanceDB.Connection.create_table(conn, "docs", rows)
    assert {:ok, :created} = LanceDB.Index.create(table, :id, type: :btree, name: "ids")
    assert {:ok, :created} = LanceDB.Index.create(table, :text, type: :fts, name: "words")

    assert {:ok, :created} =
             LanceDB.Index.create(table, :vector,
               type: :ivf_flat,
               num_partitions: 2,
               name: "vectors"
             )

    {:ok, version} = LanceDB.History.version(table)
    {:ok, reopened_conn} = LanceDB.connect(path)
    {:ok, reopened} = LanceDB.Connection.open_table(reopened_conn, "docs")

    for name <- [:ids, "words", "vectors"] do
      assert {:ok, :prewarmed} = LanceDB.Index.prewarm(reopened, name)
      assert {:ok, :prewarmed} = LanceDB.Index.prewarm(reopened, name)
    end

    assert {:ok, ^version} = LanceDB.History.version(reopened)
    assert {:ok, 256} = LanceDB.Table.count_rows(reopened)
    assert {:ok, [%{"id" => 42}]} = LanceDB.Query.query(reopened, filter: "id = 42")
    assert {:ok, [_]} = LanceDB.Query.full_text_search(reopened, "document", limit: 1)

    assert {:ok, [%{"id" => 42}]} =
             LanceDB.Query.search(reopened, [42.0, 1.0], limit: 1, nprobes: 2)

    assert {:error, _} = LanceDB.Index.prewarm(reopened, "missing")
    assert {:ok, reopened} = LanceDB.History.snapshot(reopened, version)
    assert {:ok, :prewarmed} = LanceDB.Index.prewarm(reopened, "ids")

    for name <- ["", nil, 123] do
      assert_raise ArgumentError, fn -> LanceDB.Index.prewarm(reopened, name) end
    end
  end
end
