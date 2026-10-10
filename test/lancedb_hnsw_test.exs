defmodule LanceDB.HnswTest do
  use LanceDB.DataCase, async: true

  setup %{path: path} do
    {:ok, conn} = LanceDB.connect(path)
    %{conn: conn, path: path}
  end

  for {type, native_type} <- [hnsw_pq: "IVF_HNSW_PQ", hnsw_sq: "IVF_HNSW_SQ"] do
    pq_options =
      if type == :hnsw_pq do
        [num_sub_vectors: 1]
      else
        []
      end

    test("#{type} trains, persists, and supports tuned vector and hybrid searches", %{
      conn: conn,
      path: path
    }) do
      type = unquote(type)

      rows =
        for id <- 1..300 do
          %{id: id, text: "document", vector: [id / 300, :math.sin(id)]}
        end

      {:ok, table} = LanceDB.Connection.create_table(conn, "vectors", rows)
      options = [type: type, name: "vectors", num_partitions: 1, m: 8, ef_construction: 100]
      options = options ++ unquote(pq_options)
      assert {:ok, :created} = LanceDB.Index.create(table, :vector, options)

      assert {:ok, [%LanceDB.Index.Description{index_type: unquote(native_type)}]} =
               LanceDB.Index.list(table)

      assert {:ok, %{"num_indexed_rows" => 300, "loss" => loss}} =
               LanceDB.Index.stats(table, "vectors")

      assert is_float(loss) and loss >= 0
      assert {:ok, :created} = LanceDB.Index.create(table, :text, type: :fts, name: "words")
      {:ok, reopened_conn} = LanceDB.connect(path)
      {:ok, reopened} = LanceDB.Connection.open_table(reopened_conn, "vectors")
      assert {:ok, :prewarmed} = LanceDB.Index.prewarm(reopened, "vectors")
      query = [1.0, :math.sin(300)]

      assert {:ok, [%{"id" => 300}]} =
               LanceDB.Query.search(reopened, query,
                 limit: 1,
                 ef: 64,
                 nprobes: 1,
                 refine_factor: 10
               )

      assert {:ok, [%{"id" => 300}]} =
               LanceDB.Query.search(reopened, query, limit: 1, ef: 64, bypass_vector_index: true)

      assert {:ok, [%{"id" => 300, "_distance" => distance}]} =
               LanceDB.Query.search(reopened, query,
                 limit: 1,
                 ef: 32,
                 filter: "id = 300",
                 columns: [:id]
               )

      assert is_number(distance)

      assert {:ok, [%{"id" => 300, "_relevance_score" => score}]} =
               LanceDB.Query.hybrid_search(reopened, query, "document",
                 ef: 64,
                 limit: 1,
                 filter: "id = 300",
                 columns: [:id]
               )

      assert is_number(score)
      assert {:ok, :dropped} = LanceDB.Index.drop(reopened, "vectors")
      assert {:ok, nil} = LanceDB.Index.stats(reopened, "vectors")
    end
  end

  test("HNSW defaults and cosine distance", %{conn: conn}) do
    rows =
      for id <- 1..32 do
        %{id: id, vector: [id / 32, 1.0]}
      end

    {:ok, table} = LanceDB.Connection.create_table(conn, "defaults", rows)

    assert {:ok, :created} =
             LanceDB.Index.create(table, :vector, type: :hnsw_sq, distance: :cosine)

    assert {:ok, [%{"id" => 32}]} =
             LanceDB.Query.search(table, [1.0, 1.0], limit: 1, distance: :cosine, ef: 32)
  end

  test "invalid and inapplicable HNSW options fail before native execution" do
    for opts <- [
          [type: :btree, m: 8],
          [type: :ivf_pq, ef_construction: 100],
          [type: :hnsw_sq, num_sub_vectors: 1],
          [type: :hnsw_sq, m: 0],
          [type: :hnsw_pq, m: 1.5],
          [type: :hnsw_sq, ef_construction: -1],
          [type: :hnsw_pq, ef_construction: 4_294_967_296]
        ] do
      assert_raise ArgumentError, fn -> LanceDB.Index.create(:unused, :vector, opts) end
    end

    for ef <- [0, -1, nil, 1.5, "32", 4_294_967_296] do
      assert_raise ArgumentError, fn ->
        LanceDB.Query.search(%LanceDB.Table{resource: make_ref()}, [1.0], ef: ef)
      end

      assert_raise ArgumentError, fn ->
        LanceDB.Query.hybrid_search(%LanceDB.Table{resource: make_ref()}, [1.0], "word", ef: ef)
      end
    end
  end
end
