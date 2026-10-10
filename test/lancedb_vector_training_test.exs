defmodule LanceDB.VectorTrainingTest do
  use LanceDB.DataCase, async: true

  setup %{path: path} do
    {:ok, conn} = LanceDB.connect(path)
    %{conn: conn, path: path}
  end

  defp vector(id) do
    for d <- 1..16 do
      :math.sin(id * d / 20)
    end
  end

  test("all vector families accept IVF training controls and preserve indexed retrieval", %{
    conn: conn,
    path: path
  }) do
    rows =
      for id <- 1..300 do
        %{id: id, vector: vector(id)}
      end

    {:ok, table} = LanceDB.Connection.create_table(conn, "vectors", rows)

    for {type, expected} <- [
          ivf_flat: "IVF_FLAT",
          ivf_pq: "IVF_PQ",
          ivf_sq: "IVF_SQ",
          ivf_rq: "IVF_RQ",
          hnsw_flat: "IVF_HNSW_FLAT",
          hnsw_pq: "IVF_HNSW_PQ",
          hnsw_sq: "IVF_HNSW_SQ"
        ] do
      opts = [
        type: type,
        name: "vectors",
        replace: true,
        target_partition_size: 300,
        sample_rate: 32,
        max_iterations: 2
      ]

      opts =
        if type in [:hnsw_flat, :hnsw_pq, :hnsw_sq] do
          opts ++ [m: 300, ef_construction: 300]
        else
          opts
        end

      assert {:ok, :created} = LanceDB.Index.create(table, :vector, opts)

      assert {:ok, [%LanceDB.Index.Description{index_type: ^expected}]} =
               LanceDB.Index.list(table)

      assert {:ok, %{"num_indexed_rows" => 300, "index_type" => ^expected}} =
               LanceDB.Index.stats(table, "vectors")

      {:ok, other} = LanceDB.connect(path)
      {:ok, reopened} = LanceDB.Connection.open_table(other, "vectors")
      assert {:ok, :prewarmed} = LanceDB.Index.prewarm(reopened, "vectors")

      assert {:ok, [%{"id" => 300}]} =
               LanceDB.Query.search(reopened, vector(300),
                 limit: 1,
                 nprobes: 1,
                 ef: 300,
                 refine_factor: 300
               )

      assert {:ok, plan} =
               LanceDB.TestSupport.explain_plan(reopened, vector: vector(300), limit: 1)

      assert plan =~ ~r/ANN|Ivf/i
    end

    assert {:ok, :dropped} = LanceDB.Index.drop(table, "vectors")
  end

  test("4-bit PQ trains on fewer than 256 rows and enforces even subdivisions", %{conn: conn}) do
    {:ok, table} =
      LanceDB.Connection.create_table(
        conn,
        "pq4",
        for id <- 1..32 do
          %{id: id, vector: vector(id)}
        end
      )

    for type <- [:ivf_pq, :hnsw_pq] do
      assert {:ok, :created} =
               LanceDB.Index.create(table, :vector,
                 type: type,
                 name: "pq4",
                 replace: true,
                 num_partitions: 1,
                 num_bits: 4,
                 num_sub_vectors: 2,
                 max_iterations: 2
               )

      assert {:ok, [%{"id" => 32}]} =
               LanceDB.Query.search(table, vector(32), limit: 1, refine_factor: 32, ef: 32)
    end
  end

  test("RabitQ bit widths and approximation modes work across query execution paths", %{
    conn: conn
  }) do
    {:ok, table} =
      LanceDB.Connection.create_table(
        conn,
        "rq",
        for id <- 1..64 do
          %{id: id, text: "document", vector: vector(id)}
        end
      )

    assert {:ok, :created} = LanceDB.Index.create(table, :text, type: :fts)

    for bits <- [1, 4, 9] do
      assert {:ok, :created} =
               LanceDB.Index.create(table, :vector,
                 type: :ivf_rq,
                 name: "rq",
                 replace: true,
                 num_bits: bits,
                 num_partitions: 1,
                 sample_rate: 16,
                 max_iterations: 2
               )

      for mode <- [:fast, :normal, :accurate, nil] do
        opts = [approx_mode: mode, limit: 1, refine_factor: 64, columns: [:id], with_row_id: true]
        assert {:ok, [%{"id" => 64}]} = LanceDB.Query.search(table, vector(64), opts)
        selectors = [vector: vector(64)] ++ opts
        assert [%{"id" => 64}] = Enum.to_list(LanceDB.TestSupport.rows(table, selectors))
        assert {:ok, analysis} = LanceDB.TestSupport.analyze_plan(table, selectors)
        assert analysis =~ "Output: rows=1"

        assert {:ok, %LanceDB.ArrowSchema{fields: fields}} =
                 LanceDB.TestSupport.output_schema(table, selectors)

        assert Enum.map(fields, & &1.name) == ["id", "_distance", "_rowid"]
        assert {:ok, plan} = LanceDB.TestSupport.explain_plan(table, selectors)
        assert plan =~ ~r/ANN|Ivf/i
        assert {:ok, rows} = LanceDB.Query.batch_search(table, [vector(64), vector(1)], opts)
        assert Enum.sort(Enum.map(rows, &{&1["query_index"], &1["id"]})) == [{0, 64}, {1, 1}]

        assert {:ok, [%{"id" => 64}]} =
                 LanceDB.Query.hybrid_search(
                   table,
                   vector(64),
                   "document",
                   opts ++ [filter: "id = 64"]
                 )
      end
    end
  end

  test("new index types support cosine and dot metrics", %{conn: conn}) do
    {:ok, table} =
      LanceDB.Connection.create_table(
        conn,
        "metrics",
        for id <- 1..64 do
          %{id: id, vector: [id / 64, 1.0, 0, 0, 0, 0, 0, 0]}
        end
      )

    for type <- [:ivf_sq, :ivf_rq, :hnsw_flat], distance <- [:cosine, :dot] do
      options = [type: type, name: "metric", replace: true, distance: distance, num_partitions: 1]

      options =
        if type == :hnsw_flat do
          options ++ [m: 64, ef_construction: 64]
        else
          options
        end

      assert {:ok, :created} = LanceDB.Index.create(table, :vector, options)

      assert {:ok, [%{"id" => 64}]} =
               LanceDB.Query.search(table, [1, 1, 0, 0, 0, 0, 0, 0],
                 distance: distance,
                 limit: 1,
                 refine_factor: 64,
                 ef: 64
               )
    end
  end

  test("new indexes and approximation tuning support multivector documents", %{conn: conn}) do
    rows =
      for id <- 1..64 do
        %{id: id, embeddings: [vector(id), vector(id + 1)]}
      end

    {:ok, table} =
      LanceDB.Connection.create_table(conn, "multi_training", rows,
        schema: [id: :int64, embeddings: {:list, {:vector, 16}}]
      )

    for type <- [:ivf_sq, :ivf_rq, :hnsw_flat] do
      assert {:ok, :created} =
               LanceDB.Index.create(table, :embeddings,
                 type: type,
                 name: "multi",
                 replace: true,
                 distance: :cosine,
                 num_partitions: 1,
                 sample_rate: 16,
                 max_iterations: 2
               )

      for mode <- [:fast, :normal, :accurate] do
        opts = [approx_mode: mode, limit: 1, filter: "id = 64", nprobes: 1, ef: 64]

        assert {:ok, [%{"id" => 64}]} =
                 LanceDB.Query.multivector_search(table, [vector(64), vector(65)], opts)

        assert [%{"id" => 64}] =
                 Enum.to_list(
                   LanceDB.TestSupport.rows(
                     table,
                     [multivector: [vector(64), vector(65)]] ++ opts
                   )
                 )
      end
    end
  end

  test("RabitQ dimension constraints return an error without replacing an existing index", %{
    conn: conn
  }) do
    {:ok, table} =
      LanceDB.Connection.create_table(
        conn,
        "bad_rq",
        for id <- 1..32 do
          %{id: id, vector: [id / 32, 1.0]}
        end
      )

    assert {:ok, :created} =
             LanceDB.Index.create(table, :vector, type: :ivf_flat, name: "existing")

    assert {:error, reason} =
             LanceDB.Index.create(table, :vector, type: :ivf_rq, name: "existing", replace: true)

    assert reason.message =~ "divisible by 8"
    assert {:ok, [%LanceDB.Index.Description{index_type: "IVF_FLAT"}]} = LanceDB.Index.list(table)
  end

  test "invalid training options and incompatible families fail before native execution" do
    for key <- [:sample_rate, :max_iterations, :target_partition_size],
        value <- [nil, 0, -1, 1.5, "32", 4_294_967_296] do
      assert_raise ArgumentError, fn ->
        LanceDB.Index.create(:unused, :vector, [{key, value}, type: :ivf_flat])
      end
    end

    for type <- [:auto, :btree, :bitmap, :label_list, :fm, :fts],
        key <- [:sample_rate, :max_iterations, :target_partition_size, :num_bits] do
      assert_raise ArgumentError, fn ->
        LanceDB.Index.create(:unused, :vector, [{key, 4}, type: type])
      end
    end

    for opts <- [
          [type: :ivf_flat, num_partitions: 1, target_partition_size: 100],
          [type: :ivf_sq, num_bits: 8],
          [type: :hnsw_flat, num_bits: 8],
          [type: :ivf_rq, num_sub_vectors: 2],
          [type: :ivf_sq, m: 8],
          [type: :ivf_pq, num_bits: 4, num_sub_vectors: 1],
          [type: :hnsw_pq, num_bits: 4, num_sub_vectors: 3],
          [type: :ivf_pq, num_bits: 1],
          [type: :ivf_pq, num_bits: 4.0],
          [type: :ivf_rq, num_bits: 0],
          [type: :ivf_rq, num_bits: 10],
          [type: :ivf_rq, num_bits: 257],
          [type: :ivf_rq, num_bits: nil]
        ] do
      assert_raise ArgumentError, fn -> LanceDB.Index.create(:unused, :vector, opts) end
    end

    for mode <- [:unknown, "fast", true, 1] do
      assert_raise ArgumentError, fn ->
        LanceDB.Query.search(%LanceDB.Table{resource: make_ref()}, [1, 0], approx_mode: mode)
      end

      assert_raise ArgumentError, fn ->
        LanceDB.Query.batch_search(%LanceDB.Table{resource: make_ref()}, [[1, 0]],
          approx_mode: mode
        )
      end

      assert_raise ArgumentError, fn ->
        LanceDB.Query.multivector_search(%LanceDB.Table{resource: make_ref()}, [[1, 0]],
          approx_mode: mode
        )
      end

      assert_raise ArgumentError, fn ->
        LanceDB.Query.hybrid_search(%LanceDB.Table{resource: make_ref()}, [1, 0], "word",
          approx_mode: mode
        )
      end
    end
  end
end
