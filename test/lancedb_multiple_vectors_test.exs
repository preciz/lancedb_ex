defmodule LanceDB.MultipleVectorsTest do
  use LanceDB.DataCase, async: true
  use ExUnitProperties

  setup %{path: path} do
    {:ok, conn} = LanceDB.connect(path)
    %{conn: conn}
  end

  test("batch results match independent searches with per-query pagination and projections", %{
    conn: conn
  }) do
    {:ok, table} =
      LanceDB.Connection.create_table(
        conn,
        "vectors",
        for id <- 1..20 do
          %{id: id, vector: [id, 1]}
        end
      )

    assert {:ok, :created} =
             LanceDB.Index.create(table, :vector, type: :ivf_flat, num_partitions: 1)

    for vectors <- [[[1, 1]], [[1, 1], [20, 1], [1, 1]]],
        projection <- [[], [columns: [:id]], [select: [label: "id + 10"]]],
        bypass <- [false, true] do
      opts =
        projection ++
          [limit: 2, offset: 1, filter: "id > 2", with_row_id: true, bypass_vector_index: bypass]

      assert {:ok, actual} = LanceDB.Query.batch_search(table, vectors, opts)
      assert length(actual) == length(vectors) * 2

      for {vector, index} <- Enum.with_index(vectors) do
        assert {:ok, expected} = LanceDB.Query.search(table, vector, opts)

        found =
          actual
          |> Enum.filter(&(&1["query_index"] == index))
          |> Enum.map(&Map.delete(&1, "query_index"))

        assert Enum.sort_by(found, & &1["_distance"]) == expected
      end

      selectors = [vectors: vectors] ++ opts
      streamed = LanceDB.TestSupport.rows(table, selectors ++ [batch_size: 1]) |> Enum.to_list()
      assert Enum.sort(streamed) == Enum.sort(actual)

      assert {:ok, %LanceDB.ArrowSchema{fields: fields}} =
               LanceDB.TestSupport.output_schema(table, selectors)

      assert Enum.sort(Enum.map(fields, & &1.name)) == Enum.sort(Map.keys(hd(actual)))
      assert Enum.find(fields, &(&1.name == "query_index")).type == :int32
    end

    assert {:ok, []} = LanceDB.Query.batch_search(table, [[1, 1], [20, 1]], filter: "id < 0")
    assert {:ok, plan} = LanceDB.TestSupport.explain_plan(table, vectors: [[1, 1], [20, 1]])
    assert plan =~ "query_index"

    assert {:ok, analysis} =
             LanceDB.TestSupport.analyze_plan(table, vectors: [[1, 1], [20, 1]], limit: 1)

    assert analysis =~ "Output: rows=2"
  end

  test("multivector search ranks documents, supports singleton queries and shared execution", %{
    conn: conn
  }) do
    {:ok, table} =
      LanceDB.Connection.create_table(
        conn,
        "documents",
        [
          %{id: 1, embeddings: [[1, 0], [0, 1]]},
          %{id: 2, embeddings: [[1, 0]]},
          %{id: 3, embeddings: [[-1, 0], [0, -1]]}
        ],
        schema: [id: :int64, embeddings: {:list, {:vector, 2}}]
      )

    for vectors <- [[[1, 0]], [[1, 0], [0, 1]]] do
      opts = [select: [item: "id"], limit: 3, with_row_id: true]
      assert {:ok, rows} = LanceDB.Query.multivector_search(table, vectors, opts)
      assert length(rows) == 3
      assert List.last(rows)["item"] == 3
      refute Map.has_key?(hd(rows), "query_index")
      assert Map.has_key?(hd(rows), "_rowid")
      selectors = [multivector: vectors] ++ opts
      assert Enum.to_list(LanceDB.TestSupport.rows(table, selectors)) == rows

      assert {:ok, %LanceDB.ArrowSchema{fields: fields}} =
               LanceDB.TestSupport.output_schema(table, selectors)

      assert Enum.sort(Enum.map(fields, & &1.name)) == Enum.sort(Map.keys(hd(rows)))
      assert {:ok, plan} = LanceDB.TestSupport.explain_plan(table, selectors)
      assert is_binary(plan)
      assert {:ok, analysis} = LanceDB.TestSupport.analyze_plan(table, selectors)
      assert analysis =~ "Output: rows=3"
    end

    assert {:ok, [%{"id" => 1}]} =
             LanceDB.Query.multivector_search(table, [[1, 0], [0, 1]], limit: 1)

    assert {:ok, [%{"id" => 2}]} =
             LanceDB.Query.multivector_search(table, [[1, 0], [0, 1]],
               filter: "id != 1",
               limit: 1
             )

    assert {:error, _} =
             LanceDB.Query.batch_search(table, [[1, 0], [0, 1]], vector_column: :embeddings)
  end

  test("mode and dimensions are checked against the stored column", %{conn: conn}) do
    {:ok, table} = LanceDB.Connection.create_table(conn, "single", [%{id: 1, vector: [1, 0]}])
    assert {:error, _} = LanceDB.Query.multivector_search(table, [[1, 0]], vector_column: :vector)
    assert {:error, _} = LanceDB.Query.multivector_search(table, [[1, 0]])
    assert {:error, _} = LanceDB.Query.batch_search(table, [[1, 0, 0]], vector_column: :vector)
    assert {:error, _} = LanceDB.Query.batch_search(table, [[1, 0]], vector_column: :missing)

    {:ok, collision} =
      LanceDB.Connection.create_table(conn, "collision", [%{query_index: 9, vector: [1, 0]}])

    assert {:error, reason} = LanceDB.Query.batch_search(collision, [[1, 0]])
    assert reason.message =~ "query_index"
  end

  test("multivector indexes preserve the best match and work after reopening", %{conn: conn}) do
    rows =
      for id <- 1..300 do
        %{id: id, embeddings: [[1, id / 300], [0, 1]]}
      end

    {:ok, table} =
      LanceDB.Connection.create_table(conn, "indexed_multi", rows,
        schema: [id: :int64, embeddings: {:list, {:vector, 2}}]
      )

    vectors = [[1, 1], [0, 1]]
    assert {:ok, [%{"id" => 300}]} = LanceDB.Query.multivector_search(table, vectors, limit: 1)

    assert {:ok, :created} =
             LanceDB.Index.create(table, :embeddings,
               type: :ivf_flat,
               distance: :cosine,
               num_partitions: 1
             )

    {:ok, table} = LanceDB.Connection.open_table(conn, "indexed_multi")

    assert {:ok, [%{"id" => 300}]} =
             LanceDB.Query.multivector_search(table, vectors, limit: 1, nprobes: 1)

    assert {:ok, [%{"id" => 300}]} =
             LanceDB.Query.multivector_search(table, vectors, limit: 1, bypass_vector_index: true)
  end

  test("nested columns are resolved by mode and explicit paths", %{conn: conn}) do
    fields = [id: :int64, data: {:struct, single: {:vector, 2}, multi: {:list, {:vector, 2}}}]

    {:ok, table} =
      LanceDB.Connection.create_table(
        conn,
        "nested",
        [%{id: 1, data: %{single: [1, 0], multi: [[1, 0], [0, 1]]}}],
        schema: fields
      )

    for opts <- [[], [vector_column: "data.single"]] do
      assert {:ok, [%{"id" => 1, "query_index" => 0}]} =
               LanceDB.Query.batch_search(table, [[1, 0]], opts)
    end

    for opts <- [[], [vector_column: "data.multi"]] do
      assert {:ok, [%{"id" => 1}]} =
               LanceDB.Query.multivector_search(table, [[1, 0], [0, 1]], opts)
    end
  end

  test("empty tables retain metadata schemas and return empty streams", %{conn: conn}) do
    {:ok, table} =
      LanceDB.Connection.create_empty_table(conn, "empty", id: :int64, vector: {:vector, 2})

    for vectors <- [[[1, 0]], [[1, 0], [0, 1]]] do
      opts = [vectors: vectors, columns: [:query_index]]
      assert {:ok, []} = LanceDB.Query.batch_search(table, vectors, columns: [:query_index])
      assert [] == Enum.to_list(LanceDB.TestSupport.rows(table, opts))

      assert {:ok, %LanceDB.ArrowSchema{fields: fields}} =
               LanceDB.TestSupport.output_schema(table, opts)

      assert Enum.map(fields, & &1.name) == ["query_index", "_distance"]
    end
  end

  property("exhaustive multivector distances match a cosine MaxSim oracle") do
    axes = [[1, 0], [0, 1], [-1, 0], [0, -1]]

    check(
      all(
        query <- list_of(member_of(axes), min_length: 1, max_length: 4),
        documents <-
          list_of(list_of(member_of(axes), min_length: 1, max_length: 4),
            min_length: 1,
            max_length: 6
          ),
        max_runs: 20
      )
    ) do
      with_database(fn conn ->
        rows =
          Enum.with_index(documents, fn embeddings, id -> %{id: id, embeddings: embeddings} end)

        {:ok, table} =
          LanceDB.Connection.create_table(conn, "docs", rows,
            schema: [id: :int64, embeddings: {:list, {:vector, 2}}]
          )

        assert {:ok, found} =
                 LanceDB.Query.multivector_search(table, query, limit: 10, columns: [:id])

        assert length(found) == length(documents)

        for row <- found do
          document = Enum.at(documents, row["id"])

          similarity =
            Enum.sum(
              Enum.map(query, fn [x, y] ->
                document |> Enum.map(fn [a, b] -> x * a + y * b end) |> Enum.max()
              end)
            )

          assert_in_delta row["_distance"], 1 - similarity, 1.0e-5
        end
      end)
    end
  end

  test "invalid vector collections and selector combinations fail before native calls" do
    for vectors <- [[], nil, [1, 2], [[]], [[1, 0], [1]], [[1, nil]], [[1, "x"]], [[1.0e100]]] do
      assert_raise ArgumentError, fn ->
        LanceDB.Query.batch_search(%LanceDB.Table{resource: make_ref()}, vectors)
      end

      assert_raise ArgumentError, fn ->
        LanceDB.Query.multivector_search(%LanceDB.Table{resource: make_ref()}, vectors)
      end
    end

    assert_raise ArgumentError, fn ->
      LanceDB.Query.multivector_search(%LanceDB.Table{resource: make_ref()}, [[1, 0]],
        distance: :l2
      )
    end

    for selectors <- [
          [vectors: [[1, 0]], multivector: [[1, 0]]],
          [vectors: [[1, 0]], vector: [1, 0]],
          [multivector: [[1, 0]], text: "hello"],
          [vectors: [[1, 0]], row_ids: [0]]
        ] do
      assert_raise ArgumentError, fn -> LanceDB.TestSupport.rows(:unused, selectors) end
      assert_raise ArgumentError, fn -> LanceDB.TestSupport.explain_plan(:unused, selectors) end
    end
  end
end
