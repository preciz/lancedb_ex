defmodule LanceDB.SearchControlsTest do
  use LanceDB.DataCase, async: true

  setup %{path: path} do
    {:ok, conn} = LanceDB.connect(path)
    %{conn: conn}
  end

  test("selects between vector columns and projects fields while retaining distance", %{
    conn: conn
  }) do
    {:ok, table} =
      LanceDB.Connection.create_table(conn, "multi", [
        %{id: 1, text: "first", title_vector: [1, 0], body_vector: [0, 1]},
        %{id: 2, text: nil, title_vector: [0, 1], body_vector: [1, 0]}
      ])

    assert {:ok, [title]} =
             LanceDB.Query.search(table, [1, 0],
               vector_column: :title_vector,
               columns: [:id],
               limit: 1
             )

    assert title == %{"id" => 1, "_distance" => 0.0}

    assert {:ok, [body]} =
             LanceDB.Query.search(table, [1, 0],
               vector_column: "body_vector",
               columns: ["id", :text, :_distance],
               limit: 1
             )

    assert body == %{"id" => 2, "text" => nil, "_distance" => 0.0}

    assert {:ok, [%{"id" => 1}]} =
             LanceDB.Query.search(table, [1, 0],
               vector_column: :body_vector,
               columns: [:id],
               filter: "id = 1"
             )

    assert {:ok, []} =
             LanceDB.Query.search(table, [1, 0],
               vector_column: :body_vector,
               columns: [:id],
               filter: "id = 3"
             )

    assert {:ok, [%{"_distance" => +0.0}]} =
             LanceDB.Query.search(table, [1, 0],
               vector_column: :title_vector,
               columns: [:_distance],
               limit: 1
             )

    assert {:ok, :created} =
             LanceDB.Index.create(table, :body_vector, type: :ivf_flat, num_partitions: 1)

    assert {:ok, [%{"id" => 2}]} =
             LanceDB.Query.search(table, [1, 0],
               vector_column: :body_vector,
               columns: [:id],
               nprobes: 1,
               limit: 1
             )

    for opts <- [
          [vector_column: :missing],
          [vector_column: :id],
          [vector_column: :body_vector, columns: [:missing]]
        ] do
      assert {:error, _} = LanceDB.Query.search(table, [1, 0], opts)
    end

    assert {:error, _} = LanceDB.Query.search(table, [1, 0, 0], vector_column: :title_vector)
  end

  test("refinement returns original-vector distances and exhaustive search bypasses PQ", %{
    conn: conn
  }) do
    rows =
      for id <- 1..300 do
        %{id: id, vector: [id / 300, :math.sin(id)]}
      end

    {:ok, table} = LanceDB.Connection.create_table(conn, "pq", rows)
    query = [0.45, 0.2]
    assert {:ok, baseline} = LanceDB.Query.search(table, query, limit: 5, columns: [:id])

    assert {:ok, :created} =
             LanceDB.Index.create(table, :vector,
               type: :ivf_pq,
               num_partitions: 2,
               num_sub_vectors: 1
             )

    assert {:ok, exact} =
             LanceDB.Query.search(table, query,
               limit: 5,
               columns: [:id],
               bypass_vector_index: true
             )

    assert exact == baseline

    assert {:ok, approximate} =
             LanceDB.Query.search(table, query, limit: 5, nprobes: 1, columns: [:id])

    assert length(approximate) == 5

    assert {:ok, refined} =
             LanceDB.Query.search(table, query,
               limit: 5,
               nprobes: 2,
               refine_factor: 60,
               columns: [:id]
             )

    assert Enum.map(refined, & &1["id"]) == Enum.map(exact, & &1["id"])

    for row <- refined do
      vector = Enum.at(rows, row["id"] - 1).vector
      distance = Enum.zip_with(vector, query, fn a, b -> (a - b) * (a - b) end) |> Enum.sum()
      assert_in_delta row["_distance"], distance, 1.0e-5
      assert Enum.sort(Map.keys(row)) == ["_distance", "id"]
    end

    assert {:ok, [_]} = LanceDB.Query.search(table, query, limit: 1, refine_factor: 1)
    assert {:ok, %{inserted: 1}} = LanceDB.Table.add(table, [%{id: 301, vector: query}])

    assert {:ok, [%{"id" => 301}]} =
             LanceDB.Query.search(table, query,
               limit: 1,
               columns: [:id],
               nprobes: 2,
               refine_factor: 2
             )

    assert {:ok, [%{"id" => 301}]} =
             LanceDB.Query.search(table, query,
               limit: 1,
               columns: [:id],
               bypass_vector_index: true,
               filter: "id > 300"
             )
  end

  test("projection and tuning also work without an index and on empty tables", %{conn: conn}) do
    {:ok, table} =
      LanceDB.Connection.create_empty_table(conn, "empty", id: :int64, embedding: {:vector, 2})

    assert {:ok, []} =
             LanceDB.Query.search(table, [1, 0],
               vector_column: :embedding,
               columns: [:id],
               nprobes: 2,
               refine_factor: 2
             )

    assert {:ok, %{inserted: 1}} = LanceDB.Table.add(table, [%{id: 1, embedding: [1, 0]}])

    for distance <- [:l2, :cosine, :dot] do
      assert {:ok, [%{"id" => 1} = result]} =
               LanceDB.Query.search(table, [1, 0],
                 vector_column: :embedding,
                 columns: [:id],
                 distance: distance,
                 bypass_vector_index: true
               )

      assert Map.keys(result) |> Enum.sort() == ["_distance", "id"]
    end

    assert {:ok, [%{"embedding" => _}]} =
             LanceDB.Query.search(table, [1, 0], vector_column: :embedding, columns: nil)
  end

  test "validates control options before native execution" do
    for opts <- [
          [typo: true],
          [columns: []],
          [columns: [:id, "id"]],
          [columns: [nil]],
          [columns: :id],
          [vector_column: ""],
          [vector_column: 1],
          [bypass_vector_index: nil],
          [nprobes: 0],
          [nprobes: nil],
          [nprobes: 1.5],
          [nprobes: 4_294_967_296],
          [refine_factor: -1],
          [refine_factor: "2"],
          [refine_factor: 4_294_967_296],
          [limit: 9_223_372_036_854_775_808],
          [limit: 9_223_372_036_854_775_807, refine_factor: 2]
        ] do
      assert_raise ArgumentError, fn ->
        LanceDB.Query.search(%LanceDB.Table{resource: make_ref()}, [1, 0], opts)
      end
    end
  end
end
