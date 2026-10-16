defmodule LanceDB.ScoringProjectionTest do
  use LanceDB.DataCase, async: true
  @moduletag :native_operation_contract

  alias LanceDB.{Batch, Connection, Index, Query}

  setup %{path: path} do
    {:ok, conn} = LanceDB.connect(path)

    {:ok, table} =
      Connection.create_table(conn, "docs", [
        %{id: 1, text: "needle needle needle", vector: [0, 0]},
        %{id: 2, text: "needle code", vector: [1, 0]},
        %{id: 3, text: "needle language programming", vector: [4, 0]},
        %{id: 4, text: "unrelated", vector: [9, 0]}
      ])

    %{conn: conn, table: table}
  end

  test "automatic scores default on and can be disabled for every projection shape", %{
    table: table
  } do
    for indexed <- [false, true] do
      if indexed do
        assert {:ok, :created} = Index.create(table, :text, type: :fts)

        assert {:ok, :created} =
                 Index.create(table, :vector, type: :ivf_flat, num_partitions: 1)
      end

      for {query, score} <- searches(table),
          projection <- [[], [columns: [:id]], [select: [item: "id", constant: "42"]]] do
        query = Query.options(query, projection)
        assert {:ok, [_ | _] = baseline} = Query.all(query)
        assert Enum.all?(baseline, &Map.has_key?(&1, score))

        assert {:ok, ^baseline} =
                 query |> Query.options(disable_scoring_autoprojection: false) |> Query.all()

        hidden = Query.options(query, disable_scoring_autoprojection: true)
        expected = Enum.map(baseline, &Map.delete(&1, score))
        assert {:ok, ^expected} = Query.all(hidden)
        assert_output(hidden, expected)
      end
    end
  end

  test "explicit score columns survive without automatic scores or duplicate metadata", %{
    table: table
  } do
    for {query, score} <- searches(table), columns <- [[:id, score], [score]] do
      query = Query.select(query, columns)
      assert {:ok, baseline} = Query.all(query)
      explicit = Query.options(query, disable_scoring_autoprojection: true)
      assert {:ok, ^baseline} = Query.all(explicit)

      names = Enum.map(columns, &to_string/1) |> Enum.sort()
      assert Enum.all?(baseline, &(Enum.sort(Map.keys(&1)) == names))

      assert_output(explicit, baseline)
    end
  end

  test "standalone score expressions can be retained under ordinary aliases", %{table: table} do
    for {query, score} <- Enum.take(searches(table), 2) do
      assert {:ok, baseline} = Query.all(query)

      aliased =
        query
        |> Query.select_as(item: "id", metric: "#{score} + 1")
        |> Query.options(disable_scoring_autoprojection: true)

      assert {:ok, rows} = Query.all(aliased)
      assert Enum.map(rows, & &1["item"]) == Enum.map(baseline, & &1["id"])

      for {row, original} <- Enum.zip(rows, baseline) do
        assert Enum.sort(Map.keys(row)) == ["item", "metric"]
        assert_in_delta row["metric"], original[score] + 1, 1.0e-6
      end

      assert_output(aliased, rows)
    end
  end

  test "ordering, pagination and row-ID selection work without automatic scores", %{
    table: table
  } do
    for {query, score} <- searches(table), projection <- [[], [columns: [:id]]] do
      ordering = if score == "_relevance_score", do: score, else: :id

      query =
        query
        |> Query.options(projection)
        |> Query.options(order_by: [desc: ordering], limit: 2, offset: 1, with_row_id: true)

      assert {:ok, baseline} = Query.all(query)
      hidden = Query.options(query, disable_scoring_autoprojection: true)
      expected = Enum.map(baseline, &Map.delete(&1, score))
      assert {:ok, ^expected} = Query.all(hidden)
      assert Enum.all?(expected, &is_integer(&1["_rowid"]))
      assert_output(hidden, expected)
      assert {:ok, _} = Query.explain(hidden)
      assert {:ok, analysis} = Query.analyze(hidden)
      assert analysis =~ "Output: rows=2"
    end
  end

  test "custom rerankers still receive channel scores and control the final ordering", %{
    table: table
  } do
    parent = self()

    callback = fn vector, text ->
      send(parent, {:candidates, vector, text})

      (vector ++ text)
      |> Enum.uniq_by(& &1["_rowid"])
      |> Enum.reverse()
      |> Enum.with_index()
      |> Enum.map(fn {row, index} -> %{_rowid: row["_rowid"], _relevance_score: index + 1} end)
    end

    for normalize <- [:score, :rank],
        projection <- [[], [columns: [:id]], [select: [item: "id"]]],
        channels <- [[], [distance_range: {1000, nil}], [filter: "id < 0"]] do
      query =
        Query.new(
          table,
          {:hybrid, [0, 0], "needle"},
          [reranker: callback, normalize: normalize, text_column: :text, limit: 3] ++
            projection ++ channels
        )

      assert {:ok, baseline} = Query.all(query)
      hidden = Query.options(query, disable_scoring_autoprojection: true)
      expected = Enum.map(baseline, &Map.delete(&1, "_relevance_score"))
      assert {:ok, ^expected} = Query.all(hidden)
      assert_output(hidden, expected)
      assert {:ok, analysis} = Query.analyze(hidden)
      assert analysis =~ "custom Elixir callback"
      assert analysis =~ "Output: rows=#{length(expected)}"
    end

    assert_receive {:candidates, [_ | _] = vector, [_ | _] = text}
    assert Enum.all?(vector, &(is_number(&1["_distance"]) and is_integer(&1["_rowid"])))
    assert Enum.all?(text, &(is_number(&1["_score"]) and is_integer(&1["_rowid"])))

    query = Query.new(table, {:hybrid, [0, 0], "needle"}, reranker: callback, text_column: :text)

    explicit =
      Query.options(query,
        columns: [:id, :_relevance_score],
        disable_scoring_autoprojection: true
      )

    assert {:ok, [_ | _] = rows} = Query.all(explicit)
    assert Enum.all?(rows, &(Enum.sort(Map.keys(&1)) == ["_relevance_score", "id"]))
    assert_output(explicit, rows)
  end

  test "empty results have the same score-free output schema as nonempty results", %{table: table} do
    for {query, _score} <- searches(table) do
      hidden = Query.options(query, columns: [:id], disable_scoring_autoprojection: true)
      empty = Query.where(hidden, "id < 0")
      assert {:ok, schema} = Query.schema(hidden)
      assert Enum.map(schema.fields, & &1.name) == ["id"]
      assert {:ok, ^schema} = Query.schema(empty)
      assert {:ok, []} = Query.all(empty)
      assert_output(empty, [])
    end
  end

  test "batch searches retain query_index across typed, singleton and indexed plans", %{
    conn: conn
  } do
    for type <- [:float16, :float32, :float64, :uint8] do
      {:ok, table} =
        Connection.create_table(
          conn,
          to_string(type),
          [%{id: 1, vector: [1, 2]}, %{id: 2, vector: [3, 4]}],
          schema: [id: :int64, vector: {:vector, 2, type}]
        )

      distance = if type == :uint8, do: :hamming, else: :l2

      for indexed <- [false, true] do
        if indexed do
          assert {:ok, :created} =
                   Index.create(table, :vector,
                     type: :ivf_flat,
                     distance: distance,
                     num_partitions: 1
                   )
        end

        for vectors <- [[[1, 2]], [[1, 2], [3, 4]]] do
          query =
            Query.new(table, {:batch, vectors}, distance: distance, limit: 1, columns: [:id])

          assert {:ok, baseline} = Query.all(query)
          hidden = Query.options(query, disable_scoring_autoprojection: true)
          expected = Enum.map(baseline, &Map.delete(&1, "_distance"))
          assert {:ok, rows} = Query.all(hidden)
          assert Enum.sort(rows) == Enum.sort(expected)

          expected_ids =
            Map.new(Enum.with_index(vectors), fn {_, index} -> {index, index + 1} end)

          assert Map.new(rows, &{&1["query_index"], &1["id"]}) == expected_ids

          assert_output(hidden, expected, false)
        end
      end
    end
  end

  test "multivector results can omit or explicitly retain distance", %{conn: conn} do
    {:ok, table} =
      Connection.create_table(
        conn,
        "multivector",
        [
          %{id: 1, embeddings: [[1, 0], [0, 1]]},
          %{id: 2, embeddings: [[1, 0]]},
          %{id: 3, embeddings: [[-1, 0], [0, -1]]}
        ],
        schema: [id: :int64, embeddings: {:list, {:vector, 2}}]
      )

    for vectors <- [[[1, 0]], [[1, 0], [0, 1]]] do
      query = Query.new(table, {:multivector, vectors}, limit: 3, columns: [:id])
      assert {:ok, baseline} = Query.all(query)
      hidden = Query.options(query, disable_scoring_autoprojection: true)
      expected = Enum.map(baseline, &Map.delete(&1, "_distance"))
      assert {:ok, ^expected} = Query.all(hidden)
      assert_output(hidden, expected)
      explicit = Query.select(hidden, [:id, :_distance])
      assert {:ok, ^baseline} = Query.all(explicit)
    end
  end

  test "scans and direct retrieval retain row IDs and stored columns", %{table: table} do
    query = Query.new(table, columns: [:id], with_row_id: true)
    assert {:ok, baseline} = Query.all(query)
    hidden = Query.options(query, disable_scoring_autoprojection: true)
    assert {:ok, ^baseline} = Query.all(hidden)
    assert_output(hidden, baseline)

    for take <- [
          Query.row_ids(hidden, Enum.map(baseline, & &1["_rowid"])),
          Query.row_offsets(hidden, [0, 1, 2, 3])
        ] do
      assert {:ok, rows} = Query.all(take)
      assert Enum.sort(rows) == Enum.sort(baseline)
      assert_output(take, baseline, false)
    end

    assert {:ok, ^baseline} =
             Query.query(table,
               columns: [:id],
               with_row_id: true,
               disable_scoring_autoprojection: true
             )
  end

  defp searches(table) do
    [
      {Query.new(table, {:vector, [0, 0]}, limit: 3), "_distance"},
      {Query.new(table, {:text, "needle"}, text_column: :text, limit: 3), "_score"},
      {Query.new(table, {:hybrid, [0, 0], "needle"}, text_column: :text, limit: 3),
       "_relevance_score"}
    ]
  end

  defp assert_output(query, expected, ordered? \\ true) do
    normalize = fn rows -> if ordered?, do: rows, else: Enum.sort(rows) end

    assert query |> Query.stream(batch_size: 1) |> Enum.to_list() |> normalize.() ==
             normalize.(expected)

    assert {:ok, schema} = Query.schema(query)

    batches = query |> Query.stream_batches(batch_size: 1) |> Enum.to_list()
    for batch <- batches, do: assert({:ok, ^schema} = Batch.schema(batch))

    rows =
      Enum.flat_map(batches, fn batch ->
        {:ok, rows} = Batch.to_rows(batch)
        rows
      end)

    assert normalize.(rows) == normalize.(expected)
    names = Enum.map(schema.fields, & &1.name) |> Enum.sort()
    assert Enum.all?(expected, &(Enum.sort(Map.keys(&1)) == names))
  end
end
