defmodule LanceDB.FullTextTest do
  use LanceDB.DataCase, async: true

  setup %{path: path} do
    {:ok, conn} = LanceDB.connect(path)

    rows = [
      %{id: 1, text: "elixir elixir otp", title: "runtime", category: "code", vector: [0, 1]},
      %{id: 2, text: "bread sourdough", title: "elixir", category: "food", vector: [1, 0]},
      %{
        id: 3,
        text: "elixir concurrency",
        title: "processes",
        category: "code",
        vector: [0.8, 0.2]
      },
      %{id: 4, text: "gardening flowers", title: nil, category: "garden", vector: [0.4, 0.6]}
    ]

    {:ok, table} = LanceDB.Connection.create_table(conn, "docs", rows)
    %{conn: conn, table: table, path: path}
  end

  test("FTS index lifecycle, scores, projection, filtering and persistence", %{
    table: table,
    path: path
  }) do
    assert {:ok, :created} = LanceDB.Index.create(table, :text, type: :fts, name: "text_fts")
    assert {:ok, [%LanceDB.Index.Description{name: "text_fts"}]} = LanceDB.Index.list(table)
    assert {:ok, %{"num_indexed_rows" => 4}} = LanceDB.Index.stats(table, "text_fts")
    assert {:ok, results} = LanceDB.Query.full_text_search(table, "elixir", columns: [:id])
    assert Enum.sort(Enum.map(results, & &1["id"])) == [1, 3]
    assert Enum.all?(results, &(Enum.sort(Map.keys(&1)) == ["_score", "id"]))
    scores = Enum.map(results, & &1["_score"])
    assert Enum.all?(scores, &(&1 > 0))
    assert scores == Enum.sort(scores, :desc)

    assert {:ok, [_]} =
             LanceDB.Query.full_text_search(table, "elixir", text_column: "text", limit: 1)

    assert {:ok, [%{"id" => 3}]} =
             LanceDB.Query.full_text_search(table, "elixir", filter: "id = 3")

    assert {:ok, []} = LanceDB.Query.full_text_search(table, "nonexistenttoken")

    assert {:ok, []} =
             LanceDB.Query.full_text_search(table, "elixir", filter: "category = 'food'")

    assert {:ok, [%{"_score" => _}]} =
             LanceDB.Query.full_text_search(table, "elixir", columns: [:_score], limit: 1)

    assert {:error, _} = LanceDB.Index.create(table, :text, type: :fts, name: "text_fts")

    assert {:ok, :created} =
             LanceDB.Index.create(table, :text, type: :fts, name: "text_fts", replace: true)

    assert {:ok, other} = LanceDB.connect(path)
    assert {:ok, reopened} = LanceDB.Connection.open_table(other, "docs")
    assert {:ok, [_, _]} = LanceDB.Query.full_text_search(reopened, "elixir")
    assert {:ok, :dropped} = LanceDB.Index.drop(table, "text_fts")
  end

  test("searches the chosen text column and reports invalid columns", %{table: table}) do
    assert {:ok, :created} = LanceDB.Index.create(table, :text, type: :fts)
    assert {:ok, :created} = LanceDB.Index.create(table, :title, type: :fts)

    assert {:ok, [%{"id" => 2}]} =
             LanceDB.Query.full_text_search(table, "elixir", text_column: :title)

    assert {:ok, [_, _]} = LanceDB.Query.full_text_search(table, "elixir", text_column: :text)

    for opts <- [
          [text_column: :missing],
          [text_column: :id],
          [text_column: :text, columns: [:missing]],
          [filter: "id ="]
        ] do
      assert {:error, _} = LanceDB.Query.full_text_search(table, "elixir", opts)
    end

    assert {:error, _} = LanceDB.Index.create(table, :id, type: :fts)
  end

  test("hybrid scores are reciprocal rank fusion of both channels", %{table: table}) do
    assert {:ok, :created} = LanceDB.Index.create(table, :text, type: :fts)
    assert {:ok, vector} = LanceDB.Query.search(table, [1, 0], limit: 4, columns: [:id])
    assert {:ok, text} = LanceDB.Query.full_text_search(table, "elixir", limit: 4, columns: [:id])

    for k <- [60, 10.0] do
      opts = [text_column: :text, vector_column: :vector, limit: 4, columns: [:id], rrf_k: k]
      assert {:ok, hybrid} = LanceDB.Query.hybrid_search(table, [1, 0], "elixir", opts)

      expected =
        Enum.reduce([vector, text], %{}, fn rows, scores ->
          rows
          |> Enum.with_index(0)
          |> Enum.reduce(scores, fn {row, rank}, acc ->
            Map.update(acc, row["id"], 1 / (k + rank), &(&1 + 1 / (k + rank)))
          end)
        end)

      assert Enum.sort(Enum.map(hybrid, & &1["id"])) == [1, 2, 3, 4]

      for row <- hybrid do
        assert Enum.sort(Map.keys(row)) == ["_relevance_score", "id"]
        assert_in_delta row["_relevance_score"], expected[row["id"]], 1.0e-6
      end

      scores = Enum.map(hybrid, & &1["_relevance_score"])
      assert scores == Enum.sort(scores, :desc)
    end

    assert {:ok, [_]} = LanceDB.Query.hybrid_search(table, [1, 0], "elixir", limit: 1)

    assert {:ok, [_, _]} =
             LanceDB.Query.hybrid_search(table, [1, 0], "elixir", filter: "category = 'code'")

    assert {:ok, []} = LanceDB.Query.hybrid_search(table, [1, 0], "elixir", filter: "id = 99")

    assert {:ok, results} =
             LanceDB.Query.hybrid_search(table, [1, 0], "nonexistenttoken", columns: [:id])

    assert length(results) == 4

    assert {:ok, :created} =
             LanceDB.Index.create(table, :vector, type: :ivf_flat, num_partitions: 1)

    assert {:ok, [_ | _]} =
             LanceDB.Query.hybrid_search(table, [1, 0], "elixir",
               nprobes: 1,
               refine_factor: 2,
               columns: [:id]
             )

    assert {:ok, [_ | _]} =
             LanceDB.Query.hybrid_search(table, [1, 0], "elixir", bypass_vector_index: true)
  end

  test("FTS observes writes, deletions and index maintenance", %{table: table}) do
    assert {:ok, :created} = LanceDB.Index.create(table, :text, type: :fts)
    row = %{id: 5, text: "elixir newcomer", title: nil, category: "code", vector: [1, 0]}
    assert {:ok, %{inserted: 1}} = LanceDB.Table.add(table, [row])
    assert {:ok, [%{"id" => 5}]} = LanceDB.Query.full_text_search(table, "newcomer")

    assert {:ok, %{updated: 1}} =
             LanceDB.Table.update(table, %{text: "erlang newcomer"}, filter: "id = 5")

    assert {:ok, []} = LanceDB.Query.full_text_search(table, "elixir", filter: "id = 5")
    assert {:ok, :optimized} = LanceDB.Maintenance.optimize_indices(table)
    assert {:ok, [%{"id" => 5}]} = LanceDB.Query.full_text_search(table, "erlang")
    assert {:ok, %{operation: :delete}} = LanceDB.Table.delete(table, "id = 5")
    assert {:ok, []} = LanceDB.Query.full_text_search(table, "erlang")
    assert {:ok, _} = LanceDB.Maintenance.compact_files(table)
    assert {:ok, [_, _]} = LanceDB.Query.full_text_search(table, "elixir")
  end

  test("empty FTS tables and empty vector channels are supported", %{conn: conn}) do
    {:ok, table} =
      LanceDB.Connection.create_empty_table(conn, "empty_fts",
        id: :int64,
        text: :string,
        vector: {:vector, 2}
      )

    assert {:ok, :created} = LanceDB.Index.create(table, :text, type: :fts)
    assert {:ok, []} = LanceDB.Query.full_text_search(table, "elixir")
    assert {:ok, []} = LanceDB.Query.hybrid_search(table, [1, 0], "elixir")

    assert {:ok, %{inserted: 1}} =
             LanceDB.Table.add(table, [%{id: 1, text: "elixir", vector: nil}])

    assert {:ok, [%{"id" => 1}]} =
             LanceDB.Query.hybrid_search(table, [1, 0], "elixir", columns: [:id])

    assert {:ok, [%{"_relevance_score" => score} = row]} =
             LanceDB.Query.hybrid_search(table, [1, 0], "elixir", columns: [:_relevance_score])

    assert score > 0
    assert map_size(row) == 1
  end

  test("unindexed text scans work and invalid hybrid columns return errors", %{table: table}) do
    assert {:ok, [_, _]} = LanceDB.Query.full_text_search(table, "elixir", text_column: :text)

    assert {:ok, [_ | _]} =
             LanceDB.Query.hybrid_search(table, [1, 0], "elixir", text_column: :text)

    assert {:ok, :created} = LanceDB.Index.create(table, :text, type: :fts)
    assert {:error, _} = LanceDB.Query.hybrid_search(table, [1, 0, 0], "elixir")

    assert {:error, _} =
             LanceDB.Query.hybrid_search(table, [1, 0], "elixir", text_column: :missing)
  end

  test "validates text, hybrid and index options before native execution" do
    for text <- [nil, 1, "", "  ", <<255>>] do
      assert_raise ArgumentError, fn ->
        LanceDB.Query.full_text_search(%LanceDB.Table{resource: make_ref()}, text)
      end

      assert_raise ArgumentError, fn ->
        LanceDB.Query.hybrid_search(%LanceDB.Table{resource: make_ref()}, [1, 0], text)
      end
    end

    for opts <- [
          [limit: 0],
          [limit: 1.5],
          [limit: 9_223_372_036_854_775_808],
          [columns: []],
          [text_column: ""],
          [filter: ""],
          [typo: 1]
        ] do
      assert_raise ArgumentError, fn ->
        LanceDB.Query.full_text_search(%LanceDB.Table{resource: make_ref()}, "elixir", opts)
      end
    end

    for opts <- [
          [rrf_k: 0],
          [rrf_k: -1],
          [rrf_k: nil],
          [rrf_k: "60"],
          [rrf_k: 1.0e100],
          [rrf_k: 1.0e-100],
          [typo: 1]
        ] do
      assert_raise ArgumentError, fn ->
        LanceDB.Query.hybrid_search(%LanceDB.Table{resource: make_ref()}, [1, 0], "elixir", opts)
      end
    end

    for vector <- [[], ["invalid"]] do
      assert_raise ArgumentError, fn ->
        LanceDB.Query.hybrid_search(%LanceDB.Table{resource: make_ref()}, vector, "elixir")
      end
    end

    assert_raise ArgumentError, fn ->
      LanceDB.Index.create(:unused, :text, type: :fts, distance: :l2)
    end
  end
end
