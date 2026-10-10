defmodule LanceDB.StructuredFTSTest do
  use LanceDB.DataCase, async: true
  alias LanceDB.FTS

  setup %{path: path} do
    {:ok, conn} = LanceDB.connect(path)

    {:ok, table} =
      LanceDB.Connection.create_table(conn, "docs", [
        %{id: 1, text: "quick brown fox", title: "animal story", vector: [1, 0]},
        %{id: 2, text: "quick red brown fox", title: "quick fox", vector: [0, 1]},
        %{id: 3, text: "slow brown dog", title: "quick", vector: [0.5, 0.5]},
        %{id: 4, text: "fox quick brown", title: "garden", vector: [-1, 0]}
      ])

    analyzer = [with_position: true, stem: false, remove_stop_words: false]

    assert {:ok, :created} =
             LanceDB.Index.create(table, :text, type: :fts, name: "words", analyzer: analyzer)

    %{table: table, conn: conn, analyzer: analyzer}
  end

  test("match operator and fuzzy controls survive implicit single-column selection", %{
    table: table
  }) do
    assert ids(table, FTS.match("quick dog", operator: :and)) == []
    assert ids(table, FTS.match("quick dog", operator: :or)) == [1, 2, 3, 4]
    assert ids(table, FTS.match("quik")) == []
    assert ids(table, FTS.match("quik", fuzziness: 1)) == [1, 2, 4]

    assert ids(table, FTS.match("quik", fuzziness: 1, max_expansions: 10, prefix_length: 3)) == [
             1,
             2,
             4
           ]

    assert ids(table, FTS.match("quik", fuzziness: 1, prefix_length: 4)) == []
    assert ids(table, FTS.match("qick", fuzziness: 2)) == [1, 2, 4]
  end

  test("phrases honor positions and slop, and expose plans", %{table: table}) do
    assert ids(table, FTS.phrase("quick brown")) == [1, 4]
    assert ids(table, FTS.phrase("quick brown", slop: 1)) == [1, 2, 4]

    assert {:ok, plan} =
             LanceDB.TestSupport.explain_plan(table,
               text: FTS.phrase("quick brown"),
               verbose: true
             )

    assert plan =~ "Phrase"

    assert {:ok, rows} =
             LanceDB.Query.full_text_search(table, FTS.phrase("quick brown"),
               columns: [:id],
               with_row_id: true,
               limit: 1,
               offset: 1,
               timeout_ms: 10_000
             )

    assert [%{"_score" => _, "_rowid" => _}] = rows
  end

  test("Boolean clauses support nesting, optional scoring and exclusion", %{table: table}) do
    assert ids(table, FTS.boolean(must: ["brown"], must_not: ["dog"])) == [1, 2, 4]
    assert ids(table, FTS.boolean(should: ["dog", "fox"])) == [1, 2, 3, 4]
    assert ids(table, FTS.boolean(must: ["brown"], should: ["dog"])) == [1, 2, 3, 4]

    query =
      FTS.boolean(
        must: [FTS.boolean(should: ["quick", "dog"])],
        must_not: [FTS.phrase("quick brown")]
      )

    assert ids(table, query) == [2, 3]
  end

  test("multi-match preserves AND and column boosts", %{table: table, analyzer: analyzer}) do
    assert {:ok, :created} = LanceDB.Index.create(table, :title, type: :fts, analyzer: analyzer)

    assert ids(table, FTS.multi_match("quick brown", [:text, :title], operator: :and)) == [
             1,
             2,
             4
           ]

    assert ids(table, FTS.multi_match("quick brown", [:text, :title])) == [1, 2, 3, 4]

    assert {:ok, baseline} =
             LanceDB.Query.full_text_search(table, FTS.multi_match("quick", [:text, :title]))

    assert {:ok, boosted} =
             LanceDB.Query.full_text_search(
               table,
               FTS.multi_match("quick", [:text, :title], boosts: [1, 10])
             )

    before_score = Enum.find(baseline, &(&1["id"] == 3))["_score"]
    after_score = Enum.find(boosted, &(&1["id"] == 3))["_score"]
    assert_in_delta after_score, 10 * before_score, 1.0e-5
  end

  test("explicit columns win over defaults, including negative Boolean leaves", %{
    table: table,
    analyzer: analyzer
  }) do
    assert {:ok, :created} = LanceDB.Index.create(table, :title, type: :fts, analyzer: analyzer)

    assert {:error, reason} =
             LanceDB.Query.full_text_search(table, FTS.match("quik", fuzziness: 1))

    assert reason.message =~ "text_column"
    assert ids(table, FTS.match("brown", column: :text), text_column: :title) == [1, 2, 3, 4]
    query = FTS.boolean(must: [FTS.match("brown", column: :text)], must_not: ["garden"])
    assert ids(table, query, text_column: :title) == [1, 2, 3]
    assert ids(table, FTS.phrase("quick brown"), text_column: :text) == [1, 4]

    assert {:error, _} =
             LanceDB.Query.full_text_search(table, FTS.match("quick", column: :missing))
  end

  test("boosting demotes matching negatives without excluding them", %{table: table}) do
    assert {:ok, base} = LanceDB.Query.full_text_search(table, FTS.match("brown"))
    assert {:ok, boosted} = LanceDB.Query.full_text_search(table, FTS.match("brown", boost: 2))
    assert {:ok, negatives} = LanceDB.Query.full_text_search(table, FTS.match("dog"))
    negative_scores = Map.new(negatives, &{&1["id"], &1["_score"]})

    assert {:ok, demoted} =
             LanceDB.Query.full_text_search(table, FTS.boost("brown", "dog", negative_boost: 0.1))

    assert length(demoted) == 4

    for row <- base do
      assert_in_delta Enum.find(boosted, &(&1["id"] == row["id"]))["_score"],
                      2 * row["_score"],
                      1.0e-5

      expected = row["_score"] - 0.1 * Map.get(negative_scores, row["id"], 0)
      assert_in_delta Enum.find(demoted, &(&1["id"] == row["id"]))["_score"], expected, 1.0e-5
    end

    assert ids(table, FTS.boost("brown", "dog")) == [1, 2, 3, 4]
  end

  test("structured hybrid search and plans preserve both empty-channel fallbacks", %{table: table}) do
    phrase = FTS.phrase("quick brown")

    assert {:ok, vector_only} =
             LanceDB.Query.hybrid_search(table, [1, 0], phrase, filter: "id = 3")

    assert [%{"id" => 3, "_relevance_score" => score}] = vector_only
    assert_in_delta score, 1 / 60, 1.0e-7

    assert {:ok, text_only} =
             LanceDB.Query.hybrid_search(table, [1, 0], phrase,
               distance_range: {100, nil},
               with_row_id: true
             )

    assert Enum.sort(Enum.map(text_only, & &1["id"])) == [1, 4]
    assert Enum.all?(text_only, &is_integer(&1["_rowid"]))

    assert {:ok, plan} =
             LanceDB.TestSupport.explain_plan(table, vector: [1, 0], text: phrase)

    assert plan =~ "Phrase"
    assert plan =~ "Fusion:"
  end

  test("phrases require indexed positions and unindexed appends need optimization", %{
    table: table,
    conn: conn
  }) do
    {:ok, other} =
      LanceDB.Connection.create_table(conn, "unindexed", [%{id: 1, text: "quick brown"}])

    assert ids(other, FTS.match("quick", column: :text)) == [1]

    assert {:error, _} =
             LanceDB.Query.full_text_search(other, FTS.phrase("quick brown", column: :text))

    assert {:ok, :created} =
             LanceDB.Index.create(other, :text, type: :fts, analyzer: [with_position: false])

    assert {:error, _} = LanceDB.Query.full_text_search(other, FTS.phrase("quick brown"))

    assert {:ok, %{inserted: 1}} =
             LanceDB.Table.add(table, [
               %{id: 5, text: "quick brown", title: "new", vector: [1, 1]}
             ])

    assert ids(table, FTS.phrase("quick brown")) == [1, 4]
    assert {:ok, :optimized} = LanceDB.Maintenance.optimize_indices(table)
    assert ids(table, FTS.phrase("quick brown")) == [1, 4, 5]
  end

  test "builders reject invalid structures and unsupported options" do
    for build <- [
          fn -> FTS.match("") end,
          fn -> FTS.match("x", operator: :xor) end,
          fn -> FTS.match("x", fuzziness: 3) end,
          fn -> FTS.match("x", fuzziness: nil) end,
          fn -> FTS.match("x", fuzziness: :auto) end,
          fn -> FTS.match("café", fuzziness: 1, prefix_length: 1) end,
          fn -> FTS.match("x", max_expansions: 0) end,
          fn -> FTS.match("x", prefix_length: -1) end,
          fn -> FTS.match("x", boost: 0) end,
          fn -> FTS.phrase("x", slop: 4_294_967_296) end,
          fn -> FTS.phrase("x", unknown: true) end,
          fn -> FTS.multi_match("x", []) end,
          fn -> FTS.multi_match("x", [:text], boosts: []) end,
          fn -> FTS.multi_match("x", [:text], boosts: [1.0e40]) end,
          fn -> FTS.boolean([]) end,
          fn -> FTS.boolean(must_not: ["x"]) end,
          fn -> FTS.boolean(must: "x") end,
          fn -> FTS.boolean(should: [%{}]) end,
          fn -> FTS.boost("x", "y", negative_boost: -1) end
        ] do
      assert_raise ArgumentError, build
    end
  end

  defp ids(table, query, opts \\ []) do
    assert {:ok, rows} = LanceDB.Query.full_text_search(table, query, opts)
    Enum.sort(Enum.map(rows, & &1["id"]))
  end
end
