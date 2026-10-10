defmodule LanceDB.AnalyzerTest do
  use LanceDB.DataCase, async: true

  setup %{path: path} do
    {:ok, conn} = LanceDB.connect(path)
    %{conn: conn, path: path}
  end

  test "raw case-sensitive analyzer persists and applies to full-text and hybrid search", %{
    conn: conn,
    path: path
  } do
    {:ok, table} =
      LanceDB.Connection.create_table(conn, "raw", [%{id: 1, text: "MiXeD café", vector: [1, 0]}])

    analyzer = [
      tokenizer: :raw,
      language: "English",
      lower_case: false,
      stem: false,
      remove_stop_words: false,
      ascii_folding: false,
      with_position: true,
      max_token_length: nil
    ]

    assert {:ok, :created} = LanceDB.Index.create(table, :text, type: :fts, analyzer: analyzer)
    assert {:ok, [%{"id" => 1}]} = LanceDB.Query.full_text_search(table, "MiXeD café")
    assert {:ok, []} = LanceDB.Query.full_text_search(table, "mixed cafe")

    assert {:ok, [%{"_relevance_score" => score}]} =
             LanceDB.Query.hybrid_search(table, [1, 0], "MiXeD café")

    assert_in_delta score, 2 / 60, 1.0e-6
    {:ok, other} = LanceDB.connect(path)
    {:ok, reopened} = LanceDB.Connection.open_table(other, "raw")
    assert {:ok, [%{"id" => 1}]} = LanceDB.Query.full_text_search(reopened, "MiXeD café")
  end

  test "stemming and stop words are configurable, with replacement required", %{conn: conn} do
    {:ok, table} =
      LanceDB.Connection.create_table(conn, "stem", [%{id: 1, text: "the running café"}])

    assert {:ok, :created} =
             LanceDB.Index.create(table, :text,
               type: :fts,
               name: "words",
               analyzer: [
                 tokenizer: :simple,
                 language: "English",
                 stem: true,
                 remove_stop_words: true,
                 lower_case: true,
                 ascii_folding: true
               ]
             )

    assert {:ok, [_]} = LanceDB.Query.full_text_search(table, "run")
    assert {:ok, [_]} = LanceDB.Query.full_text_search(table, "CAFE")
    assert {:ok, []} = LanceDB.Query.full_text_search(table, "the")

    assert {:error, _} =
             LanceDB.Index.create(table, :text,
               type: :fts,
               name: "words",
               analyzer: [stem: false]
             )

    assert {:ok, :created} =
             LanceDB.Index.create(table, :text,
               type: :fts,
               name: "words",
               replace: true,
               analyzer: [stem: false, remove_stop_words: false]
             )

    assert {:ok, []} = LanceDB.Query.full_text_search(table, "run")
    assert {:ok, [_]} = LanceDB.Query.full_text_search(table, "the")

    assert {:error, _} =
             LanceDB.Index.create(table, :text,
               type: :fts,
               analyzer: [language: "not-a-language"]
             )
  end

  test "token length limits and supported tokenizers", %{conn: conn} do
    {:ok, table} = LanceDB.Connection.create_table(conn, "tokens", [%{id: 1, text: "coding"}])

    assert {:ok, :created} =
             LanceDB.Index.create(table, :text,
               type: :fts,
               name: "words",
               analyzer: [max_token_length: 3]
             )

    assert {:ok, []} = LanceDB.Query.full_text_search(table, "coding")

    assert {:ok, :created} =
             LanceDB.Index.create(table, :text,
               type: :fts,
               name: "words",
               replace: true,
               analyzer: [
                 tokenizer: :ngram,
                 ngram_min_length: 2,
                 ngram_max_length: 3,
                 ngram_prefix_only: true,
                 stem: false,
                 remove_stop_words: false
               ]
             )

    assert {:ok, [_]} = LanceDB.Query.full_text_search(table, "cod")

    for tokenizer <- [:whitespace, :code] do
      assert {:ok, :created} =
               LanceDB.Index.create(table, :text,
                 type: :fts,
                 name: "words",
                 replace: true,
                 analyzer: [tokenizer: tokenizer]
               )

      assert {:ok, [_]} = LanceDB.Query.full_text_search(table, "coding")
    end
  end

  test "analyzer options reject invalid combinations before native execution" do
    for analyzer <- [
          nil,
          %{},
          [typo: 1],
          [tokenizer: :unknown],
          [lower_case: nil],
          [stem: 1],
          [language: ""],
          [language: :english],
          [max_token_length: 0],
          [ngram_min_length: 0],
          [ngram_min_length: 2],
          [tokenizer: :ngram, ngram_min_length: 4],
          [tokenizer: :ngram, with_position: true],
          [tokenizer: :ngram, ngram_max_length: 4_294_967_296]
        ] do
      assert_raise ArgumentError, fn ->
        LanceDB.Index.create(:unused, :text, type: :fts, analyzer: analyzer)
      end
    end

    assert_raise ArgumentError, fn ->
      LanceDB.Index.create(:unused, :id, type: :btree, analyzer: [])
    end
  end
end
