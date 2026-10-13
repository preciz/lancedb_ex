defmodule LanceDB.TokenizerTest do
  use LanceDB.DataCase, async: true

  setup %{path: path} do
    {:ok, conn} = LanceDB.connect(path)

    {:ok, table} =
      LanceDB.Connection.create_table(conn, "docs", [%{id: 1, text: "Running in cafés"}])

    %{conn: conn, table: table, path: path}
  end

  test "explicit analyzers expose normalized tokens and position gaps without a table" do
    assert {:ok, [%{"text" => "run", "position" => 0}, %{"text" => "cafe", "position" => 2}]} =
             LanceDB.Tokenizer.tokenize("Running in cafés")

    assert {:ok, []} = LanceDB.Tokenizer.tokenize("")
    assert {:ok, []} = LanceDB.Tokenizer.tokenize("the and in")

    assert {:ok, [%{"text" => "run", "position" => 0}, %{"text" => "cafe", "position" => 2}]} =
             LanceDB.Tokenizer.tokenize("the Running in cafés")

    assert {:ok, [%{"text" => "MiXeD café", "position" => 0}]} =
             LanceDB.Tokenizer.tokenize("MiXeD café",
               tokenizer: :raw,
               lower_case: false,
               stem: false,
               remove_stop_words: false,
               ascii_folding: false
             )

    assert {:ok, []} = LanceDB.Tokenizer.tokenize("longword", max_token_length: 3)

    assert {:ok, [%{"text" => "longword"}]} =
             LanceDB.Tokenizer.tokenize("longword", max_token_length: nil)

    assert {:ok, tokens} =
             LanceDB.Tokenizer.tokenize("coding",
               tokenizer: :ngram,
               ngram_min_length: 2,
               ngram_max_length: 3,
               ngram_prefix_only: true,
               stem: false
             )

    assert Enum.map(tokens, & &1["text"]) == ["co", "cod"]
  end

  test "index and column inspection use persisted settings and respect snapshots", %{
    table: table,
    path: path
  } do
    analyzer = [lower_case: false, stem: false, remove_stop_words: false, ascii_folding: false]

    assert {:ok, :created} =
             LanceDB.Index.create(table, :text, type: :fts, name: "words", analyzer: analyzer)

    assert {:ok, expected} = LanceDB.Tokenizer.tokenize("Running in cafés", analyzer)

    assert {:ok, ^expected} =
             LanceDB.Tokenizer.tokenize(table, "Running in cafés", index: "words")

    assert {:ok, ^expected} = LanceDB.Tokenizer.tokenize(table, "Running in cafés", column: :text)
    assert {:ok, []} = LanceDB.Tokenizer.tokenize(table, "", column: :text)
    assert {:ok, saved} = LanceDB.History.version(table)

    assert {:ok, :created} =
             LanceDB.Index.create(table, :text, type: :fts, name: "words", replace: true)

    assert {:ok, normalized} =
             LanceDB.Tokenizer.tokenize(table, "Running in cafés", index: :words)

    refute normalized == expected
    assert {:ok, table} = LanceDB.History.snapshot(table, saved)
    assert {:ok, ^expected} = LanceDB.Tokenizer.tokenize(table, "Running in cafés", index: :words)
    assert {:ok, %LanceDB.Table{}} = LanceDB.History.latest(table)
    {:ok, other} = LanceDB.connect(path)
    {:ok, reopened} = LanceDB.Connection.open_table(other, "docs")

    assert {:ok, ^normalized} =
             LanceDB.Tokenizer.tokenize(reopened, "Running in cafés", column: "text")
  end

  test "n-gram defaults preserve terms while explicit filters still apply" do
    opts = [tokenizer: :ngram, ngram_min_length: 7, ngram_max_length: 7]
    assert {:ok, [%{"text" => "running"}]} = LanceDB.Tokenizer.tokenize("running", opts)

    assert {:ok, [%{"text" => "run"}]} =
             LanceDB.Tokenizer.tokenize("running", Keyword.put(opts, :stem, true))

    assert {:ok, [%{"text" => "the"}]} = LanceDB.Tokenizer.tokenize("the", tokenizer: :ngram)

    assert {:ok, []} =
             LanceDB.Tokenizer.tokenize("the", tokenizer: :ngram, remove_stop_words: true)
  end

  test "missing and non-FTS indexes return errors", %{table: table} do
    assert {:error, _} = LanceDB.Tokenizer.tokenize(table, "word", column: :text)
    assert {:error, _} = LanceDB.Tokenizer.tokenize(table, "word", index: "missing")
    assert {:error, _} = LanceDB.Tokenizer.tokenize(table, "word", column: :missing)
    assert {:ok, :created} = LanceDB.Index.create(table, :id, type: :btree, name: "ids")
    assert {:error, _} = LanceDB.Tokenizer.tokenize(table, "word", index: "ids")
    assert {:error, _} = LanceDB.Tokenizer.tokenize("word", language: "not-a-language")
  end

  test "invalid text, selectors and analyzer settings raise before native execution" do
    for text <- [nil, 1, <<255>>] do
      assert_raise ArgumentError, fn -> LanceDB.Tokenizer.tokenize(text) end

      assert_raise ArgumentError, fn ->
        LanceDB.Tokenizer.tokenize(:unused, text, index: "words")
      end
    end

    for opts <- [[], [index: ""], [column: nil], [index: "x", column: :text], [unknown: :text]] do
      assert_raise ArgumentError, fn -> LanceDB.Tokenizer.tokenize(:unused, "word", opts) end
    end

    assert_raise ArgumentError, fn -> LanceDB.Tokenizer.tokenize("word", tokenizer: :unknown) end
    assert_raise ArgumentError, fn -> LanceDB.Tokenizer.tokenize("word", %{}) end
  end

  test "saved analyzer selectors reject ambiguity and resolve quoted nested fields", %{
    conn: conn,
    table: table
  } do
    for name <- ["first", "second"] do
      assert {:ok, :created} =
               LanceDB.Index.create(table, :text, type: :fts, name: name)
    end

    assert {:error, %LanceDB.Error{kind: :invalid_data, message: message}} =
             LanceDB.Tokenizer.tokenize(table, "Running", column: :text)

    assert message =~ "ambiguous"

    assert {:ok, [%{"text" => "run"}]} =
             LanceDB.Tokenizer.tokenize(table, "Running", index: "first")

    {:ok, nested} =
      LanceDB.Connection.create_table(conn, "nested", [%{info: %{"Mixed.Text" => "Running"}}],
        schema: [info: {:struct, [{"Mixed.Text", :string}]}]
      )

    assert {:ok, :created} =
             LanceDB.Index.create(nested, "info.`Mixed.Text`", type: :fts)

    assert {:ok, [%{"text" => "run"}]} =
             LanceDB.Tokenizer.tokenize(nested, "Running", column: "INFO.`mixed.text`")
  end
end
