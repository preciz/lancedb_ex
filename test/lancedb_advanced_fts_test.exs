defmodule LanceDB.AdvancedFTSTest do
  alias LanceDB.Query.{Definition, Request}
  use LanceDB.DataCase, async: false
  alias LanceDB.{Connection, FTS, Index, Query, Tokenizer}

  setup %{path: path} do
    {:ok, conn} = Connection.connect(path)
    %{conn: conn}
  end

  test "custom stop words and code controls survive index persistence", %{conn: conn} do
    analyzer = [custom_stop_words: ["elixir"], remove_stop_words: true, stem: false]
    assert {:ok, [%{"text" => "beam"}]} = Tokenizer.tokenize("elixir beam", analyzer)
    {:ok, table} = Connection.create_table(conn, "words", [%{id: 1, text: "elixir beam"}])

    definition =
      Index.new(
        type: :fts,
        analyzer: analyzer,
        block_size: 256,
        format_version: :v3,
        memory_limit_mb: 8,
        num_workers: 1
      )

    assert {:ok, :created} = Index.create(table, :text, definition)
    {:ok, reopened} = Connection.open_table(conn, "words")
    assert {:ok, []} = Query.full_text_search(reopened, "elixir")
    assert {:ok, [%{"id" => 1}]} = Query.full_text_search(reopened, "beam", columns: [:id])

    assert {:ok, [%{"text" => "beam"}]} =
             Tokenizer.tokenize(reopened, "elixir beam", column: :text)

    code = [
      tokenizer: :code,
      split_identifiers: false,
      split_on_numerics: false,
      preserve_original: true,
      index_operators: false
    ]

    assert {:ok, tokens} = Tokenizer.tokenize("HTTPServer42::run", code)
    assert Enum.any?(tokens, &(&1["text"] == "httpserver42"))
    refute Enum.any?(tokens, &(&1["text"] == "::"))
    assert {:ok, :created} = Index.create(table, :text, type: :fts, replace: true, analyzer: code)
  end

  test "JSON documents and WAND controls work with reusable full-text and hybrid queries", %{
    conn: conn
  } do
    {:ok, table} =
      Connection.create_table(conn, "json", [
        %{id: 1, text: ~s({"title":"elixir beam"}), vector: [1, 0]},
        %{id: 2, text: ~s({"title":"rust"}), vector: [0, 1]}
      ])

    assert {:ok, :created} =
             Index.create(table, :text,
               type: :fts,
               analyzer: [document_tokenizer: :json, stem: false]
             )

    text = FTS.json_match("title", "elixir")

    query =
      table
      |> Query.new()
      |> Query.text(text)
      |> Query.options(wand_factor: 1.0)
      |> Query.select([:id])

    assert Request.compile(query.definition).request.wand_factor == 1.0
    assert {:ok, [%{"id" => 1}]} = Query.all(query)
    assert [%{"id" => 1}] = Enum.to_list(Query.stream(query))
    assert {:ok, _} = Query.explain(query)
    assert {:ok, _} = Query.analyze(query)

    hybrid =
      table
      |> Query.new()
      |> Query.hybrid([1, 0], text)
      |> Query.options(wand_factor: 1.5)
      |> Query.limit(1)

    assert Request.compile(hybrid.definition).request.controls.hybrid.wand_factor ==
             1.5

    assert {:ok, [%{"id" => 1}]} = Query.all(Query.select(hybrid, [:id]))
  end

  test "JSON scalar matches and invalid query input have defined results", %{conn: conn} do
    analyzer = [document_tokenizer: :json, stem: false, remove_stop_words: false]
    document = JSON.encode!(%{title: "elixir", number: 42, enabled: false, empty: nil})
    {:ok, table} = Connection.create_table(conn, "scalars", [%{id: 1, text: document}])
    {:ok, :created} = Index.create(table, :text, type: :fts, analyzer: analyzer)

    for {path, value} <- [{"title", "elixir"}, {"number", 42}, {"enabled", false}, {"empty", nil}] do
      assert {:ok, [%{"id" => 1}]} =
               Query.full_text_search(table, FTS.json_match(path, value), columns: [:id])
    end

    assert {:error, %LanceDB.Error{kind: :invalid_data}} =
             Tokenizer.tokenize("invalid query", analyzer)

    assert {:error, %LanceDB.Error{kind: :invalid_data}} =
             Tokenizer.tokenize(table, "invalid query", column: :text)

    assert {:ok, [%{"text" => "title,str,elixir"}]} =
             Tokenizer.tokenize("title,str,elixir", analyzer)

    for {path, value} <- [{"x,y", "value"}, {"x;y", "value"}, {"x", "a;b"}, {"x", []}, {"x", ""}] do
      assert_raise ArgumentError, fn -> FTS.json_match(path, value) end
    end

    assert_raise ArgumentError, fn -> FTS.json_match("title", "elixr", fuzziness: 1) end
  end

  test "analysis contains tokenizer panics for text and hybrid queries", %{conn: conn} do
    {:ok, table} =
      Connection.create_table(conn, "analysis_errors", [
        %{id: 1, text: ~s({"title":"elixir"}), vector: [1, 0]}
      ])

    {:ok, :created} =
      Index.create(table, :text, type: :fts, analyzer: [document_tokenizer: :json, stem: false])

    for text <- ["invalid query", FTS.match("invalid query")] do
      hybrid = table |> Query.new() |> Query.hybrid([1, 0], text)

      for query <- [
            table |> Query.new() |> Query.text(text),
            hybrid,
            Query.options(hybrid,
              reranker: fn _, _ -> flunk("invalid input reached reranker") end
            )
          ] do
        assert {:error, %LanceDB.Error{kind: :execution, operation: :analyze}} =
                 Query.analyze(query)
      end
    end

    # A failed analysis must leave the table usable for subsequent reads.
    query = table |> Query.new() |> Query.text(FTS.json_match("title", "elixir"))
    assert {:ok, analysis} = Query.analyze(query)
    assert analysis =~ "Output: rows=1"
    assert {:ok, [%{"id" => 1}]} = Query.all(Query.select(query, [:id]))
  end

  test "invalid analysis, build and WAND controls fail before execution" do
    for options <- [
          [document_tokenizer: :xml],
          [custom_stop_words: [1]],
          [split_identifiers: true],
          [tokenizer: :code, preserve_original: 1]
        ],
        do: assert_raise(ArgumentError, fn -> Tokenizer.tokenize("text", options) end)

    for options <- [
          [type: :btree, block_size: 128],
          [type: :fts, block_size: 64],
          [type: :fts, num_workers: 0],
          [type: :fts, memory_limit_mb: -1],
          [type: :fts, format_version: :v4],
          [type: :fts, format_version: :v2, block_size: 256],
          [type: :fts, format_version: :v1, analyzer: [tokenizer: :code]]
        ],
        do: assert_raise(ArgumentError, fn -> Index.new(options) end)

    for value <- [0, -1, "fast"],
        do:
          assert_raise(ArgumentError, fn ->
            Definition.new({:text, "text"}, wand_factor: value)
          end)

    assert_raise ArgumentError, fn -> Definition.new(:scan, wand_factor: 1) end
  end
end
