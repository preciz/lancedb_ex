defmodule LanceDB.PreparedFTSTest do
  use LanceDB.DataCase, async: true
  use ExUnitProperties
  @moduletag :fts_preparation
  alias LanceDB.{Connection, Error, FTS, History, Index, Query, Table}
  alias LanceDB.FTS.Prepared

  @plain [stem: false, remove_stop_words: false]

  setup %{path: path} do
    {:ok, conn} = LanceDB.connect(path)
    %{conn: conn}
  end

  test "saved analysis fixes uppercase, accent and stemming fuzzy mismatches", %{conn: conn} do
    german = [language: "German", stem: true, remove_stop_words: false]
    table = indexed(conn, "german", ["Intersport Nürnberg"], german)
    assert ids(table, FTS.match("INTERSPORT", fuzziness: 1)) == []

    for text <- ["INTERSPORT", "INTERSPRT", "NÜRNBRG", "INTERSPRT NÜRNBRG"] do
      assert {:ok, prepared} = FTS.prepare(table, text, column: :text)
      assert ids(table, FTS.match(prepared, operator: :and, fuzziness: 1)) == [1]
    end

    table = indexed(conn, "english", ["running", "jumping"], language: "English", stem: true)
    assert ids(table, FTS.match("running", fuzziness: 1)) == []
    assert {:ok, prepared} = FTS.prepare("running", analyzer: [language: "English", stem: true])
    assert Prepared.terms(prepared) == [%{analyzed: "run", originals: ["running"]}]
    assert ids(table, FTS.match(prepared, fuzziness: 1)) == [1]
  end

  test "original spellings survive filtering and normalized terms are deduplicated" do
    assert {:ok, prepared} =
             FTS.prepare("skip BERLIN, Berlin berlin skip CAFÉ!",
               analyzer: [stem: false, custom_stop_words: ["skip"]],
               max_terms: 2
             )

    assert FTS.term_count(prepared) == 2

    assert Prepared.terms(prepared) == [
             %{analyzed: "berlin", originals: ["BERLIN", "Berlin", "berlin"]},
             %{analyzed: "cafe", originals: ["CAFÉ"]}
           ]

    assert_raise ArgumentError, ~r/max_terms/, fn ->
      FTS.prepare("BERLIN Berlin berlin Paris", analyzer: @plain, max_terms: 1)
    end
  end

  property "source terms stay paired after punctuation and stop-word removal" do
    normalized = %{
      "CAFÉ" => "cafe",
      "Über" => "uber",
      "Alpha" => "alpha",
      "BERLIN" => "berlin",
      "12345" => "12345"
    }

    check all(
            words <- list_of(member_of(["skip" | Map.keys(normalized)]), max_length: 40),
            separator <- member_of([" ", ",", "—", "\n", "..."]),
            max_runs: 40
          ) do
      assert {:ok, prepared} =
               FTS.prepare(Enum.join(words, separator),
                 analyzer: [stem: false, custom_stop_words: ["skip"]]
               )

      retained = Enum.reject(words, &(&1 == "skip"))

      expected =
        retained
        |> Enum.uniq_by(&Map.fetch!(normalized, &1))
        |> Enum.map(fn original ->
          analyzed = Map.fetch!(normalized, original)

          originals =
            Enum.filter(retained, &(Map.fetch!(normalized, &1) == analyzed)) |> Enum.uniq()

          %{analyzed: analyzed, originals: originals}
        end)

      assert Prepared.terms(prepared) == expected
      assert FTS.term_count(prepared) == length(expected)
    end
  end

  test "empty analysis is inspectable and cannot build a positive query" do
    for text <- ["", " \n ", "...—😀", "the and in"] do
      assert {:ok, prepared} = FTS.prepare(text)
      assert FTS.term_count(prepared) == 0
      assert Prepared.terms(prepared) == []
      assert_raise ArgumentError, ~r/empty/, fn -> FTS.match(prepared) end
    end

    assert {:ok, prepared} = FTS.prepare("longword", analyzer: [max_token_length: 3])
    assert FTS.term_count(prepared) == 0
  end

  test "byte limits and distinct-term limits have inclusive boundaries and can be disabled" do
    assert {:ok, _} = FTS.prepare("café", max_bytes: 5)
    assert_raise ArgumentError, ~r/max_bytes/, fn -> FTS.prepare("café", max_bytes: 4) end
    long = String.duplicate("x", 2049)
    assert_raise ArgumentError, ~r/max_bytes/, fn -> FTS.prepare(long) end

    assert_raise ArgumentError, ~r/max_bytes/, fn ->
      FTS.prepare(:binary.copy(<<255>>, 2049))
    end

    assert {:ok, prepared} =
             FTS.prepare(long, analyzer: [max_token_length: nil], max_bytes: nil)

    assert FTS.term_count(prepared) == 1
    text = Enum.map_join(1..33, " ", &"word#{&1}")
    assert_raise ArgumentError, ~r/max_terms/, fn -> FTS.prepare(text) end
    assert {:ok, prepared} = FTS.prepare(text, max_terms: nil)
    assert FTS.term_count(prepared) == 33
    assert {:ok, prepared} = FTS.prepare(text, max_terms: 33)
    assert FTS.term_count(prepared) == 33
  end

  test "one preparation builds AND and OR with alternatives for each term", %{conn: conn} do
    table =
      indexed(
        conn,
        "terms",
        ["alpha bravo", "alphx bravo", "alpha bravx", "alphx bravx", "alpha", "bravo"],
        @plain
      )

    assert {:ok, prepared} = FTS.prepare(table, "alpha bravo", index: "words")
    both = FTS.match(prepared, operator: :and, fuzziness: 1)
    either = FTS.match(prepared, operator: :or, fuzziness: 1)
    assert ids(table, both) == [1, 2, 3, 4]
    assert ids(table, either) == [1, 2, 3, 4, 5, 6]
    assert ids(table, FTS.boolean(must: [both], must_not: ["alphx"])) == [1, 3]
    assert {:ok, rows} = table |> Query.new() |> Query.text(both) |> Query.all()
    assert Enum.sort(Enum.map(rows, & &1["id"])) == [1, 2, 3, 4]
  end

  test "length thresholds choose zero, one and two edits on analyzed graphemes", %{conn: conn} do
    for {word, one, two} <- [
          {"abcd", "abxd", "abxy"},
          {"abcde", "abcdx", "abcxy"},
          {"abcdefgh", "abcdefgx", "abcdefxx"},
          {"abcdefghi", "abcdefghx", "abcdefgxx"},
          {"éabcd", "éabcx", "éabxx"},
          {"काअबक", "काअबख", "काअगख"}
        ] do
      name = "boundary_#{System.unique_integer([:positive])}"
      table = indexed(conn, name, [word, one, two], @plain ++ [ascii_folding: false])
      assert {:ok, prepared} = FTS.prepare(table, word, column: :text)

      expected =
        cond do
          String.length(word) < 5 -> [1]
          String.length(word) < 9 -> [1, 2]
          true -> [1, 2, 3]
        end

      assert ids(table, FTS.match(prepared, fuzziness: {:length, 5, 9})) == expected
      assert ids(table, FTS.match(prepared, fuzziness: 0)) == [1]
      assert ids(table, FTS.match(prepared, fuzziness: 1)) == [1, 2]
      assert ids(table, FTS.match(prepared, fuzziness: 2)) == [1, 2, 3]

      if word == "éabcd",
        do: assert(ids(table, FTS.match(prepared, fuzziness: {:length, 6, 9})) == [1])
    end

    table = indexed(conn, "normalized_length", ["running", "ruining"], stem: true)
    assert {:ok, prepared} = FTS.prepare(table, "running", column: :text)
    assert ids(table, FTS.match(prepared, fuzziness: {:length, 5, 9})) == [1]
  end

  test "numeric tokens next to punctuation stay exact unless explicitly enabled", %{conn: conn} do
    table = indexed(conn, "numbers", ["12345 alpha", "12346 alpha", "12345 bravo"], @plain)
    assert {:ok, prepared} = FTS.prepare(table, "(12345), ALPHA!", column: :text)
    opts = [operator: :and, fuzziness: {:length, 2, 9}]
    assert ids(table, FTS.match(prepared, opts)) == [1]
    assert ids(table, FTS.match(prepared, opts ++ [exact_numbers: false])) == [1, 2]
    assert ids(table, FTS.match(prepared)) == [1, 2, 3]
  end

  test "exact boosts express preferences and compose with existing boosts", %{conn: conn} do
    table = indexed(conn, "ranking", ["cobalt", "cobolt"], @plain)
    assert {:ok, prepared} = FTS.prepare(table, "cobalt", column: :text)
    opts = [fuzziness: 1, exact_boost: 5]

    assert {:ok, [%{"id" => 1}, %{"id" => 2}]} =
             Query.full_text_search(table, FTS.match(prepared, opts))

    assert {:ok, base} = Query.full_text_search(table, FTS.match(prepared, opts))
    assert {:ok, boosted} = Query.full_text_search(table, FTS.match(prepared, opts ++ [boost: 2]))

    for row <- base do
      assert_in_delta Enum.find(boosted, &(&1["id"] == row["id"]))["_score"],
                      2 * row["_score"],
                      1.0e-5
    end

    assert {:ok, repeated} = FTS.prepare(table, "cobalt COBALT cobalt", column: :text)
    assert Query.full_text_search(table, FTS.match(repeated, opts)) == {:ok, base}
  end

  test "fuzzy prefix and expansion options apply to analyzed terms", %{conn: conn} do
    table = indexed(conn, "prefix", ["quick"], @plain)
    assert {:ok, prepared} = FTS.prepare(table, "QUIK", column: :text)

    assert ids(table, FTS.match(prepared, fuzziness: 1, prefix_length: 3, max_expansions: 1)) == [
             1
           ]

    assert ids(table, FTS.match(prepared, fuzziness: 1, prefix_length: 4)) == []
  end

  test "saved preparation enforces the same limits and preserves Unicode prefixes", %{conn: conn} do
    table =
      indexed(conn, "unicode_prefix", ["éclair", "eclair"], @plain ++ [ascii_folding: false])

    assert {:ok, prepared} =
             FTS.prepare(table, "ÉCLAI", column: :text, max_bytes: 6, max_terms: 1)

    assert ids(table, FTS.match(prepared, fuzziness: 1, prefix_length: 1)) == [1]
    assert ids(table, FTS.match(prepared, fuzziness: 2, prefix_length: 1)) == [1]
    assert ids(table, FTS.match(prepared, fuzziness: 2, prefix_length: 0)) == [1, 2]

    assert_raise ArgumentError, ~r/max_bytes/, fn ->
      FTS.prepare(table, "ÉCLAI", column: :text, max_bytes: 5)
    end

    assert_raise ArgumentError, ~r/max_terms/, fn ->
      FTS.prepare(table, "ÉCLAI eclair", index: "words", max_terms: 1)
    end

    assert {:ok, prepared} =
             FTS.prepare(table, "ÉCLAI eclair", index: "words", max_bytes: nil, max_terms: nil)

    assert FTS.term_count(prepared) == 2
  end

  test "saved preparation uses persisted settings, reopening and snapshots", %{
    conn: conn,
    path: path
  } do
    table = indexed(conn, "saved", ["Running cafés"], [])
    assert {:ok, before} = FTS.prepare(table, "Running cafés", index: :words)
    assert {:ok, version} = History.version(table)

    assert {:ok, :created} =
             Index.create(table, :text,
               name: "words",
               type: :fts,
               replace: true,
               analyzer: @plain
             )

    assert {:ok, after_rebuild} = FTS.prepare(table, "Running cafés", column: :text)
    refute Prepared.terms(before) == Prepared.terms(after_rebuild)
    assert {:ok, snapshot} = History.snapshot(table, version)
    assert {:ok, ^before} = FTS.prepare(snapshot, "Running cafés", index: "words")
    assert {:ok, other} = LanceDB.connect(path)
    assert {:ok, reopened} = Connection.open_table(other, "saved")
    assert {:ok, ^after_rebuild} = FTS.prepare(reopened, "Running cafés", column: "text")
    assert {:ok, ^after_rebuild} = FTS.prepare("Running cafés", analyzer: @plain)
  end

  test "saved preparation preserves missing, ambiguous and non-FTS selector errors", %{conn: conn} do
    {:ok, table} = Connection.create_table(conn, "selectors", [%{id: 1, text: "alpha"}])

    for selector <- [[column: :text], [column: :missing], [index: "missing"]] do
      assert {:error, %Error{}} = FTS.prepare(table, "alpha", selector)
    end

    assert {:ok, :created} = Index.create(table, :id, name: "ids", type: :btree)
    assert {:error, %Error{}} = FTS.prepare(table, "alpha", index: "ids")

    for name <- ["first", "second"] do
      assert {:ok, :created} = Index.create(table, :text, type: :fts, name: name)
    end

    assert {:error, %Error{message: message}} = FTS.prepare(table, "alpha", column: :text)
    assert message =~ "ambiguous"
    assert {:ok, _} = FTS.prepare(table, "alpha", index: "first")
  end

  test "unsupported explicit and saved analyzers are rejected clearly", %{conn: conn} do
    for tokenizer <- [:raw, :whitespace, :ngram, :code] do
      assert_raise ArgumentError, ~r/simple tokenizer/, fn ->
        FTS.prepare("coding", analyzer: [tokenizer: tokenizer])
      end

      table = indexed(conn, to_string(tokenizer), ["coding"], tokenizer: tokenizer)

      assert {:error, %Error{kind: :unsupported, operation: :prepare_fts}} =
               FTS.prepare(table, "coding", column: :text)
    end

    assert_raise ArgumentError, ~r/plain text/, fn ->
      FTS.prepare("name,str,coding", analyzer: [document_tokenizer: :json])
    end

    table = indexed(conn, "json", [~s({"name":"coding"})], document_tokenizer: :json)

    assert {:error, %Error{kind: :unsupported}} =
             FTS.prepare(table, "name,str,coding", index: "words")

    assert {:error, %Error{kind: :unsupported}} =
             FTS.prepare("İstanbul", analyzer: @plain ++ [ascii_folding: false])

    assert {:error, %Error{}} = FTS.prepare("word", analyzer: [language: "not-a-language"])
  end

  test "invalid preparation inputs, limits and selectors raise before native execution" do
    for text <- [nil, 1, <<255>>] do
      assert_raise ArgumentError, fn -> FTS.prepare(text) end
      assert_raise ArgumentError, fn -> FTS.prepare(:unused, text, column: :text) end
    end

    for opts <- [
          %{},
          [unknown: true],
          [max_bytes: 0],
          [max_terms: -1],
          [max_bytes: 1.5],
          [max_terms: 4_294_967_296],
          [analyzer: %{}]
        ] do
      assert_raise ArgumentError, fn -> FTS.prepare("word", opts) end
    end

    for opts <- [
          [],
          %{},
          [column: nil],
          [index: ""],
          [index: :words, column: :text],
          [analyzer: []],
          [max_terms: 0]
        ] do
      assert_raise ArgumentError, fn -> FTS.prepare(:unused, "word", opts) end
    end
  end

  test "prepared-only options validate without changing string-based matches" do
    assert {:ok, prepared} = FTS.prepare("word")

    for opts <- [
          %{},
          [operator: :xor],
          [fuzziness: nil],
          [fuzziness: :auto],
          [fuzziness: 3],
          [fuzziness: {:length, 0, 5}],
          [fuzziness: {:length, 5, 5}],
          [fuzziness: {:length, 9, 5}],
          [fuzziness: {:length, 1.5, 5}],
          [exact_numbers: nil],
          [exact_boost: 0],
          [exact_boost: -1],
          [exact_boost: "2"],
          [exact_boost: 1.0e40],
          [max_expansions: 0],
          [prefix_length: -1],
          [boost: 0],
          [boost: 1.0e38, exact_boost: 10],
          [column: nil, unknown: true]
        ] do
      assert_raise ArgumentError, fn -> FTS.match(prepared, opts) end
    end

    for opts <- [[fuzziness: {:length, 5, 9}], [exact_numbers: true], [exact_boost: 2]] do
      assert_raise ArgumentError, fn -> FTS.match("word", opts) end
    end
  end

  test "unindexed rows keep exact matching while fuzzy expansion uses indexed vocabulary", %{
    conn: conn
  } do
    table = indexed(conn, "residual", ["alpha"], @plain)
    assert {:ok, %{inserted: 1}} = Table.add(table, [%{id: 2, text: "bravo"}])
    assert {:ok, exact} = FTS.prepare(table, "bravo", column: :text)
    assert ids(table, FTS.match(exact, fuzziness: 1)) == [2]
    assert {:ok, typo} = FTS.prepare(table, "bravx", column: :text)
    assert ids(table, FTS.match(typo, fuzziness: 1)) == []
  end

  defp indexed(conn, name, texts, analyzer) do
    rows = texts |> Enum.with_index(1) |> Enum.map(fn {text, id} -> %{id: id, text: text} end)
    assert {:ok, table} = Connection.create_table(conn, name, rows)

    assert {:ok, :created} =
             Index.create(table, :text, type: :fts, name: "words", analyzer: analyzer)

    table
  end

  defp ids(table, query) do
    assert {:ok, rows} = Query.full_text_search(table, query, columns: [:id], limit: 100)
    Enum.sort(Enum.map(rows, & &1["id"]))
  end
end
