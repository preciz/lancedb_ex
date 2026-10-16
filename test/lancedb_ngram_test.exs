defmodule LanceDB.NGramTest do
  use LanceDB.DataCase, async: true
  alias LanceDB.{Connection, Error, Expr, History, Index, Job, Maintenance, Query, Table}

  @texts [
    "cat",
    "catalog",
    "scatter",
    "cat dog",
    "rhino nose",
    "rhino horn",
    "elephant",
    "CATALOG",
    "café",
    "cafe",
    "ééb",
    "aéé",
    "雪山猫",
    "!!!",
    "",
    nil,
    "abc---bcd",
    "abcd",
    "value.abc",
    "rate%abc",
    "rate_abc"
  ]

  setup %{path: path} do
    {:ok, conn} = LanceDB.connect(path)
    %{conn: conn}
  end

  test "substring, LIKE and regex filters use indexes and recheck original text", %{conn: conn} do
    filters = [
      {Expr.call(:contains, [Expr.col(:text), "cat"]), [1, 2, 3, 4]},
      {"contains(text, 'CAT')", [8]},
      # Both rows 17 and 18 contain the trigrams abc and bcd. Only 18 matches.
      {"contains(text, 'abcd')", [18]},
      {Expr.like(Expr.col(:text), "%cat%"), [1, 2, 3, 4]},
      {"text LIKE 'cat%'", [1, 2, 4]},
      {"text LIKE '%cat_log%'", [2]},
      {"text LIKE '%value.abc%'", [19]},
      {~S(text LIKE '%rate\%abc%' ESCAPE '\'), [20]},
      {~S(text LIKE '%rate\_abc%' ESCAPE '\'), [21]},
      {"regexp_like(text, 'cat$')", [1]},
      {"contains(text, 'café')", [9]},
      {"contains(text, 'cafe')", [10]},
      {"contains(text, 'ééb')", [11]},
      {"contains(text, 'absent')", []}
    ]

    regex_filters = [
      {"regexp_match(text, '^cat.*dog$')", [4]},
      {"regexp_match(text, '(catalog|elephant)')", [2, 7]},
      {Expr.call(:regexp_match, [Expr.col(:text), "cat$"]), [1]}
    ]

    for type <- [:string, :large_string] do
      table = create_table(conn, Atom.to_string(type), type)
      for {filter, expected} <- filters ++ regex_filters, do: assert_ids(table, filter, expected)

      assert {:ok, :created} = Index.create(table, :text, type: :ngram)

      for {filter, expected} <- filters ++ regex_filters, do: assert_ids(table, filter, expected)

      # The SDK casts LargeUtf8 regex input to Utf8View, hiding the indexed
      # column. Preserve the correct scan results for that storage type.
      indexed_filters = if type == :string, do: filters ++ regex_filters, else: filters

      for {filter, _expected} <- indexed_filters do
        assert {:ok, plan} = LanceDB.TestSupport.explain_plan(table, filter: filter)
        assert plan =~ "ScalarIndexQuery", "#{type}: #{inspect(filter)}\n#{plan}"
        assert plan =~ "text_idx(NGram)", "#{type}: #{inspect(filter)}\n#{plan}"
      end
    end
  end

  test "short, Unicode and case-insensitive patterns preserve full-scan results", %{conn: conn} do
    filters = [
      {"contains(text, 'ca')", [1, 2, 3, 4, 9, 10]},
      {"contains(text, 'éé')", [11, 12]},
      {"contains(text, '雪山猫')", [13]},
      {"contains(text, '!!!')", [14]},
      {"contains(text, concat('雪', '山猫'))", [13]},
      {"NOT contains(text, '!!!')", Enum.to_list(1..21) -- [14, 16]},
      {"contains(text, '雪山猫') OR contains(text, 'cat')", [1, 2, 3, 4, 13]},
      {"contains(text, '')", Enum.to_list(1..21) -- [16]},
      {"text LIKE '%ca%'", [1, 2, 3, 4, 9, 10]},
      {"text ILIKE '%CAT%'", [1, 2, 3, 4, 8]},
      {"regexp_like(text, 'CAT', 'i')", [1, 2, 3, 4, 8]},
      {"regexp_like(text, 'c.t')", [1, 2, 3, 4]},
      {"regexp_like(text, '^cat.*dog$')", [4]},
      {"regexp_match(text, 'c.t')", [1, 2, 3, 4]},
      {"text IS NULL", [16]},
      {"text = ''", [15]}
    ]

    for type <- [:string, :large_string] do
      table = create_table(conn, "fallback_#{type}", type)
      for {filter, expected} <- filters, do: assert_ids(table, filter, expected)
      assert {:ok, :created} = Index.create(table, :text, type: :ngram)
      for {filter, expected} <- filters, do: assert_ids(table, filter, expected)

      for filter <- [
            "contains(text, 'éé')",
            "contains(text, '雪山猫')",
            "contains(text, '!!!')",
            "text ILIKE '%CAT%'",
            "regexp_like(text, 'CAT', 'i')",
            "regexp_like(text, 'c.t')"
          ] do
        assert {:ok, plan} = LanceDB.TestSupport.explain_plan(table, filter: filter)
        refute plan =~ "ScalarIndexQuery", filter
      end
    end
  end

  test "untokenizable filters preserve counts, streams, searches and filtered writes", %{
    conn: conn
  } do
    {:ok, table} =
      Connection.create_table(
        conn,
        "filtered",
        [
          %{id: 1, text: "雪山猫 !!!", body: "matching", vector: [1.0, 0.0]},
          %{id: 2, text: "雪山犬", body: "matching", vector: [0.0, 1.0]},
          %{id: 3, text: nil, body: "matching", vector: [0.5, 0.5]}
        ],
        schema: [id: :int64, text: :string, body: :string, vector: {:vector, 2}]
      )

    assert {:ok, :created} = Index.create(table, :text, type: :ngram)
    assert {:ok, :created} = Index.create(table, :body, type: :fts)
    filter = "contains(text, '雪山猫')"
    assert {:ok, 1} = Table.count_rows(table, filter)

    assert [%{"id" => 1}] =
             table
             |> Query.new()
             |> Query.where(filter)
             |> Query.select([:id])
             |> Query.stream(batch_size: 1)
             |> Enum.to_list()

    assert {:ok, [%{"id" => 1}]} =
             Query.search(table, [1.0, 0.0], filter: filter, columns: [:id])

    assert {:ok, [%{"id" => 1}]} =
             Query.full_text_search(table, "matching",
               text_column: :body,
               filter: filter,
               columns: [:id]
             )

    assert {:ok, [%{"id" => 1}]} =
             Query.hybrid_search(table, [1.0, 0.0], "matching",
               text_column: :body,
               filter: filter,
               columns: [:id]
             )

    assert {:ok, %{updated: 1}} = Table.update(table, %{body: "changed"}, filter: filter)
    assert_ids(table, "body = 'changed'", [1])
    assert {:ok, 1} = Table.count_rows(table, filter)
    assert {:ok, %{deleted: 1}} = Table.delete(table, filter)
    assert {:ok, 0} = Table.count_rows(table, filter)
    assert_ids(table, "id >= 1", [2, 3])
  end

  test "reusable definitions, empty indexes and asynchronous rebuilds retain coverage", %{
    conn: conn,
    path: path
  } do
    table = create_table(conn, "jobs", :large_string)
    definition = Index.new(type: :ngram, name: "patterns", train: false)
    assert {:ok, :created} = Index.create(table, :text, definition)

    assert {:ok, %{"num_indexed_rows" => 0, "num_unindexed_rows" => 21}} =
             Index.stats(table, "patterns")

    assert_ids(table, "text LIKE '%cat%'", [1, 2, 3, 4])
    assert {:ok, %{inserted: 1}} = Table.add(table, [%{id: 22, text: "cat"}])
    assert_ids(table, "text LIKE '%cat%'", [1, 2, 3, 4, 22])
    assert {:ok, :optimized} = Maintenance.optimize_indices(table)
    assert {:ok, :ready} = Index.wait(table, "patterns")

    assert {:ok, %{"num_indexed_rows" => 22, "num_unindexed_rows" => 0}} =
             Index.stats(table, "patterns")

    replacement = Index.new(type: :ngram, name: "patterns", replace: true)
    assert {:ok, %Job{} = job} = Index.create_async(table, :text, replacement)
    assert {:ok, :created} = Job.wait(job, timeout_ms: 10_000)
    assert {:ok, :created} = Job.wait(job)

    {:ok, other} = LanceDB.connect(path)
    {:ok, reopened} = Connection.open_table(other, "jobs")

    assert {:ok, [%Index.Description{name: "patterns", index_type: "NGRAM", columns: ["text"]}]} =
             Index.list(reopened)

    assert_ids(reopened, "text LIKE '%cat%'", [1, 2, 3, 4, 22])
    assert {:ok, :prewarmed} = Index.prewarm(reopened, "patterns")
  end

  test "binary columns and historical handles reject n-gram creation without mutation", %{
    conn: conn
  } do
    {:ok, table} =
      Connection.create_table(conn, "invalid", [%{id: 1, text: "cat", bytes: <<0, 255>>}],
        schema: [id: :int64, text: :string, bytes: :binary]
      )

    {:ok, version} = History.version(table)
    assert {:error, %Error{}} = Index.create(table, :bytes, type: :ngram)
    assert {:error, %Error{}} = Index.create(table, :missing, type: :ngram)
    {:ok, snapshot} = History.snapshot(table, version)
    assert {:error, %Error{}} = Index.create(snapshot, :text, type: :ngram)
    assert {:error, %Error{}} = Index.create_async(snapshot, :text, type: :ngram)
    assert {:ok, ^version} = History.version(table)
    assert {:ok, []} = Index.list(table)
    assert_ids(table, "contains(text, 'cat')", [1])
  end

  defp create_table(conn, name, type) do
    rows = Enum.with_index(@texts, 1) |> Enum.map(fn {text, id} -> %{id: id, text: text} end)

    {:ok, table} =
      Connection.create_table(conn, name, rows,
        schema: [id: :int64, text: type],
        max_rows_per_file: 4
      )

    table
  end

  defp assert_ids(table, filter, expected) do
    assert {:ok, rows} = Query.query(table, filter: filter, columns: [:id], limit: nil)

    assert Enum.sort(Enum.map(rows, & &1["id"])) == expected,
           "Filter: #{inspect(filter)}; rows: #{inspect(rows)}"
  end
end
