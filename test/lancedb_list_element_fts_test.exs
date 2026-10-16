defmodule LanceDB.ListElementFTSTest do
  use LanceDB.DataCase, async: true
  @moduletag :native_operation_contract

  alias LanceDB.{Batch, Connection, Error, FTS, History, Index, Job, Maintenance, Query, Table}

  @analyzer [stem: false, remove_stop_words: false, with_position: true]

  setup %{path: path} do
    {:ok, conn} = Connection.connect(path)

    {:ok, table} =
      Connection.create_table(conn, "passages", records(),
        schema: [id: :int64, docs: {:list, :string}, vector: {:vector, 2}]
      )

    %{conn: conn, table: table}
  end

  test "match returns one hit per element with parent IDs and coordinate lists", %{table: table} do
    create_index(table)

    assert {:ok, rows} = Query.full_text_search(table, match("alpha"), with_row_id: true)
    assert hits(rows) == [{1, [0]}, {1, [2]}, {2, [1]}]
    assert Enum.all?(rows, &is_number(&1["_score"]))
    assert Enum.all?(rows, &(&1["docs"] != nil))
    ids = rows |> Enum.filter(&(&1["id"] == 1)) |> Enum.map(& &1["_rowid"])
    assert [parent, parent] = ids

    assert {:ok, [%{"id" => 1}]} = Query.take_row_ids(table, [parent], columns: [:id])
  end

  test "projections, disabled scores, streams and schema retain element identity", %{table: table} do
    create_index(table)

    query =
      table
      |> Query.new()
      |> Query.text(match("alpha"))
      |> Query.options(columns: [:id], disable_scoring_autoprojection: true, with_row_id: true)

    assert {:ok, rows} = Query.all(query)
    assert hits(rows) == [{1, [0]}, {1, [2]}, {2, [1]}]
    assert Enum.all?(rows, &(Map.keys(&1) |> Enum.sort() == ["_doc_index", "_rowid", "id"]))
    assert hits(Enum.to_list(Query.stream(query, batch_size: 1))) == hits(rows)

    batches = Enum.to_list(Query.stream_batches(query, batch_size: 1))
    assert Enum.all?(batches, &(Batch.size(&1) == 1))

    batch_rows =
      Enum.flat_map(batches, fn batch ->
        {:ok, rows} = Batch.to_rows(batch)
        rows
      end)

    assert hits(batch_rows) == hits(rows)

    assert {:ok, schema} = Query.schema(query)
    assert Enum.map(schema.fields, & &1.name) |> Enum.sort() == ["_doc_index", "_rowid", "id"]
    assert {:ok, plan} = Query.explain(query)
    assert plan =~ "doc_index"
    assert {:ok, analysis} = Query.analyze(query)
    assert analysis =~ "rows=3"

    assert {:ok, selected} =
             Query.full_text_search(table, match("alpha"),
               select: [identifier: "id * 10"],
               disable_scoring_autoprojection: true
             )

    assert Enum.sort(Enum.map(selected, &{&1["identifier"], &1["_doc_index"]})) ==
             [{10, [0]}, {10, [2]}, {20, [1]}]

    assert {:ok, explicit} =
             Query.full_text_search(table, match("alpha"),
               columns: [:id, :_doc_index],
               disable_scoring_autoprojection: true
             )

    assert hits(explicit) == hits(rows)

    assert {:ok, computed} =
             Query.full_text_search(table, match("alpha"),
               select: [identifier: "id", coordinate: "_doc_index"],
               disable_scoring_autoprojection: true
             )

    assert Enum.all?(computed, &(&1["coordinate"] == &1["_doc_index"]))
  end

  test "phrase and Boolean queries join on element coordinates", %{table: table} do
    create_index(table)

    assert {:ok, rows} =
             Query.full_text_search(
               table,
               FTS.phrase("alpha beta", column: :docs, document_granularity: :list_element)
             )

    assert hits(rows) == [{1, [0]}]

    query = FTS.boolean(must: [match("alpha"), match("beta")])
    assert {:ok, rows} = Query.full_text_search(table, query)
    assert hits(rows) == [{1, [0]}]

    query = FTS.boolean(must: [match("alpha")], must_not: [match("beta")])
    assert {:ok, rows} = Query.full_text_search(table, query)
    assert hits(rows) == [{1, [2]}, {2, [1]}]

    query = FTS.boost(match("alpha"), match("beta"))
    assert {:ok, rows} = Query.full_text_search(table, query)
    assert hits(rows) == [{1, [0]}, {1, [2]}, {2, [1]}]
  end

  test "explicit fields allow flat search and index granularity inference", %{table: table} do
    assert {:ok, rows} = Query.full_text_search(table, match("alpha"), columns: [:id])
    assert hits(rows) == [{1, [0]}, {1, [2]}, {2, [1]}]

    create_index(table)

    for query <- [FTS.match("alpha", column: :docs), FTS.match("alpha")] do
      assert {:ok, rows} = Query.full_text_search(table, query, columns: [:id])
      assert hits(rows) == [{1, [0]}, {1, [2]}, {2, [1]}]
    end

    assert {:error, %Error{kind: :invalid_data}} =
             Query.full_text_search(
               table,
               FTS.match("alpha", column: :docs, document_granularity: :row)
             )
  end

  test "nested list fields use public paths and full zero-based coordinates", %{conn: conn} do
    {:ok, table} =
      Connection.create_table(
        conn,
        "nested",
        [
          %{id: 1, docs: [%{content: ["alpha", "beta"]}, %{content: [nil, "alpha beta"]}]},
          %{id: 2, docs: [nil, %{content: []}, %{content: ["alpha"]}]},
          %{id: 3, docs: nil}
        ],
        schema: [id: :int64, docs: {:list, {:struct, content: {:list, :string}}}]
      )

    definition = Index.new(type: :fts, document_granularity: :list_element, analyzer: @analyzer)
    assert {:ok, job} = Index.create_async(table, "docs.content", definition)
    assert {:ok, :created} = Job.wait(job)
    assert {:ok, :created} = Job.wait(job)

    assert {:ok, [%Index.Description{columns: ["docs.content"]}]} = Index.list(table)

    query = match("alpha", "docs.content")
    assert {:ok, rows} = Query.full_text_search(table, query, columns: [:id])
    assert hits(rows) == [{1, [0, 0]}, {1, [1, 1]}, {2, [2, 0]}]

    assert {:ok, prepared} = FTS.prepare(table, "alpha", column: "docs.content")
    query = FTS.match(prepared, column: "docs.content", document_granularity: :list_element)
    assert {:ok, rows} = Query.full_text_search(table, query, columns: [:id])
    assert hits(rows) == [{1, [0, 0]}, {1, [1, 1]}, {2, [2, 0]}]
  end

  test "element indexes survive reopening, writes, deletes and optimization", %{
    table: table,
    conn: conn
  } do
    create_index(table)
    assert {:ok, version} = History.version(table)
    assert {:ok, snapshot} = History.snapshot(table, version)

    assert {:ok, _} =
             Table.add(table, [%{id: 5, docs: ["alpha", "alpha beta"], vector: [1.0, 0.0]}])

    assert {:ok, rows} = Query.full_text_search(table, match("alpha"), columns: [:id])
    assert hits(rows) == [{1, [0]}, {1, [2]}, {2, [1]}, {5, [0]}, {5, [1]}]
    assert {:ok, indexed} = Query.full_text_search(table, match("alpha"), fast_search: true)
    assert hits(indexed) == [{1, [0]}, {1, [2]}, {2, [1]}]
    assert {:ok, historical} = Query.full_text_search(snapshot, match("alpha"))
    assert hits(historical) == hits(indexed)

    assert {:ok, _} = Table.delete(table, "id = 1")
    assert {:ok, :optimized} = Maintenance.optimize_indices(table)
    assert {:ok, reopened} = Connection.open_table(conn, "passages")
    assert {:ok, rows} = Query.full_text_search(reopened, match("alpha"), fast_search: true)
    assert hits(rows) == [{2, [1]}, {5, [0]}, {5, [1]}]

    assert {:ok, _} =
             Table.upsert(reopened, [%{id: 2, docs: ["alpha", "delta"], vector: [0.0, 1.0]}])

    assert {:ok, rows} = Query.full_text_search(reopened, match("alpha"))
    assert hits(rows) == [{2, [0]}, {5, [0]}, {5, [1]}]
  end

  test "omitted and explicit row index settings retain whole-row matches", %{table: table} do
    for granularity <- [[], [document_granularity: :row]] do
      assert {:ok, :created} =
               Index.create(
                 table,
                 :docs,
                 [type: :fts, name: "rows", replace: true, analyzer: @analyzer] ++ granularity
               )

      for options <- [[], [document_granularity: :row]] do
        query = FTS.match("alpha", [column: :docs] ++ options)
        assert {:ok, rows} = Query.full_text_search(table, query, columns: [:id])
        assert Enum.sort(Enum.map(rows, & &1["id"])) == [1, 2]
        refute Enum.any?(rows, &Map.has_key?(&1, "_doc_index"))
      end
    end
  end

  test "coexisting granularities require an explicit query choice", %{table: table} do
    create_index(table)

    assert {:ok, :created} =
             Index.create(table, :docs, type: :fts, name: "rows", analyzer: @analyzer)

    assert {:error, %Error{message: message}} =
             Query.full_text_search(table, FTS.match("alpha", column: :docs))

    assert message =~ "ambiguous"
    assert {:ok, rows} = Query.full_text_search(table, match("alpha"))
    assert hits(rows) == [{1, [0]}, {1, [2]}, {2, [1]}]

    assert {:ok, rows} =
             Query.full_text_search(
               table,
               FTS.match("alpha", column: :docs, document_granularity: :row)
             )

    assert Enum.sort(Enum.map(rows, & &1["id"])) == [1, 2]
    refute Enum.any?(rows, &Map.has_key?(&1, "_doc_index"))
  end

  test "prepared fuzzy terms keep their granularity on every generated leaf", %{table: table} do
    create_index(table)
    assert {:ok, prepared} = FTS.prepare("alpa beta", analyzer: @analyzer)

    query =
      FTS.match(prepared,
        column: :docs,
        document_granularity: :list_element,
        fuzziness: 1,
        operator: :and
      )

    assert {:ok, rows} = Query.full_text_search(table, query)
    assert hits(rows) == [{1, [0]}]
  end

  test "hybrid queries reject explicit and inferred element hits before calling rerankers", %{
    table: table
  } do
    create_index(table)

    for text <- [match("alpha"), FTS.match("alpha", column: :docs), "alpha"],
        reranker <- [:rrf, fn _, _ -> flunk("element hits reached hybrid reranking") end] do
      query =
        Query.new(table)
        |> Query.hybrid([1.0, 0.0], text)
        |> Query.options(reranker: reranker, text_column: :docs)

      assert {:error, %Error{kind: :unsupported, message: message}} = Query.all(query)
      assert message =~ "row-granularity"
      assert {:error, %Error{kind: :unsupported}} = Query.schema(query)
      assert {:error, %Error{kind: :unsupported}} = Query.explain(query)
      assert_raise Error, fn -> Enum.to_list(Query.stream(query)) end
    end
  end

  test "unsupported field and query combinations fail clearly", %{table: table} do
    create_index(table)

    assert {:error, %Error{kind: :invalid_data}} =
             Index.create(table, :id, type: :fts, document_granularity: :list_element)

    query =
      FTS.boolean(
        must: [match("alpha"), FTS.match("beta", column: :docs, document_granularity: :row)]
      )

    assert {:error, %Error{message: message}} = Query.full_text_search(table, query)
    assert message =~ "granularity"

    query = FTS.multi_match("alpha", [:docs])
    assert {:error, %Error{kind: :unsupported}} = Query.full_text_search(table, query)

    assert {:ok, _} = Table.add(table, [%{id: 5, docs: ["different"], vector: [1.0, 0.0]}])
    assert {:ok, []} = Query.full_text_search(table, match("absent"), columns: [:id])
  end

  test "element Boolean leaves cannot join different final fields", %{conn: conn} do
    {:ok, table} =
      Connection.create_table(
        conn,
        "different_fields",
        [%{id: 1, docs: [%{a: "alpha", b: "beta"}]}],
        schema: [id: :int64, docs: {:list, {:struct, a: :string, b: :string}}]
      )

    query = FTS.boolean(must: [match("alpha", "docs.a"), match("beta", "docs.b")])

    assert {:error, %Error{kind: :invalid_data, message: message}} =
             Query.full_text_search(table, query)

    assert message =~ "same final field"
  end

  test "large lists and quoted public field paths work with saved analyzers", %{conn: conn} do
    {:ok, table} =
      Connection.create_table(
        conn,
        "quoted",
        [%{id: 1, "some docs": [%{"body.text": "alpha"}, nil]}],
        schema: [
          id: :int64,
          "some docs": {:large_list, {:struct, [{"body.text", :large_string}]}}
        ]
      )

    column = "`some docs`.`body.text`"

    assert {:ok, :created} =
             Index.create(table, column,
               type: :fts,
               document_granularity: :list_element,
               analyzer: @analyzer
             )

    assert {:ok, prepared} = FTS.prepare(table, "alpha", column: column)
    query = FTS.match(prepared, column: column, document_granularity: :list_element)
    assert {:ok, rows} = Query.full_text_search(table, query, columns: [:id])
    assert hits(rows) == [{1, [0]}]
  end

  test "filtering, ordering, limit and offset operate on element hits", %{table: table} do
    create_index(table)

    assert {:ok, rows} =
             Query.full_text_search(table, match("alpha"), filter: "id = 1", columns: [:id])

    assert hits(rows) == [{1, [0]}, {1, [2]}]

    assert {:ok, rows} =
             Query.full_text_search(table, match("alpha"),
               columns: [:id],
               order_by: [desc: :id],
               limit: 1,
               offset: 1
             )

    assert [%{"id" => 1, "_doc_index" => coordinate}] = rows
    assert coordinate in [[0], [2]]

    assert {:error, %Error{kind: :unsupported, message: message}} =
             Query.full_text_search(table, match("alpha"), order_by: [asc: :_doc_index])

    assert message =~ "Ordering by _doc_index"
  end

  test "granularity arguments are validated before database work" do
    for value <- [nil, :element, "list_element", 1, true] do
      assert_raise ArgumentError, ~r/document_granularity/, fn ->
        Index.new(type: :fts, document_granularity: value)
      end

      for builder <- [&FTS.match/2, &FTS.phrase/2] do
        assert_raise ArgumentError, ~r/document_granularity/, fn ->
          builder.("alpha", document_granularity: value)
        end
      end
    end

    assert_raise ArgumentError, ~r/FTS build options/, fn ->
      Index.new(type: :btree, document_granularity: :row)
    end

    assert_raise ArgumentError, ~r/reserved metadata/, fn ->
      Query.new(%Table{resource: nil}, select: [_doc_index: "1"])
    end
  end

  defp create_index(table) do
    assert {:ok, :created} =
             Index.create(table, :docs,
               type: :fts,
               name: "elements",
               document_granularity: :list_element,
               analyzer: @analyzer
             )
  end

  defp match(text, column \\ :docs),
    do: FTS.match(text, column: column, document_granularity: :list_element)

  defp hits(rows), do: Enum.sort(Enum.map(rows, &{&1["id"], &1["_doc_index"]}))

  defp records do
    [
      %{id: 1, docs: ["alpha beta", "gamma", "alpha", nil], vector: [1.0, 0.0]},
      %{id: 2, docs: ["beta", "alpha", ""], vector: [0.0, 1.0]},
      %{id: 3, docs: [], vector: [0.5, 0.5]},
      %{id: 4, docs: nil, vector: [0.5, 0.5]}
    ]
  end
end
