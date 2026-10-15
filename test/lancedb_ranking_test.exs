defmodule LanceDB.RankingTest do
  use LanceDB.DataCase, async: true

  defmodule ReverseReranker do
    @behaviour LanceDB.Reranker
    @impl true
    def rerank(vector, _text) do
      Enum.reverse(Enum.map(vector, &Map.put(&1, "_relevance_score", 1)))
    end
  end

  setup %{path: path} do
    {:ok, conn} = LanceDB.connect(path)

    rows = [
      %{id: 1, priority: nil, text: "elixir elixir elixir", vector: [0, 0], bytes: <<255>>},
      %{id: 2, priority: 3, text: "elixir programming", vector: [1, 0], bytes: <<128>>},
      %{
        id: 3,
        priority: 1,
        text: "elixir language programming programming",
        vector: [3, 0],
        bytes: <<0>>
      },
      %{id: 4, priority: 2, text: "unrelated", vector: [9, 0], bytes: <<1>>}
    ]

    {:ok, table} =
      LanceDB.Connection.create_table(conn, "docs", rows,
        schema: [
          id: :int64,
          priority: :int32,
          text: :string,
          vector: {:vector, 2},
          bytes: :binary
        ]
      )

    {:ok, :created} = LanceDB.Index.create(table, :text, type: :fts)
    %{table: table, conn: conn}
  end

  test("null placement and multi-column ordering work across query kinds", %{table: table}) do
    for {direction, expected} <- [
          asc_nulls_first: [1, 3, 4, 2],
          asc_nulls_last: [3, 4, 2, 1],
          desc_nulls_first: [1, 2, 4, 3],
          desc_nulls_last: [2, 4, 3, 1]
        ] do
      opts = [limit: 4, order_by: [{direction, :priority}, asc: :id], columns: [:id]]
      assert {:ok, rows} = LanceDB.Query.query(table, opts)
      assert Enum.map(rows, & &1["id"]) == expected
      assert {:ok, rows} = LanceDB.Query.search(table, [0, 0], opts)
      assert Enum.map(rows, & &1["id"]) == expected
      assert {:ok, rows} = LanceDB.Query.hybrid_search(table, [0, 0], "elixir", opts)
      assert Enum.map(rows, & &1["id"]) == expected
      assert Enum.all?(rows, &(Map.keys(&1) |> Enum.sort() == ["_relevance_score", "id"]))
    end

    assert {:ok, rows} =
             LanceDB.Query.full_text_search(table, "elixir",
               order_by: [asc_nulls_first: :priority],
               columns: [:id]
             )

    assert Enum.map(rows, & &1["id"]) == [1, 3, 2]
    assert Enum.all?(rows, &(Map.keys(&1) |> Enum.sort() == ["_score", "id"]))
  end

  test("ordering survives computed projections and streaming", %{table: table}) do
    opts = [order_by: [desc: :id], select: [id: "-id"], limit: 4]
    assert {:ok, rows} = LanceDB.Query.search(table, [0, 0], opts)
    assert Enum.map(rows, & &1["id"]) == [-4, -3, -2, -1]

    assert rows ==
             LanceDB.TestSupport.rows(table, Keyword.put(opts, :vector, [0, 0])) |> Enum.to_list()

    assert {:ok, rows} = LanceDB.Query.full_text_search(table, "elixir", opts)
    assert Enum.map(rows, & &1["id"]) == [-3, -2, -1]
    assert {:ok, rows} = LanceDB.Query.hybrid_search(table, [0, 0], "elixir", opts)
    assert Enum.map(rows, & &1["id"]) == [-4, -3, -2, -1]
  end

  test("custom callbacks receive normalized projected candidates and return original data", %{
    table: table
  }) do
    parent = self()

    callback = fn vector, text ->
      send(parent, {:candidates, vector, text})

      vector
      |> Enum.reverse()
      |> Enum.map(&%{_rowid: &1["_rowid"], _relevance_score: &1["id"] / 10, bytes: "ignored"})
    end

    opts = [reranker: callback, limit: 3, columns: [:id, :bytes]]
    assert {:ok, rows} = LanceDB.Query.hybrid_search(table, [0, 0], "elixir", opts)
    assert Enum.map(rows, & &1["id"]) == [3, 2, 1]
    assert Enum.map(rows, & &1["bytes"]) == [<<0>>, <<128>>, <<255>>]
    refute Enum.any?(rows, &Map.has_key?(&1, "_rowid"))
    assert_receive {:candidates, vector, text}
    assert [+0.0, middle, 1.0] = Enum.map(vector, & &1["_distance"])
    assert_in_delta middle, 1 / 9, 1.0e-7
    assert Enum.all?(text, &(&1["_score"] >= 0 and &1["_score"] <= 1))

    assert Enum.all?(
             vector,
             &(Map.keys(&1) |> Enum.sort() == ["_distance", "_rowid", "bytes", "id"])
           )

    assert {:ok, [%{"id" => 1}]} =
             LanceDB.Query.hybrid_search(table, [0, 0], "elixir",
               reranker: ReverseReranker,
               limit: 1,
               offset: 1
             )
  end

  test("rank normalization differs from score normalization and RRF remains unchanged", %{
    table: table
  }) do
    parent = self()

    callback = fn vector, _text ->
      send(parent, {:distances, Enum.map(vector, & &1["_distance"])})
      []
    end

    for normalization <- [:score, :rank] do
      assert {:ok, []} =
               LanceDB.Query.hybrid_search(table, [0, 0], "elixir",
                 reranker: callback,
                 normalize: normalization,
                 limit: 3
               )
    end

    assert_receive {:distances, [+0.0, middle, 1.0]}
    assert_in_delta middle, 1 / 9, 1.0e-7
    assert_receive {:distances, [+0.0, 0.5, 1.0]}
    assert {:ok, expected} = LanceDB.Query.hybrid_search(table, [0, 0], "elixir")

    assert {:ok, ^expected} =
             LanceDB.Query.hybrid_search(table, [0, 0], "elixir",
               reranker: :rrf,
               normalize: :rank
             )
  end

  test("custom ordering and pagination happen after reranking", %{table: table}) do
    opts = [
      reranker: ReverseReranker,
      order_by: [asc: :id],
      limit: 2,
      offset: 1,
      columns: [:id],
      with_row_id: true
    ]

    assert {:ok, rows} = LanceDB.Query.hybrid_search(table, [0, 0], "elixir", opts)
    assert Enum.map(rows, & &1["id"]) == [2, 3]
    assert Enum.all?(rows, &Map.has_key?(&1, "_rowid"))
    stream_opts = opts ++ [vector: [0, 0], text: "elixir", batch_size: 1]
    assert rows == LanceDB.TestSupport.rows(table, stream_opts) |> Enum.to_list()

    assert {:ok, schema} =
             LanceDB.TestSupport.output_schema(
               table,
               Keyword.drop(stream_opts, [:batch_size])
             )

    assert Enum.map(schema.fields, & &1.name) == ["id", "_relevance_score", "_rowid"]

    assert {:ok, plan} =
             LanceDB.TestSupport.explain_plan(
               table,
               Keyword.drop(stream_opts, [:batch_size])
             )

    assert plan =~ "custom Elixir callback"

    assert {:ok, analysis} =
             LanceDB.TestSupport.analyze_plan(
               table,
               Keyword.drop(stream_opts, [:batch_size])
             )

    assert analysis =~ "custom Elixir callback"
    assert analysis =~ "Output: rows=2"
  end

  test("search ordering operates on each query's candidate budget", %{table: table}) do
    assert {:ok, rows} = LanceDB.Query.search(table, [0, 0], limit: 2, order_by: [desc: :id])
    assert Enum.map(rows, & &1["id"]) == [2, 1]

    assert {:ok, rows} =
             LanceDB.Query.batch_search(table, [[0, 0], [9, 0]], limit: 2, order_by: [asc: :id])

    assert rows |> Enum.group_by(& &1["query_index"], & &1["id"]) == %{0 => [1, 2], 1 => [3, 4]}

    assert {:ok, rows} =
             LanceDB.Query.search(table, [0, 0], limit: 1, offset: 1, order_by: [desc: :id])

    assert Enum.map(rows, & &1["id"]) == [1]
  end

  test("custom callbacks support empty and disjoint channels", %{table: table, conn: conn}) do
    parent = self()

    callback = fn vector, text ->
      send(parent, {:sizes, length(vector), length(text)})

      {:ok,
       Enum.uniq_by(vector ++ text, & &1["_rowid"])
       |> Enum.map(&Map.put(&1, "_relevance_score", 1))}
    end

    assert {:ok, [_ | _]} =
             LanceDB.Query.hybrid_search(table, [0, 0], "elixir",
               reranker: callback,
               distance_range: {1000, nil}
             )

    assert_receive {:sizes, 0, 3}

    assert {:ok, [_ | _]} =
             LanceDB.Query.hybrid_search(table, [0, 0], "absent", reranker: callback)

    assert_receive {:sizes, 4, 0}

    assert {:ok, []} =
             LanceDB.Query.hybrid_search(table, [0, 0], "absent",
               reranker: callback,
               filter: "id < 0"
             )

    assert_receive {:sizes, 0, 0}

    {:ok, empty} =
      LanceDB.Connection.create_empty_table(conn, "empty",
        id: :int64,
        text: :string,
        vector: {:vector, 2}
      )

    assert {:ok, []} =
             LanceDB.Query.hybrid_search(empty, [1, 0], "term",
               text_column: :text,
               reranker: callback
             )

    assert_receive {:sizes, 0, 0}

    assert {:ok, analysis} =
             LanceDB.TestSupport.analyze_plan(empty,
               vector: [1, 0],
               text: "term",
               text_column: :text,
               reranker: callback
             )

    assert analysis =~ "Output: rows=0"
  end

  test("invalid callbacks and results fail without leaking mutated data", %{table: table}) do
    for callback <- [
          fn _, _ -> raise "oops" end,
          fn _, _ -> throw(:oops) end,
          fn _, _ -> exit(:oops) end,
          fn _, _ -> Process.exit(self(), :kill) end,
          fn _, _ -> {:error, :unavailable} end,
          fn _, _ -> :bad end,
          fn _, _ -> [nil] end,
          fn _, _ -> [%{}] end,
          fn _, _ -> [%{_rowid: 99_999_999, _relevance_score: 1}] end,
          fn [row | _], _ ->
            [Map.put(row, "_relevance_score", 1), Map.put(row, "_relevance_score", 2)]
          end,
          fn [row | _], _ -> [Map.put(row, "_relevance_score", 1.0e100)] end
        ] do
      assert {:error, reason} =
               LanceDB.Query.hybrid_search(table, [0, 0], "elixir", reranker: callback)

      assert is_struct(reason, LanceDB.Error)
    end

    assert {:ok, 4} = LanceDB.Table.count_rows(table)
  end

  test("callbacks are lazy in streams and are not invoked by plan or schema inspection", %{
    table: table
  }) do
    parent = self()

    callback = fn vector, _ ->
      send(parent, :invoked)
      Enum.map(vector, &Map.put(&1, "_relevance_score", 1))
    end

    opts = [vector: [0, 0], text: "elixir", reranker: callback, batch_size: 1]
    stream = LanceDB.TestSupport.rows(table, opts)
    refute_receive :invoked, 10

    assert {:ok, _} =
             LanceDB.TestSupport.output_schema(
               table,
               Keyword.delete(opts, :batch_size)
             )

    assert {:ok, _} =
             LanceDB.TestSupport.explain_plan(
               table,
               Keyword.delete(opts, :batch_size)
             )

    refute_receive :invoked, 10
    assert length(Enum.take(stream, 1)) == 1
    assert_receive :invoked
    assert length(Enum.to_list(stream)) == 4
    assert_receive :invoked

    failing =
      LanceDB.TestSupport.rows(table,
        vector: [0, 0],
        text: "elixir",
        reranker: fn _, _ -> :bad end
      )

    error = assert_raise LanceDB.Error, fn -> Enum.to_list(failing) end
    assert error.operation == :stream_open
  end

  test("query deadlines cancel callbacks", %{table: table}) do
    parent = self()

    callback = fn _, _ ->
      send(parent, {:callback_pid, self()})
      Process.sleep(:infinity)
    end

    assert {:error, reason} =
             LanceDB.Query.hybrid_search(table, [0, 0], "elixir",
               reranker: callback,
               timeout_ms: 200
             )

    assert reason.message =~ "timed out"
    assert_receive {:callback_pid, pid}
    refute Process.alive?(pid)
  end

  test "validates ranking and ordering options before native calls" do
    for opts <- [
          [normalize: :bad],
          [reranker: nil],
          [reranker: fn _ -> [] end],
          [reranker: %{}],
          [reranker: ReverseReranker, rrf_k: 60],
          [order_by: [up: :id]],
          [order_by: [asc_nulls_first: nil]]
        ] do
      assert_raise ArgumentError, fn ->
        LanceDB.Query.hybrid_search(%LanceDB.Table{resource: make_ref()}, [1, 0], "term", opts)
      end
    end

    assert_raise ArgumentError, fn ->
      LanceDB.Query.search(%LanceDB.Table{resource: make_ref()}, [1, 0], normalize: :rank)
    end

    assert_raise ArgumentError, fn ->
      LanceDB.Query.full_text_search(%LanceDB.Table{resource: make_ref()}, "term",
        reranker: ReverseReranker
      )
    end

    assert_raise ArgumentError, fn ->
      LanceDB.Query.query(%LanceDB.Table{resource: make_ref()}, normalize: :score)
    end
  end

  test("indexed and multivector searches retain candidate ordering", %{table: table, conn: conn}) do
    assert {:ok, :created} =
             LanceDB.Index.create(table, :vector, type: :ivf_flat, num_partitions: 1)

    assert {:ok, rows} =
             LanceDB.Query.search(table, [0, 0],
               limit: 2,
               nprobes: 1,
               order_by: [desc: :id],
               select: [value: "id * 2"]
             )

    assert Enum.map(rows, & &1["value"]) == [4, 2]

    assert {:ok, rows} =
             LanceDB.Query.full_text_search(table, "elixir", limit: 2, order_by: [desc: :id])

    assert Enum.map(rows, & &1["id"]) == [2, 1]

    {:ok, multi} =
      LanceDB.Connection.create_table(
        conn,
        "multi",
        [
          %{id: 1, vectors: [[1, 0]]},
          %{id: 2, vectors: [[0, 1]]},
          %{id: 3, vectors: [[0.8, 0.2]]}
        ],
        schema: [id: :int64, vectors: {:list, {:vector, 2}}]
      )

    assert {:ok, rows} =
             LanceDB.Query.multivector_search(multi, [[1, 0]],
               limit: 2,
               vector_column: :vectors,
               order_by: [desc: :id]
             )

    assert Enum.map(rows, & &1["id"]) == [3, 1]
  end

  test("callbacks are cancelled when their caller exits", %{table: table}) do
    parent = self()

    caller =
      spawn(fn ->
        LanceDB.Query.hybrid_search(table, [0, 0], "elixir",
          reranker: fn _, _ ->
            send(parent, {:worker, self()})
            Process.sleep(:infinity)
          end
        )
      end)

    assert_receive {:worker, worker}, 1000
    monitor = Process.monitor(worker)
    Process.exit(caller, :kill)
    assert_receive {:DOWN, ^monitor, :process, ^worker, :killed}, 1000
  end

  test("custom stream deadline includes time between chunks", %{table: table}) do
    stream =
      LanceDB.TestSupport.rows(table,
        vector: [0, 0],
        text: "elixir",
        reranker: ReverseReranker,
        batch_size: 1,
        timeout_ms: 200
      )

    assert_raise LanceDB.Error, ~r/timed out/, fn ->
      stream |> Stream.each(fn _ -> Process.sleep(250) end) |> Enum.to_list()
    end
  end

  test("ordering by relevance works after callbacks and rejects missing keys", %{table: table}) do
    callback = fn vector, _ -> Enum.map(vector, &Map.put(&1, "_relevance_score", &1["key"])) end

    assert {:ok, rows} =
             LanceDB.Query.hybrid_search(table, [0, 0], "elixir",
               reranker: callback,
               order_by: [desc: :_relevance_score],
               select: [key: "id"],
               limit: 3
             )

    assert Enum.map(rows, & &1["key"]) == [3, 2, 1]

    assert {:error, _} =
             LanceDB.Query.hybrid_search(table, [0, 0], "elixir", order_by: [asc: :missing])

    assert {:error, _} = LanceDB.Query.search(table, [0, 0], order_by: [asc: :missing])

    assert {:error, _} =
             LanceDB.Query.full_text_search(table, "elixir", order_by: [asc: :missing])
  end

  test("hybrid result contracts agree across ranking and execution modes", %{table: table}) do
    callback = fn vector, text ->
      Enum.uniq_by(vector ++ text, & &1["_rowid"])
      |> Enum.map(&Map.put(&1, "_relevance_score", 1))
    end

    for reranker <- [:rrf, callback],
        channels <- [[], [distance_range: {1000, nil}], [filter: "id < 0"]],
        row_ids <- [false, true] do
      opts =
        channels ++
          [
            vector: [0, 0],
            text: "elixir",
            reranker: reranker,
            order_by: [asc_nulls_last: :priority, desc: :id],
            select: [key: "id", payload: "bytes"],
            with_row_id: row_ids,
            limit: 2,
            offset: 1
          ]

      assert {:ok, rows} =
               LanceDB.Query.hybrid_search(
                 table,
                 [0, 0],
                 "elixir",
                 Keyword.drop(opts, [:vector, :text])
               )

      assert rows == LanceDB.TestSupport.rows(table, opts ++ [batch_size: 1]) |> Enum.to_list()
      assert {:ok, schema} = LanceDB.TestSupport.output_schema(table, opts)
      names = Enum.map(schema.fields, & &1.name) |> Enum.sort()

      expected =
        ["key", "payload", "_relevance_score"] ++
          if row_ids do
            ["_rowid"]
          else
            []
          end

      assert names == Enum.sort(expected)
      assert Enum.all?(rows, &(Enum.sort(Map.keys(&1)) == names))
      assert Enum.all?(rows, &is_binary(&1["payload"]))
      assert {:ok, analysis} = LanceDB.TestSupport.analyze_plan(table, opts)
      assert String.ends_with?(analysis, "Output: rows=#{length(rows)}
")
    end
  end

  test("custom output uses a bounded native cursor with explicit cleanup", %{table: table}) do
    alias LanceDB.Query.{Definition, Executor, Request}

    plan =
      Definition.new({:hybrid, [0, 0], "elixir"},
        reranker: ReverseReranker,
        columns: [:id, :bytes]
      )
      |> Request.compile()

    assert {:ok, cursor} = Executor.open(table, plan, 1)

    try do
      assert {:ok, [%{"id" => 4, "bytes" => <<1>>}]} = LanceDB.TestSupport.next_rows(cursor)
      assert {:ok, [%{"id" => 3, "bytes" => <<0>>}]} = LanceDB.TestSupport.next_rows(cursor)
    after
      Executor.close(cursor)
    end

    assert :ok = Executor.close(cursor)

    assert {:error, %LanceDB.Error{message: "Stream is closed"}} =
             LanceDB.TestSupport.next_rows(cursor)
  end

  test(
    "native reranking analysis returns a count and final streams inherit the original deadline",
    %{table: table}
  ) do
    alias LanceDB.Backend, as: Native
    alias LanceDB.Query.{Callback, Definition, Request}

    plan =
      Definition.new({:hybrid, [0, 0], "elixir"}, reranker: ReverseReranker, timeout_ms: 300)
      |> Request.compile()

    assert {:ok, {resource, vector, text, _}} =
             Native.rerank_candidates(table, plan, true)

    assert {:ok, scores} = Callback.run(plan.reranker, vector, text, :infinity)
    assert {:ok, 4} = Native.rerank_finish(resource, scores, :analyze, 1)
    Process.sleep(350)
    assert {:error, reason} = Native.rerank_finish(resource, scores, :stream, 1)
    assert reason.message =~ "timed out"
  end
end
