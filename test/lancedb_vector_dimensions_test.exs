defmodule LanceDB.VectorDimensionsTest do
  use LanceDB.DataCase, async: true

  alias LanceDB.{Connection, Error, Index, Query}

  setup %{path: path} do
    {:ok, conn} = LanceDB.connect(path)
    %{conn: conn}
  end

  test "single, batch and hybrid execution report the stored and query dimensions", %{conn: conn} do
    {:ok, table} =
      Connection.create_table(conn, "vectors", [%{id: 1, text: "hello", vector: [1, 0]}])

    assert {:ok, :created} = Index.create(table, :text, type: :fts)
    assert {:ok, :created} = Index.create(table, :vector, type: :ivf_flat, num_partitions: 1)
    message = "Vector column 'vector' expects dimension 2; query has dimension 3"

    for opts <- [[], [vector_column: :vector]],
        source <- [
          {:vector, [1, 0, 0]},
          {:batch, [[1, 0, 0], [0, 1, 0]]},
          {:hybrid, [1, 0, 0], "hello"}
        ] do
      query = Query.new(table, source, opts)

      for execute <- [&Query.all/1, &Query.schema/1, &Query.explain/1, &Query.analyze/1] do
        assert {:error, %Error{kind: :invalid_data, message: ^message}} = execute.(query)
      end

      for stream <- [&Query.stream/1, &Query.stream_batches/1] do
        assert_raise Error, message, fn -> Enum.to_list(stream.(query)) end
      end
    end

    assert {:error, %Error{kind: :invalid_data, message: ^message}} =
             Query.search(table, [1, 0, 0], bypass_vector_index: true)

    assert {:error, %Error{kind: :invalid_data, message: ^message}} =
             Query.hybrid_search(table, [1, 0, 0], "hello",
               reranker: fn _, _ -> flunk("invalid dimensions must fail before reranking") end
             )
  end

  test "nested single and multivector columns report dimensions in both selection modes", %{
    conn: conn
  } do
    {:ok, table} =
      Connection.create_table(
        conn,
        "nested",
        [%{data: %{single: [1, 0], multi: [[1, 0], [0, 1]]}}],
        schema: [data: {:struct, single: {:vector, 2}, multi: {:list, {:vector, 2}}}]
      )

    for {source, column} <- [
          {{:vector, [1, 0, 0]}, "data.single"},
          {{:batch, [[1, 0, 0]]}, "data.single"},
          {{:multivector, [[1, 0, 0], [0, 1, 0]]}, "data.multi"}
        ],
        opts <- [[], [vector_column: column]] do
      message = "Vector column '#{column}' expects dimension 2; query has dimension 3"

      assert {:error, %Error{kind: :invalid_data, message: ^message}} =
               table |> Query.new(source, opts) |> Query.all()
    end
  end

  test "column inference keeps unique matches and distinguishes ambiguity from wrong dimensions",
       %{
         conn: conn
       } do
    {:ok, mixed} =
      Connection.create_table(conn, "mixed", [%{two: [1, 0], three: [1, 0, 0]}],
        schema: [two: {:vector, 2}, three: {:vector, 3}]
      )

    assert {:ok, [_]} = Query.search(mixed, [1, 0])
    assert {:ok, [_]} = Query.search(mixed, [1, 0, 0])

    message =
      "Query vector dimension 4 does not match any compatible vector column ('two': 2, 'three': 3)"

    assert {:error, %Error{kind: :invalid_data, message: ^message}} =
             Query.search(mixed, [1, 0, 0, 0])

    assert {:error,
            %Error{
              kind: :invalid_data,
              message: "Vector column 'two' expects dimension 2; query has dimension 3"
            }} = Query.search(mixed, [1, 0, 0], vector_column: :two)

    {:ok, ambiguous} = Connection.create_table(conn, "ambiguous", [%{a: [1, 0], b: [0, 1]}])

    assert {:error,
            %Error{
              kind: :invalid_data,
              message: "Expected exactly one matching vector column; specify vector_column"
            }} = Query.search(ambiguous, [1, 0])

    assert {:ok, [_]} = Query.search(ambiguous, [1, 0], vector_column: :a)
  end

  test "type and mode errors are distinct from dimension errors", %{conn: conn} do
    {:ok, table} = Connection.create_table(conn, "single", [%{id: 1, vector: [1, 0]}])

    assert {:error,
            %Error{
              kind: :invalid_data,
              message: "No vector column has a type compatible with multivector search"
            }} = Query.multivector_search(table, [[1, 0]])

    assert {:error,
            %Error{
              kind: :invalid_data,
              message: "Vector column 'vector' has a type incompatible with multivector search"
            }} = Query.multivector_search(table, [[1, 0]], vector_column: :vector)

    assert {:error,
            %Error{
              kind: :invalid_data,
              message: "Vector column 'id' has a type incompatible with single-vector search"
            }} = Query.search(table, [1, 0], vector_column: :id)

    assert {:error, %Error{kind: :invalid_data, message: "Unknown vector column: missing"}} =
             Query.search(table, [1, 0], vector_column: :missing)

    {:ok, scalar} = Connection.create_table(conn, "scalar", [%{id: 1}])

    assert {:error,
            %Error{
              kind: :invalid_data,
              message: "No vector column has a type compatible with single-vector search"
            }} = Query.search(scalar, [1, 0])
  end

  test "dimension diagnostics work for every vector type and empty tables", %{conn: conn} do
    message = "Vector column 'vector' expects dimension 2; query has dimension 3"

    for type <- [:float16, :float32, :float64, :uint8] do
      {:ok, table} =
        Connection.create_table(conn, to_string(type), [%{vector: [1, 0]}],
          schema: [vector: {:vector, 2, type}]
        )

      distance = if type == :uint8, do: :hamming, else: :l2
      assert {:ok, [_]} = Query.search(table, [1, 0], distance: distance)

      for opts <- [[distance: distance], [distance: distance, vector_column: :vector]] do
        assert {:error, %Error{kind: :invalid_data, message: ^message}} =
                 Query.search(table, [1, 0, 0], opts)
      end
    end

    {:ok, empty} = Connection.create_empty_table(conn, "empty", vector: {:vector, 2})

    for opts <- [[], [vector_column: :vector]] do
      assert {:error, %Error{kind: :invalid_data, message: ^message}} =
               Query.search(empty, [1, 0, 0], opts)
    end
  end
end
