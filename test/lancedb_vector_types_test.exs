defmodule LanceDB.VectorTypesTest do
  use LanceDB.DataCase, async: true
  use ExUnitProperties
  import Bitwise
  alias LanceDB.{Batch, Schema}

  setup %{path: path} do
    {:ok, db} = LanceDB.connect(path)
    %{db: db}
  end

  test("typed vectors round trip through storage, batches and IPC", %{db: db}) do
    for type <- [:float16, :float32, :float64, :uint8] do
      schema = Schema.new(id: :int64, vector: {:vector, 2, type})
      rows = [%{id: 1, vector: [1, 2]}, %{id: 2, vector: [3, 4]}]

      assert {:ok, table} =
               LanceDB.Connection.create_table(db, to_string(type), rows, schema: schema)

      assert {:ok, ^schema} = LanceDB.Table.schema(table)
      assert {:ok, batch} = Batch.from_rows(rows, schema)
      assert {:ok, ipc} = Batch.to_ipc(batch)
      assert {:ok, [copy]} = Batch.from_ipc(ipc)
      assert {:ok, ^schema} = LanceDB.TestSupport.logical_schema(Batch.schema(copy))

      distance =
        if type == :uint8 do
          :hamming
        else
          :l2
        end

      assert {:ok, [%{"id" => 2}]} =
               LanceDB.Query.search(table, [3, 4], distance: distance, limit: 1)

      assert {:ok, results} =
               LanceDB.Query.batch_search(table, [[1, 2], [3, 4]], distance: distance, limit: 1)

      assert Enum.map(results, & &1["id"]) |> Enum.sort() == [1, 2]

      assert {:ok, :created} =
               LanceDB.Index.create(table, :vector,
                 type: :ivf_flat,
                 distance: distance,
                 num_partitions: 1
               )

      assert {:ok, [%{"id" => 2}]} =
               LanceDB.Query.search(table, [3, 4], distance: distance, limit: 1)
    end
  end

  test("float64 input retains precision beyond float32", %{db: db}) do
    schema = Schema.new(id: :int64, vector: {:vector, 2, :float64})

    assert {:ok, table} =
             LanceDB.Connection.create_table(
               db,
               "precise",
               [%{id: 1, vector: [1.00000001, 0]}, %{id: 2, vector: [1.00000002, 0]}],
               schema: schema
             )

    assert {:ok, [%{"id" => 2, "_distance" => +0.0}]} =
             LanceDB.Query.search(table, [1.00000002, 0], limit: 1)
  end

  test("invalid byte values, overflow and mismatched distances fail before writes", %{db: db}) do
    for {type, invalid} <- [float16: 65_505, float32: 1.0e100, uint8: 256, uint8: -1, uint8: 1.5] do
      schema = Schema.new(vector: {:vector, 1, type})
      assert {:error, _} = Batch.from_rows([%{vector: [invalid]}], schema)
    end

    assert {:ok, table} =
             LanceDB.Connection.create_table(db, "bytes", [%{vector: [1, 2]}],
               schema: [vector: {:vector, 2, :uint8}]
             )

    assert {:error, _} = LanceDB.Query.search(table, [1, 2])

    assert_raise ArgumentError, fn ->
      LanceDB.Query.search(table, [1.5, 2], distance: :hamming)
    end

    assert_raise ArgumentError, fn ->
      LanceDB.Query.search(table, [256, 2], distance: :hamming)
    end
  end

  property("Hamming scores match a bytewise popcount oracle") do
    check(
      all(
        dimension <- integer(1..8),
        vectors <-
          uniq_list_of(list_of(integer(0..255), length: dimension), min_length: 2, max_length: 12),
        query <- list_of(integer(0..255), length: dimension),
        max_runs: 20
      )
    ) do
      with_database(fn db ->
        rows = vectors |> Enum.with_index() |> Enum.map(fn {v, i} -> %{id: i, vector: v} end)

        {:ok, table} =
          LanceDB.Connection.create_table(db, "hamming_property", rows,
            schema: [id: :int64, vector: {:vector, dimension, :uint8}],
            mode: :overwrite
          )

        assert {:ok, result} =
                 LanceDB.Query.search(table, query, distance: :hamming, limit: length(rows))

        assert Enum.sort(Enum.map(result, & &1["id"])) == Enum.to_list(0..(length(rows) - 1))
        scores = Enum.map(result, & &1["_distance"])
        assert scores == Enum.sort(scores)

        for row <- result do
          vector = Enum.at(vectors, row["id"])

          expected =
            Enum.zip(query, vector)
            |> Enum.map(fn {a, b} -> bxor(a, b) |> Integer.digits(2) |> Enum.sum() end)
            |> Enum.sum()

          assert row["_distance"] == expected
        end
      end)
    end
  end
end
