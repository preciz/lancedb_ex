defmodule LanceDB.IndexTest do
  use LanceDB.DataCase, async: true

  setup %{path: path} do
    {:ok, conn} = LanceDB.connect(path)
    %{conn: conn, path: path}
  end

  test("scalar index lifecycle, persistence and replacement", %{conn: conn, path: path}) do
    {:ok, table} =
      LanceDB.Connection.create_table(
        conn,
        "docs",
        for id <- 1..20 do
          %{id: id, category: rem(id, 2)}
        end
      )

    assert {:ok, []} = LanceDB.Index.list(table)
    assert {:ok, nil} = LanceDB.Index.stats(table, "missing")
    assert {:error, _} = LanceDB.Index.drop(table, "missing")
    assert {:error, _} = LanceDB.Index.create(table, :missing)
    assert {:ok, :created} = LanceDB.Index.create(table, :id, type: :btree, name: "ids")

    assert {:ok, :created} =
             LanceDB.Index.create(table, :category, type: :bitmap, name: "categories")

    assert {:ok, indices} = LanceDB.Index.list(table)
    assert Enum.sort(Enum.map(indices, & &1.name)) == ["categories", "ids"]
    assert Enum.find(indices, &(&1.name == "ids")).columns == ["id"]

    assert {:ok, %{"num_indexed_rows" => 20, "num_unindexed_rows" => 0, "loss" => nil}} =
             LanceDB.Index.stats(table, :ids)

    assert {:error, _} = LanceDB.Index.create(table, :id, type: :btree, name: "ids")

    assert {:ok, :created} =
             LanceDB.Index.create(table, :id, type: :btree, name: "ids", replace: true)

    assert {:ok, %{inserted: 1}} = LanceDB.Table.add(table, [%{id: 21, category: 0}])
    assert {:ok, %{"num_unindexed_rows" => 1}} = LanceDB.Index.stats(table, "ids")
    assert {:ok, [%{"id" => 21}]} = LanceDB.Query.query(table, filter: "id = 21")
    assert {:ok, other} = LanceDB.connect(path)
    assert {:ok, reopened} = LanceDB.Connection.open_table(other, "docs")
    assert {:ok, [_, _]} = LanceDB.Index.list(reopened)
    assert {:ok, :dropped} = LanceDB.Index.drop(table, :ids)
    assert {:ok, nil} = LanceDB.Index.stats(table, :ids)
    assert {:ok, :created} = LanceDB.Index.create(table, :id)
  end

  test("IVF flat indexes preserve nearest-neighbor search for each metric", %{conn: conn}) do
    rows =
      for id <- 1..32 do
        %{id: id, vector: [id / 32, 1.0]}
      end

    {:ok, table} = LanceDB.Connection.create_table(conn, "vectors", rows)

    for distance <- [:l2, :cosine, :dot] do
      assert {:ok, :created} =
               LanceDB.Index.create(table, :vector,
                 type: :ivf_flat,
                 distance: distance,
                 num_partitions: 1,
                 name: "vectors",
                 replace: true
               )

      assert {:ok, %{"num_indexed_rows" => 32, "distance_type" => metric}} =
               LanceDB.Index.stats(table, "vectors")

      assert String.downcase(metric) == Atom.to_string(distance)

      assert {:ok, [%{"id" => 32}]} =
               LanceDB.Query.search(table, [1, 1], distance: distance, limit: 1)
    end

    assert {:error, _} = LanceDB.Index.create(table, :id, type: :ivf_flat, num_partitions: 1)
  end

  test("IVF PQ index trains and can be searched", %{conn: conn}) do
    rows =
      for id <- 1..300 do
        %{id: id, vector: [id / 300, :math.sin(id)]}
      end

    {:ok, table} = LanceDB.Connection.create_table(conn, "pq", rows)

    assert {:ok, :created} =
             LanceDB.Index.create(table, :vector,
               type: :ivf_pq,
               num_partitions: 1,
               num_sub_vectors: 1
             )

    assert {:ok, [%LanceDB.Index.Description{name: name}]} = LanceDB.Index.list(table)
    assert {:ok, %{"num_indexed_rows" => 300, "loss" => loss}} = LanceDB.Index.stats(table, name)
    assert is_float(loss) and loss >= 0
    {:ok, reopened} = LanceDB.Connection.open_table(conn, "pq")
    assert {:ok, %{"loss" => ^loss}} = LanceDB.Index.stats(reopened, name)
    assert {:ok, [_]} = LanceDB.Query.search(table, [1, :math.sin(300)], limit: 1)
  end

  test "index option errors are caught before execution" do
    for opts <- [
          [type: :unknown],
          [replace: 1],
          [name: ""],
          [name: 1],
          [typo: 1],
          [distance: :bad],
          [distance: :l2],
          [type: :ivf_flat, num_sub_vectors: 1],
          [type: :ivf_flat, num_partitions: 0],
          [type: :ivf_pq, num_sub_vectors: 1.5],
          [type: :ivf_flat, num_partitions: 4_294_967_296]
        ] do
      assert_raise ArgumentError, fn -> LanceDB.Index.create(:unused, :id, opts) end
    end

    assert_raise ArgumentError, fn -> LanceDB.Index.create(:unused, "") end
    assert_raise ArgumentError, fn -> LanceDB.Index.stats(:unused, nil) end
    assert_raise ArgumentError, fn -> LanceDB.Index.drop(:unused, "") end
  end
end
