defmodule LanceDB.VectorOraclePropertyTest do
  use ExUnit.Case, async: true
  use ExUnitProperties
  import LanceDB.TestSupport, only: [with_database: 1]
  alias LanceDB.{Connection, Query}

  property "exhaustive L2, dot and cosine searches return every expected candidate and distance" do
    check all(
            dimension <- integer(2..6),
            vectors <-
              list_of(list_of(integer(-8..8), length: dimension - 1),
                min_length: 1,
                max_length: 10
              ),
            query <- list_of(integer(-8..8), length: dimension - 1),
            max_runs: 30
          ) do
      # A final nonzero component avoids undefined cosine similarity and narrows
      # the property to supported vectors without filtering or discarded cases.
      vectors = Enum.map(vectors, &(&1 ++ [1]))
      query = query ++ [1]
      rows = Enum.with_index(vectors, fn vector, id -> %{id: id, vector: vector} end)

      with_database(fn conn ->
        assert {:ok, table} =
                 Connection.create_table(conn, "vectors", rows,
                   schema: [id: :int64, vector: {:vector, dimension, :float64}]
                 )

        for metric <- [:l2, :dot, :cosine] do
          assert {:ok, found} =
                   Query.search(table, query,
                     distance: metric,
                     limit: length(rows),
                     bypass_vector_index: true
                   )

          assert Enum.sort(Enum.map(found, & &1["id"])) == Enum.to_list(0..(length(rows) - 1))
          scores = Enum.map(found, & &1["_distance"])
          assert scores == Enum.sort(scores)

          for row <- found do
            expected = distance(metric, query, Enum.at(vectors, row["id"]))
            assert_in_delta row["_distance"], expected, 1.0e-5
          end
        end
      end)
    end
  end

  defp distance(:l2, left, right),
    do: Enum.zip_with(left, right, &((&1 - &2) * (&1 - &2))) |> Enum.sum()

  defp distance(:dot, left, right), do: 1 - dot(left, right)

  defp distance(:cosine, left, right),
    do: 1 - dot(left, right) / :math.sqrt(dot(left, left) * dot(right, right))

  defp dot(left, right), do: Enum.zip_with(left, right, &(&1 * &2)) |> Enum.sum()
end
