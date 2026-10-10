defmodule LanceDB.NestedOrderingPropertyTest do
  use ExUnit.Case, async: true
  use ExUnitProperties
  import LanceDB.TestSupport, only: [with_database: 1]
  alias LanceDB.{Connection, Index, Table, TestSupport}

  @directions [:asc_nulls_first, :asc_nulls_last, :desc_nulls_first, :desc_nulls_last]

  test "nested ordering preserves nulls and quoted path segments across query modes" do
    for direction <- @directions do
      assert_ordering([3, nil, -1, :root_null, 3, :child_null, 0], direction,
        root: "data root",
        child: "rank` score",
        leaf: "score.with.dot",
        path: "`data root`.`rank`` score`.`score.with.dot`"
      )
    end
  end

  property "nested sorting and projections agree with an independent null and tie ordering model" do
    check all(
            values <-
              list_of(one_of([integer(-5..5), member_of([nil, :root_null, :child_null])]),
                max_length: 8
              ),
            direction <- member_of(@directions),
            max_runs: 15
          ) do
      assert_ordering(values, direction,
        root: "data",
        child: "rank",
        leaf: "score",
        path: "data.rank.score"
      )
    end
  end

  defp assert_ordering(values, direction, names) do
    rows =
      Enum.with_index(values, 1)
      |> Enum.map(fn {value, id} ->
        nested =
          case value do
            :root_null -> nil
            :child_null -> %{names[:child] => nil}
            score -> %{names[:child] => %{names[:leaf] => score}}
          end

        %{:id => id, :text => "elixir", :vector => [0, 0], names[:root] => nested}
      end)

    expected = expected_ids(values, direction)

    with_database(fn conn ->
      schema = [
        {:id, :int64},
        {:text, :string},
        {:vector, {:vector, 2}},
        {names[:root], {:struct, [{names[:child], {:struct, [{names[:leaf], :int64}]}}]}}
      ]

      assert {:ok, table} = Connection.create_empty_table(conn, "nested", schema)
      if rows != [], do: assert({:ok, _} = Table.add(table, rows))
      assert {:ok, :created} = Index.create(table, :text, type: :fts)

      callback = fn vector, text ->
        Enum.uniq_by(vector ++ text, & &1["_rowid"])
        |> Enum.map(&Map.put(&1, "_relevance_score", 1))
      end

      for {projection, expected} <- [
            {[], expected},
            {[columns: [:id]], expected},
            {[select: [id: "-id"]], Enum.map(expected, &(-&1))}
          ],
          source <- [
            [],
            [vector: [0, 0]],
            [text: "elixir"],
            [vector: [0, 0], text: "elixir"],
            [vector: [0, 0], text: "elixir", reranker: callback]
          ] do
        opts =
          source ++
            projection ++
            [limit: max(length(rows), 1), order_by: [{direction, names[:path]}, asc: :id]]

        assert {:ok, result} = table |> TestSupport.query(opts) |> LanceDB.Query.all()
        assert Enum.map(result, & &1["id"]) == expected
        assert Enum.to_list(TestSupport.rows(table, opts ++ [batch_size: 2])) == result

        assert_projection(result, projection)
      end
    end)
  end

  defp assert_projection(_rows, []), do: :ok

  defp assert_projection(rows, _projection) do
    for row <- rows do
      assert Map.keys(row) -- ~w(id _distance _score _relevance_score) == []
    end
  end

  defp expected_ids(values, direction) do
    ascending? = direction in [:asc_nulls_first, :asc_nulls_last]
    nulls_first? = direction in [:asc_nulls_first, :desc_nulls_first]

    values
    |> Enum.with_index(1)
    |> Enum.sort_by(&ordering_key(&1, ascending?, nulls_first?))
    |> Enum.map(&elem(&1, 1))
  end

  defp ordering_key({value, id}, ascending?, nulls_first?) when is_integer(value),
    do: {if(nulls_first?, do: 1, else: 0), if(ascending?, do: value, else: -value), id}

  defp ordering_key({_null, id}, _ascending?, nulls_first?),
    do: {if(nulls_first?, do: 0, else: 1), 0, id}
end
