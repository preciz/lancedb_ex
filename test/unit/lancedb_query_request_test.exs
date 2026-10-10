defmodule LanceDB.QueryRequestTest do
  use ExUnit.Case, async: true
  use ExUnitProperties
  alias LanceDB.{Query, Table}
  alias LanceDB.Query.{Definition, Request}

  test "omitted controls retain source-specific defaults; explicit values survive composition" do
    scan = Query.new(%Table{resource: make_ref()})
    vector = Query.nearest_to(scan, [1, 0])
    assert Request.compile(scan.definition).request.limit == 100
    assert Request.compile(vector.definition).request.limit == 10
    assert scan.definition.controls == %{}
    assert Query.limit(scan, nil).definition.controls == %{limit: nil}

    assert Query.limit(scan, 7)
           |> Query.nearest_to([1, 0])
           |> Map.fetch!(:definition)
           |> Request.compile()
           |> Map.fetch!(:request)
           |> Map.fetch!(:limit) == 7
  end

  property "independent builder updates commute and preserve explicit controls" do
    check all(
            limit <- integer(1..1000),
            offset <- integer(0..1000),
            column <- member_of([:id, :text])
          ) do
      base = Query.new(%Table{resource: make_ref()})
      first = base |> Query.limit(limit) |> Query.offset(offset) |> Query.select([column])
      second = base |> Query.select([column]) |> Query.offset(offset) |> Query.limit(limit)
      assert first == second
      assert Request.compile(first.definition).request.limit == limit

      assert first.definition.controls == %{
               limit: limit,
               offset: offset,
               columns: [to_string(column)]
             }
    end
  end

  property "repeated builder updates follow a last-write-wins model and leave prior queries immutable" do
    update =
      one_of([
        map(one_of([constant(nil), integer(1..1000)]), &{:limit, &1}),
        map(integer(0..1000), &{:offset, &1}),
        map(boolean(), &{:with_row_id, &1}),
        map(one_of([constant(nil), member_of([[:id], [:text], [:id, :text]])]), &{:columns, &1})
      ])

    check all(updates <- list_of(update, min_length: 1, max_length: 25)) do
      base = Query.new(%Table{resource: make_ref()})

      Enum.reduce(updates, {base, %{}}, fn {key, value}, {prior, model} ->
        query = Query.options(prior, [{key, value}])

        normalized =
          if key == :columns and value != nil, do: Enum.map(value, &to_string/1), else: value

        next_model = Map.put(model, key, normalized)
        assert prior.definition.controls == model
        assert query.definition.controls == next_model
        request = Request.compile(query.definition).request
        assert request.limit == Map.get(next_model, :limit, 100)
        assert request.read.offset == Map.get(next_model, :offset, 0)
        assert request.read.with_row_id == Map.get(next_model, :with_row_id, false)
        assert request.projection.columns == next_model[:columns]
        {query, next_model}
      end)

      assert base.definition.controls == %{}
    end
  end

  property "refinement budgets accept the exact arithmetic boundary and reject overflow" do
    max = 9_223_372_036_854_775_807

    check all(factor <- integer(2..100), offset <- integer(0..1000)) do
      budget = div(max, factor)
      limit = budget - offset

      assert %Definition{} =
               Definition.new({:vector, [1, 0]},
                 limit: limit,
                 offset: offset,
                 refine_factor: factor
               )

      assert_raise ArgumentError, ~r/too large/, fn ->
        Definition.new({:vector, [1, 0]}, limit: limit + 1, offset: offset, refine_factor: factor)
      end
    end
  end

  test "normalization happens before lowering and invalid combinations fail during construction" do
    definition =
      Definition.new({:vector, [1, 0]}, distance: "cosine", order_by: [desc_nulls_first: :id])

    assert definition.controls.distance == :cosine
    assert Request.compile(definition).request.order_by == [["id", false, true]]

    for opts <- [
          [nprobes: 1, minimum_nprobes: 2],
          [fast_search: true, bypass_vector_index: true],
          [minimum_nprobes: 3, maximum_nprobes: 1],
          [columns: [:id], select: [id: "id"]],
          [filter: ""],
          [filter: 1],
          [limit: nil],
          [limit: 0]
        ] do
      assert_raise ArgumentError, fn -> Definition.new({:vector, [1, 0]}, opts) end
    end

    assert_raise ArgumentError, fn -> Definition.new(:scan, wand_factor: 1) end

    assert_raise ArgumentError, fn ->
      Definition.new({:hybrid, [1, 0], "term"}, reranker: fn _, _ -> [] end, rrf_k: 4)
    end
  end

  test "callbacks remain outside the native request" do
    callback = fn _, _ -> [] end

    plan =
      Definition.new({:hybrid, [1, 0], "term"}, reranker: callback, order_by: [asc: :id])
      |> Request.compile()

    assert plan.reranker == callback
    assert plan.request.controls.hybrid.custom
    refute Map.has_key?(plan.request, :reranker)
    assert is_binary(JSON.encode!(plan.request))
  end
end
