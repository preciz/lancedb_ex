defmodule LanceDB.Query.Request do
  @moduledoc false
  alias LanceDB.Query.{Controls, Definition, Plan}

  # Lower semantic values in one direction; no keyword reconstruction from requests.
  @spec compile(Definition.t()) :: Plan.t()
  def compile(%Definition{source: source, controls: c}) do
    kind = if source == :scan, do: :scan, else: elem(source, 0)
    limit = Map.get(c, :limit, Controls.default_limit(kind))

    base = %{
      projection: %{
        columns: c[:columns],
        select: if(c[:select], do: Enum.map(c.select, &Tuple.to_list/1))
      },
      read: %{
        offset: Map.get(c, :offset, 0),
        prefilter: Map.get(c, :prefilter, true),
        fast_search: Map.get(c, :fast_search, false),
        with_row_id: Map.get(c, :with_row_id, false)
      },
      execution: %{timeout_ms: c[:timeout_ms]},
      order_by:
        Enum.map(Map.get(c, :order_by, []), fn {direction, column} ->
          [
            column,
            direction in [:asc, :asc_nulls_first, :asc_nulls_last],
            direction in [:asc_nulls_first, :desc_nulls_first]
          ]
        end)
    }

    request = lower(source, Map.merge(base, %{limit: limit, filter: c[:filter]}), c)
    Plan.new(request, Map.get(c, :reranker, :rrf))
  end

  defp lower(:scan, base, _), do: Map.put(base, :kind, :scan)

  defp lower({:take, kind, values}, base, _) do
    base
    |> Map.drop([:limit, :filter])
    |> Map.merge(%{
      kind: :take,
      row_ids: if(kind == :row_ids, do: values),
      row_offsets: if(kind == :row_offsets, do: values)
    })
  end

  defp lower({:text, text}, base, c),
    do:
      Map.merge(base, %{
        kind: :text,
        text: LanceDB.FTS.query!(text),
        column: c[:text_column],
        wand_factor: c[:wand_factor]
      })

  defp lower({:hybrid, vector, text}, base, c) do
    hybrid = %{
      query: LanceDB.FTS.query!(text),
      column: c[:text_column],
      wand_factor: c[:wand_factor],
      rrf_k: Map.get(c, :rrf_k, 60.0),
      normalize: Map.get(c, :normalize, :score),
      custom: Map.get(c, :reranker, :rrf) != :rrf
    }

    vector(base, vector, c, hybrid)
  end

  defp lower({:vector, values}, base, c), do: vector(base, values, c, nil)

  defp lower({kind, [first | rest]}, base, c) when kind in [:batch, :multivector] do
    c = if kind == :multivector, do: Map.put_new(c, :distance, :cosine), else: c

    vector(base, first, c, nil)
    |> Map.merge(%{search_mode: kind, additional_vectors: Enum.map(rest, &numbers(&1, c))})
  end

  defp vector(base, values, c, hybrid) do
    probes =
      if Map.has_key?(c, :minimum_nprobes) or Map.has_key?(c, :maximum_nprobes),
        do: %{
          minimum: Map.get(c, :minimum_nprobes, 20),
          maximum: Map.get(c, :maximum_nprobes, 20)
        }

    Map.merge(base, %{
      kind: :vector,
      vector: numbers(values, c),
      distance_type: to_string(Map.get(c, :distance, :l2)),
      controls: %{
        vector_column: c[:vector_column],
        nprobes: c[:nprobes],
        ef: c[:ef],
        refine_factor: c[:refine_factor],
        bypass_vector_index: Map.get(c, :bypass_vector_index, false),
        hybrid: hybrid,
        probes: probes,
        distance_range: if(c[:distance_range], do: Tuple.to_list(c.distance_range)),
        approx_mode: c[:approx_mode]
      }
    })
  end

  defp numbers(values, %{distance: :hamming}), do: values
  defp numbers(values, _), do: Enum.map(values, &(&1 / 1))
end
