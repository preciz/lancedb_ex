defmodule LanceDB.Reranker do
  @moduledoc """
  Custom hybrid rerankers receive two lists of candidate maps: vector results
  with normalized `_distance` (smaller is better), and text results with
  normalized `_score` (larger is better). Both include `_rowid`, independently
  of `with_row_id`, and the requested data columns or computed projections.

  Pass a two-argument function or a module implementing `rerank/2` through
  `reranker:`. Return a list of maps containing `_rowid` and `_relevance_score`,
  or `{:ok, list}` / `{:error, reason}`. String or atom keys are accepted. Extra
  fields are ignored: returned data always comes from the original candidates.
  Row IDs must be unique and belong to either channel; scores must be finite
  float32-compatible numbers. An empty list produces no results.

  The returned order is authoritative. Sort by your scores in the callback if
  desired. Explicit `order_by` overrides this order; pagination follows reranking
  and ordering. Candidates are limited to `limit + offset` per channel.

  Callbacks run in a separate process. Exceptions, throws and exits become error
  tuples; the query timeout includes candidate retrieval, callback execution,
  and final result processing. Streaming invokes the callback on enumeration
  and gathers both candidate sets before yielding rows.
  """

  @callback rerank([map()], [map()]) :: [map()] | {:ok, [map()]} | {:error, term()}

  @doc false
  def validate!(:rrf), do: :rrf
  def validate!(fun) when is_function(fun, 2), do: fun

  def validate!(module) when is_atom(module) do
    if Code.ensure_loaded?(module) and function_exported?(module, :rerank, 2),
      do: &module.rerank/2,
      else:
        raise(ArgumentError, "reranker must be :rrf, a function of arity 2, or a rerank/2 module")
  end

  def validate!(_),
    do: raise(ArgumentError, "reranker must be :rrf, a function of arity 2, or a rerank/2 module")
end
