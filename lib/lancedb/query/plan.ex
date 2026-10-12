defmodule LanceDB.Query.Plan do
  @moduledoc false
  @enforce_keys [:request]
  defstruct [:request, reranker: :rrf]

  @type t :: %__MODULE__{request: map(), reranker: LanceDB.Reranker.t()}

  @spec new(map(), LanceDB.Reranker.t()) :: t()
  def new(request, reranker), do: %__MODULE__{request: request, reranker: reranker}
  @spec custom?(t()) :: boolean()
  def custom?(%__MODULE__{reranker: reranker}), do: is_function(reranker, 2)
end
