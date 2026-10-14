defmodule LanceDB.FTS.Prepared do
  @moduledoc """
  Reusable analyzed terms returned by `LanceDB.FTS.prepare/2` and `/3`.

  Terms retain their original spellings and analyzed form, grouped by analyzed
  text in first-occurrence order. Removed terms are omitted. Preparation does
  not bind a query column or retain a table handle. Use the value with an index
  using the same analyzer settings, and prepare again after those settings change.
  """
  @enforce_keys [:terms]
  defstruct [:terms]
  @type prepared_term :: %{analyzed: String.t(), originals: nonempty_list(String.t())}
  @opaque t :: %__MODULE__{terms: [prepared_term()]}

  @doc "Returns distinct analyzed terms and their original spellings in occurrence order."
  @spec terms(t()) :: [prepared_term()]
  def terms(%__MODULE__{terms: terms}), do: terms

  @doc "Counts distinct analyzed terms after filtering and deduplication."
  @spec term_count(t()) :: non_neg_integer()
  def term_count(%__MODULE__{terms: terms}), do: length(terms)

  @doc false
  @spec new([%{String.t() => String.t()}]) :: t()
  def new(tokens) do
    originals = Enum.group_by(tokens, & &1["analyzed"], & &1["original"])

    terms =
      tokens
      |> Enum.uniq_by(& &1["analyzed"])
      |> Enum.map(fn token ->
        text = token["analyzed"]
        %{analyzed: text, originals: Enum.uniq(Map.fetch!(originals, text))}
      end)

    %__MODULE__{terms: terms}
  end
end
