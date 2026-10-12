defmodule LanceDB.Query.Definition do
  @moduledoc false
  alias LanceDB.Query.Controls
  @enforce_keys [:source]
  defstruct [:source, controls: %{}]

  @type source ::
          :scan
          | {:vector, [number()]}
          | {:text, LanceDB.FTS.input()}
          | {:hybrid, [number()], LanceDB.FTS.input()}
          | {:batch | :multivector, [[number()]]}
          | {:take, :row_ids | :row_offsets, [non_neg_integer()]}
  @type t :: %__MODULE__{source: source(), controls: map()}

  @spec new(source(), keyword()) :: t()
  def new(source, opts \\ []), do: update(%__MODULE__{source: source!(source)}, opts)

  @spec update(t(), keyword()) :: t()
  def update(%__MODULE__{} = definition, opts) do
    controls = Map.merge(definition.controls, Controls.normalize!(opts))
    Controls.validate!(definition.source, controls)
    %{definition | controls: controls}
  end

  @spec source(t(), source()) :: t()
  def source(%__MODULE__{} = definition, source) do
    source = source!(source)
    Controls.validate!(source, definition.controls)
    %{definition | source: source}
  end

  defp source!(:scan), do: :scan
  defp source!({:vector, vector}), do: {:vector, vector!(vector)}
  defp source!({:text, text}), do: {:text, text!(text)}
  defp source!({:hybrid, vector, text}), do: {:hybrid, vector!(vector), text!(text)}

  defp source!({kind, vectors}) when kind in [:batch, :multivector] do
    if !(is_list(vectors) and vectors != []),
      do: raise(ArgumentError, "query vectors must be a non-empty list of vectors")

    vectors = Enum.map(vectors, &vector!/1)

    if !Enum.all?(vectors, &(length(&1) == length(hd(vectors)))),
      do: raise(ArgumentError, "query vectors must have the same dimension")

    {kind, vectors}
  end

  defp source!({:take, kind, values}) when kind in [:row_ids, :row_offsets] do
    if !(is_list(values) and
           Enum.all?(values, &(is_integer(&1) and &1 >= 0 and &1 <= 18_446_744_073_709_551_615))),
       do: raise(ArgumentError, "#{kind} must be a list of non-negative uint64 integers")

    {:take, kind, values}
  end

  defp source!(_), do: raise(ArgumentError, "invalid query source")

  defp vector!(vector) do
    if !(is_list(vector) and vector != [] and
           Enum.all?(vector, &(is_number(&1) and abs(&1) <= 1.797_693_134_862_315_7e308))),
       do:
         raise(
           ArgumentError,
           "query_vector must be a non-empty list of finite float64-compatible numbers"
         )

    vector
  end

  defp text!(text) do
    LanceDB.FTS.query!(text)
    text
  end
end
