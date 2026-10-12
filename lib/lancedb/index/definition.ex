defmodule LanceDB.Index.Definition do
  alias LanceDB.Index.Options
  @moduledoc "A reusable index configuration. Construct through LanceDB.Index.new/1."
  @enforce_keys [:options]
  defstruct [:options]
  @opaque t :: %__MODULE__{options: map()}

  @doc false
  @spec new(keyword() | t()) :: t()
  def new(opts \\ [])
  def new(%__MODULE__{} = definition), do: definition

  def new(opts), do: %__MODULE__{options: Options.parse!(opts)}
end
