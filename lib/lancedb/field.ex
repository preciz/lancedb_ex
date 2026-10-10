defmodule LanceDB.Field do
  @moduledoc "A schema field. Names stay strings; nested list items retain their full field definition."
  @enforce_keys [:name, :type]
  defstruct [:name, :type, nullable: true, metadata: %{}]

  @type t :: %__MODULE__{
          name: String.t(),
          type: LanceDB.Schema.logical_type(),
          nullable: boolean(),
          metadata: map()
        }
end
