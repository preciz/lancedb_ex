defmodule LanceDB.Blob.Reference do
  @moduledoc "A blob write value referring to an external object's URI. Construct with Blob.reference/1."
  @enforce_keys [:uri]
  defstruct [:uri]
  @opaque t :: %__MODULE__{uri: String.t()}
end
