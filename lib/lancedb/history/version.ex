defmodule LanceDB.History.Version do
  @moduledoc "A historical table version; timestamp is the SDK UTC timestamp string and metadata retains string keys."
  defstruct [:version, :timestamp, :metadata]

  @type t :: %__MODULE__{
          version: pos_integer(),
          timestamp: String.t(),
          metadata: %{String.t() => String.t()}
        }
end
