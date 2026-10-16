defmodule LanceDB.RefreshResult do
  @moduledoc """
  A completed SQL computed-column refresh, including a successful no-op.

  `rows_filled` is the SDK count of non-null values filled or recomputed in the
  requested column; expressions evaluating to null do not increase it. Refresh
  tracks freshness by fragment, so it may recompute unchanged rows too.
  `version` is the operation's last committed version, or its source version
  when nothing was committed. It is not a separate read of the latest version.
  """
  @enforce_keys [:rows_filled, :version]
  defstruct [:rows_filled, :version]

  @type t :: %__MODULE__{rows_filled: non_neg_integer(), version: pos_integer()}

  @doc false
  @spec from_wire(map()) :: t()
  def from_wire(result), do: struct!(__MODULE__, result)
end
