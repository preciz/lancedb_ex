defmodule LanceDB.WriteResult do
  @moduledoc """
  A completed write. Unavailable counts and commit versions are nil; zero means
  measured zero. Insertion reports confirmed `commits` independently of input chunk
  size; other operations leave it nil when the SDK does not expose that count.
  """
  defstruct [:operation, :version, :inserted, :updated, :deleted, :attempts, :commits]

  @doc false
  def counts(%__MODULE__{} = value, operation), do: %{value | operation: operation}

  def counts({:write, value}, operation),
    do: struct!(__MODULE__, Map.put(value, :operation, operation))

  @doc false
  def version({:version, value}, operation), do: %__MODULE__{operation: operation, version: value}

  @type t :: %__MODULE__{
          operation: atom(),
          version: non_neg_integer() | nil,
          inserted: non_neg_integer() | nil,
          updated: non_neg_integer() | nil,
          deleted: non_neg_integer() | nil,
          attempts: non_neg_integer() | nil,
          commits: non_neg_integer() | nil
        }
end
