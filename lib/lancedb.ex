defmodule LanceDB do
  @moduledoc """
  Embedded LanceDB for Elixir. Use `LanceDB.Connection` for databases,
  `LanceDB.Table` for rows and cache statistics, `LanceDB.Query` for reads,
  and `LanceDB.Index` for indexes. `LanceDB.History` and `LanceDB.Branch` manage
  versions and branches. `LanceDB.Source` describes write input.
  """
  @type result(value) :: {:ok, value} | {:error, LanceDB.Error.t()}
  @doc "Connects to local or object storage. See LanceDB.Connection.connect/3."
  @spec connect(String.t(), map(), keyword()) :: result(LanceDB.Connection.t())
  defdelegate connect(uri \\ "./lancedb", storage_options \\ %{}, opts \\ []),
    to: LanceDB.Connection
end
