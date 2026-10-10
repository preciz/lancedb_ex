defmodule LanceDB do
  @moduledoc """
  Embedded LanceDB for Elixir. Use Connection for databases, Table for data and
  history, Query for reads, and Index for indexes. Source describes write input.
  """
  @type result(value) :: {:ok, value} | {:error, LanceDB.Error.t()}
  @doc "Connects to local or object storage. See LanceDB.Connection.connect/3."
  @spec connect(String.t(), map(), keyword()) :: result(LanceDB.Connection.t())
  defdelegate connect(uri \\ "./lancedb", storage_options \\ %{}, opts \\ []),
    to: LanceDB.Connection
end
