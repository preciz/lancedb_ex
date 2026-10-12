defmodule LanceDB.Batch do
  alias LanceDB.Backend.SchemaCodec

  @moduledoc """
  An immutable native Arrow record batch. Batches may be shared between processes.

  `Query.stream_batches/2` and `Table.add/3` with `Source.batches/2` transfer compatible batches without row
  serialization. Conversion to Elixir maps is explicit through `to_rows/1`.
  Retaining a batch retains its Arrow buffers, including buffers shared by slices.
  """
  alias LanceDB.{Backend, Records, Result, Schema}
  @enforce_keys [:resource]
  defstruct [:resource]
  @opaque t :: %__MODULE__{resource: reference()}

  @doc "Encodes rows against an explicit schema."
  @spec from_rows([map()], Schema.t()) :: LanceDB.result(t())
  def from_rows(rows, %Schema{} = schema) do
    LanceDB.Execution.run(:batch_from_rows, fn ->
      with {:ok, records_wire} <- Records.prepare(rows, :typed, true),
           do:
             Backend.batch_from_terms(
               records_wire,
               SchemaCodec.encode!(schema),
               schema.metadata
             )
    end)
  end

  @doc "Converts the batch into string-keyed Elixir row maps."
  @spec to_rows(t()) :: LanceDB.result([map()])
  def to_rows(%__MODULE__{} = batch) do
    LanceDB.Execution.run(:batch_rows, fn ->
      with {:ok, json} <- Backend.batch_rows(batch), do: {:ok, Result.rows!(json)}
    end)
  end

  @doc "Returns the number of rows without converting the batch."
  @spec size(t()) :: non_neg_integer()
  def size(%__MODULE__{} = batch), do: Backend.batch_size(batch)
  @doc "Returns the physical ArrowSchema; convert explicitly before table creation."
  @spec schema(t()) :: {:ok, LanceDB.ArrowSchema.t()} | {:error, JSON.decode_error_reason()}
  def schema(%__MODULE__{} = batch),
    do: batch |> Backend.batch_schema() |> SchemaCodec.decode(:arrow)

  @doc "Exports one batch as a standard Arrow IPC stream binary."
  @spec to_ipc(t()) :: LanceDB.result(binary())
  def to_ipc(%__MODULE__{} = batch),
    do: LanceDB.Execution.run(:batch_to_ipc, fn -> Backend.batch_to_ipc(batch) end)

  @doc "Imports every batch in an Arrow IPC stream binary; input is materialized in memory."
  @spec from_ipc(binary()) :: LanceDB.result([t()])
  def from_ipc(binary) when is_binary(binary),
    do: LanceDB.Execution.run(:batch_from_ipc, fn -> Backend.batch_from_ipc(binary) end)
end
