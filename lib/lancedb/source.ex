defmodule LanceDB.Source do
  alias LanceDB.Backend.SchemaCodec

  @moduledoc """
  Explicit input for creation, writes, merges and column backfills.

  `rows/2` consumes an enumerable of maps in bounded chunks. `batches/2` consumes
  native Arrow batches without converting them to rows. Supply `schema:` when
  creating a table, either on the Source or as the creation operation's option;
  existing-table writes use the stored schema and reject incompatible explicitly
  supplied source fields. Conflicting source and creation schemas raise before
  input is consumed. Sources are lazy and run in the calling process. Reuse
  requires a repeatable enumerable.
  Commit policy belongs to the write operation, independently of chunk size.
  """
  alias LanceDB.{Batch, Schema}
  @enforce_keys [:kind, :data]
  defstruct [:kind, :data, :schema, batch_size: 1024]

  @opaque t :: %__MODULE__{
            kind: :rows | :batches,
            data: Enumerable.t(),
            schema: Schema.t() | nil,
            batch_size: pos_integer()
          }

  @doc "Describes row input. Options: schema and batch_size (default 1024)."
  @spec rows(Enumerable.t(), keyword()) :: t()
  def rows(data, opts \\ []) do
    {size, opts} = LanceDB.Options.batch_size!(opts)
    %__MODULE__{kind: :rows, data: enumerable!(data), schema: schema!(opts), batch_size: size}
  end

  @doc "Describes an enumerable of Batch values. Accepts an optional schema."
  @spec batches(Enumerable.t(), keyword()) :: t()
  def batches(data, opts \\ []),
    do: %__MODULE__{kind: :batches, data: enumerable!(data), schema: schema!(opts)}

  @doc false
  def coerce(%__MODULE__{} = source), do: source
  def coerce(rows) when is_list(rows), do: rows(rows)
  def coerce(_), do: raise(ArgumentError, "input must be a row list or a LanceDB.Source")

  @doc false
  def chunks(%__MODULE__{kind: :rows, data: data, batch_size: size}),
    do: Stream.chunk_every(data, size)

  def chunks(%__MODULE__{kind: :batches, data: data}) do
    Stream.map(data, fn
      %Batch{} = batch -> batch
      _ -> raise ArgumentError, "batch sources must contain LanceDB.Batch values"
    end)
  end

  @doc false
  def with_schema(%__MODULE__{} = source, nil), do: source

  def with_schema(%__MODULE__{} = source, schema) do
    schema = schema!(schema: schema)

    if source.schema != nil and source.schema != schema,
      do: raise(ArgumentError, "source and operation schemas conflict")

    %{source | schema: schema}
  end

  @doc false
  def check_schema(_table, %__MODULE__{schema: nil}), do: :ok

  def check_schema(table, %__MODULE__{schema: schema}) do
    with {:ok, json} <- LanceDB.Backend.get_schema_json(table),
         {:ok, stored} <- SchemaCodec.decode(json) do
      if schema.fields == stored.fields,
        do: :ok,
        else:
          {:error,
           %LanceDB.Error{
             kind: :invalid_data,
             message: "Source fields do not match destination schema",
             details: %{phase: :validation, commit_outcome: :not_started}
           }}
    end
  end

  defp schema!(opts) do
    opts = Keyword.validate!(opts, [:schema])

    case Keyword.get(opts, :schema) do
      nil ->
        nil

      %Schema{} = schema ->
        schema

      %LanceDB.ArrowSchema{} ->
        raise ArgumentError,
              "convert ArrowSchema.to_schema/1 before using an output schema for writes"

      fields ->
        Schema.new(fields)
    end
  end

  defp enumerable!(data) do
    if Enumerable.impl_for(data),
      do: data,
      else: raise(ArgumentError, "source must be enumerable")
  end
end
