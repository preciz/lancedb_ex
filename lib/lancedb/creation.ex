defmodule LanceDB.Creation do
  alias LanceDB.Backend.SchemaCodec
  alias LanceDB.Creation.Options, as: CreationOptions
  @moduledoc false
  alias LanceDB.{Backend, Error, Ingestion, Options, Records, Schema}

  def create(conn, name, input, opts) do
    opts = CreationOptions.parse!(opts)
    name = Options.string!(name, "table name")
    source = LanceDB.Source.with_schema(LanceDB.Source.coerce(input), Keyword.get(opts, :schema))

    if is_nil(source.schema) and not is_list(input),
      do: raise(ArgumentError, "creation sources require a schema")

    create = fn ->
      with {:ok, source} <- prepare(source) do
        create_batches(conn, name, LanceDB.Source.chunks(source), source.schema, opts)
      end
    end

    if Keyword.get(opts, :mode) == :exist_ok,
      do: create_or_open(conn, name, create, opts),
      else: create.()
  end

  defp prepare(%LanceDB.Source{schema: nil, data: rows}) do
    with {:ok, records} <- Records.prepare(rows, :infer),
         {:ok, batch} <- Backend.batch_infer(records),
         {:ok, schema} <- batch |> Backend.batch_schema() |> SchemaCodec.decode() do
      {:ok, LanceDB.Source.batches([batch], schema: schema)}
    end
  end

  defp prepare(source), do: {:ok, source}

  defp create_batches(conn, name, batches, schema, opts) do
    name = Options.string!(name, "table name")
    schema = schema!(schema)

    metadata =
      Map.merge(
        schema.metadata,
        Keyword.get(opts, :schema_metadata, %{})
      )

    wire = SchemaCodec.encode!(schema)

    native_options = CreationOptions.native_options(opts)

    Ingestion.run_operation(
      fn ->
        Backend.create_stream_open(
          conn,
          name,
          wire,
          metadata,
          native_options
        )
      end,
      batches
    )
  end

  defp create_or_open(conn, name, create, opts) do
    open_options = CreationOptions.open_options(opts)

    with {:error, %{kind: :not_found}} <- Backend.open_table(conn, name, open_options),
         {:error, reason} <- create.() do
      case Error.normalize(reason, nil) do
        %Error{kind: :already_exists} -> Backend.open_table(conn, name, open_options)
        error -> {:error, error}
      end
    end
  end

  defp schema!(%Schema{} = schema), do: schema
  defp schema!(fields), do: Schema.new(fields)
end
