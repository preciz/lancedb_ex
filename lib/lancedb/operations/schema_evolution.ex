defmodule LanceDB.Operations.SchemaEvolution do
  alias LanceDB.Backend.SchemaCodec
  @moduledoc false
  alias LanceDB.{Backend, Batch, Ingestion, Options, Query, Schema}
  import LanceDB.Options, only: [column_name!: 1, column_names!: 1]

  @spec add_columns(LanceDB.Table.t(), map() | list()) ::
          LanceDB.Native.result(LanceDB.Native.version_output())
  def add_columns(table, expressions) do
    expressions = evolution_pairs!(expressions)

    expressions =
      Enum.map(expressions, fn {name, expression} ->
        name = evolution_name!(name)
        expression = Options.filter!(expression)

        if !(is_binary(expression) and String.valid?(expression) and
               String.trim(expression) != ""),
           do: raise(ArgumentError, "column expressions must be non-empty SQL strings")

        {name, expression}
      end)

    Backend.add_columns(table, expressions)
  end

  @spec alter_columns(LanceDB.Table.t(), map() | list()) ::
          LanceDB.Native.result(LanceDB.Native.version_output())
  def alter_columns(table, changes) do
    changes = evolution_pairs!(changes)

    changes =
      Enum.map(changes, fn {name, opts} ->
        opts = Keyword.validate!(opts, [:rename, :nullable, :type])

        if opts == [],
          do: raise(ArgumentError, "each alteration must specify rename, nullable, or type")

        rename =
          if Keyword.has_key?(opts, :rename), do: evolution_name!(Keyword.fetch!(opts, :rename))

        nullable = Keyword.get(opts, :nullable)

        if Keyword.has_key?(opts, :nullable) and not is_boolean(nullable),
          do: raise(ArgumentError, "nullable must be a boolean")

        %{path: name, rename: rename, nullable: nullable, type: alteration_type!(opts)}
      end)

    Backend.alter_columns(table, changes)
  end

  @spec drop_columns(LanceDB.Table.t(), [atom() | String.t()]) ::
          LanceDB.Native.result(LanceDB.Native.version_output())
  def drop_columns(table, columns) do
    columns = column_names!(columns)
    Backend.drop_columns(table, columns)
  end

  defp alteration_type!(opts) do
    if Keyword.has_key?(opts, :type),
      do: SchemaCodec.type_definition!(Keyword.fetch!(opts, :type))
  end

  defp evolution_pairs!(pairs) when is_map(pairs) and not is_struct(pairs),
    do: evolution_pairs!(Map.to_list(pairs))

  defp evolution_pairs!([_ | _] = pairs) do
    pairs =
      Enum.map(pairs, fn
        {name, value} -> {column_name!(name), value}
        _ -> raise ArgumentError, "column changes must be name/value pairs"
      end)

    column_names!(Enum.map(pairs, &elem(&1, 0)))
    pairs
  end

  defp evolution_pairs!(_),
    do: raise(ArgumentError, "column changes must be a non-empty map or list")

  defp evolution_name!(name) do
    name = column_name!(name)

    if String.contains?(name, "."),
      do: raise(ArgumentError, "new column names and rename targets must not contain dots")

    name
  end

  @spec columns(LanceDB.Table.t(), LanceDB.Source.t(), keyword()) ::
          LanceDB.Backend.result(LanceDB.Backend.write_output())
          | {:error, map() | LanceDB.Error.t() | String.t()}
  def columns(table, source, opts) do
    source = LanceDB.Source.coerce(source)
    if is_nil(source.schema), do: raise(ArgumentError, "column sources require a schema")
    add_columns_batches(table, LanceDB.Source.chunks(source), source.schema, opts)
  end

  defp add_columns_batches(table, batches, schema, opts) do
    opts = Keyword.validate!(opts, [:expected_version])

    version =
      if Keyword.has_key?(opts, :expected_version),
        do: Options.version!(opts[:expected_version])

    wire = SchemaCodec.encode!(schema)

    Ingestion.run_operation(
      fn -> Backend.columns_stream_open(table, wire, version) end,
      batches
    )
  end

  @spec backfill_columns(
          LanceDB.Table.t(),
          Schema.t() | list(),
          (Batch.t() -> Batch.t() | [map()]),
          keyword()
        ) ::
          LanceDB.Backend.result(LanceDB.Backend.write_output())
          | {:error, map() | LanceDB.Error.t() | String.t()}
  def backfill_columns(table, schema, mapper, opts) do
    if not is_function(mapper, 1),
      do: raise(ArgumentError, "backfill mapper must be a function of arity one")

    opts = Keyword.validate!(opts, [:read_columns, :batch_size])
    {size, opts} = Options.batch_size!(opts)

    columns =
      if Keyword.has_key?(opts, :read_columns), do: Options.column_names!(opts[:read_columns])

    schema = schema!(schema)
    query_opts = if columns, do: [limit: nil, columns: columns], else: [limit: nil]

    with {:ok, version} <- Backend.table_version(table),
         {:ok, snapshot} <- Backend.snapshot_table(table, version, nil) do
      batches = snapshot |> Query.new(query_opts) |> Query.stream_batches(batch_size: size)
      output = mapped_batches(batches, mapper)
      add_columns_batches(table, output, schema, expected_version: version)
    end
  end

  defp mapped_batches(batches, mapper), do: Stream.map(batches, &map_batch(&1, mapper))

  defp map_batch(batch, mapper) do
    output = mapper.(batch)

    count =
      case output do
        %Batch{} -> Batch.size(output)
        rows when is_list(rows) -> length(rows)
        _ -> raise ArgumentError, "backfill mapper must return a Batch or list of rows"
      end

    if count != Batch.size(batch),
      do: raise(ArgumentError, "backfill mapper must preserve row count and order")

    output
  end

  defp schema!(%Schema{} = schema), do: schema
  defp schema!(fields), do: Schema.new(fields)
end
