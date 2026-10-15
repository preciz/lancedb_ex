defmodule LanceDB.ArrowSchema do
  @moduledoc """
  A physical Arrow output description. Fields share the Schema/Field vocabulary,
  but may contain blob descriptors, unsupported external types, or field properties
  that the storage engine cannot persist. Inspection never silently drops them.

  Use `to_schema/1` to validate and explicitly convert a description into a table
  definition. Blob descriptors require fetching payloads and declaring `:blob`;
  they cannot be converted into writable payload fields automatically.
  """
  alias LanceDB.{Backend, Error, Field, Options, Schema}
  alias LanceDB.Backend.SchemaCodec
  defstruct fields: [], metadata: %{}
  @type t :: %__MODULE__{fields: [LanceDB.Field.t()], metadata: map()}

  @doc "Validates storage compatibility, returning a logical Schema or an Error. Malformed descriptions raise ArgumentError."
  @spec to_schema(t()) :: LanceDB.result(Schema.t())
  def to_schema(%__MODULE__{} = arrow) do
    LanceDB.Execution.run(:schema_conversion, fn ->
      validate_description!(arrow)
      convert(arrow)
    end)
  end

  defp convert(arrow) do
    schema = Schema.new(SchemaCodec.logical_fields(arrow.fields), arrow.metadata)

    with {:ok, _} <- Backend.schema_validate(SchemaCodec.encode_description(arrow.fields)),
         do: {:ok, schema}
  rescue
    error in ArgumentError ->
      {:error,
       %Error{
         kind: :unsupported,
         operation: :schema_conversion,
         message: Exception.message(error)
       }}
  end

  defp validate_description!(arrow) do
    fields = validation_fields!(arrow.fields)
    Schema.new(fields, arrow.metadata)
    :ok
  end

  defp validation_fields!(fields) when is_list(fields),
    do: Enum.map(fields, &validation_field!/1)

  defp validation_fields!(_), do: raise(ArgumentError, "Arrow schema fields must be a list")

  defp validation_field!(%Field{} = field),
    do: %{field | type: validation_type!(field.type)}

  defp validation_field!(_), do: raise(ArgumentError, "Arrow schema fields must be Field structs")

  defp validation_type!({:unsupported, description}) do
    Options.string!(description, "unsupported Arrow type description")
    # Check the surrounding structure with the ordinary schema validator;
    # the original unsupported type is retained for conversion's error result.
    :binary
  end

  defp validation_type!({:struct, fields}), do: {:struct, validation_fields!(fields)}

  defp validation_type!({kind, %Field{} = item}) when kind in [:list, :large_list, :map],
    do: {kind, validation_field!(item)}

  defp validation_type!({:dictionary, key, value}) do
    Schema.normalize_type!({:dictionary, key, :binary})
    Schema.normalize_type!(validation_type!(value))
    :binary
  end

  defp validation_type!(type), do: type
end
