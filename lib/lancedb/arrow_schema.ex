defmodule LanceDB.ArrowSchema do
  @moduledoc """
  A physical Arrow output description. Fields share the Schema/Field vocabulary,
  but may contain blob descriptors, unsupported external types, or field properties
  that the storage engine cannot persist. Inspection never silently drops them.

  Use `to_schema/1` to validate and explicitly convert a description into a table
  definition. Blob descriptors require fetching payloads and declaring `:blob`;
  they cannot be converted into writable payload fields automatically.
  """
  alias LanceDB.{Backend, Error, Schema}
  alias LanceDB.Backend.SchemaCodec
  defstruct fields: [], metadata: %{}
  @type t :: %__MODULE__{fields: [LanceDB.Field.t()], metadata: map()}

  @doc "Validates storage compatibility, returning a logical Schema or an Error."
  @spec to_schema(t()) :: LanceDB.result(Schema.t())
  def to_schema(%__MODULE__{} = arrow) do
    LanceDB.Execution.run(:schema_conversion, fn ->
      with {:ok, _} <- Backend.schema_validate(SchemaCodec.encode_description(arrow.fields)) do
        {:ok, Schema.new(SchemaCodec.logical_fields(arrow.fields), arrow.metadata)}
      end
    end)
  rescue
    error in ArgumentError ->
      {:error,
       %Error{
         kind: :unsupported,
         operation: :schema_conversion,
         message: Exception.message(error)
       }}
  end
end
