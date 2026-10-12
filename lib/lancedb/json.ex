defmodule LanceDB.JSON do
  @moduledoc """
  Explicit JSON values for `:json` schema fields, stored using Lance's native JSONB.

  Maps, lists, strings, numbers and booleans may be written directly to JSON fields.
  `nil` means SQL NULL; `LanceDB.JSON.null/0` means the JSON value `null`. Reads return
  ordinary Elixir values, with `%LanceDB.JSON{value: nil}` preserving JSON null at
  the column level. Nulls inside JSON objects and arrays remain ordinary nil.
  `new/1` is also useful to mark an entire JSON value explicitly.

  Object keys must be unique strings or atoms; strings must be valid UTF-8.
  Arbitrary structs and binaries containing invalid UTF-8 are not JSON values.
  """
  @enforce_keys [:value]
  defstruct [:value]
  @type t :: %__MODULE__{value: term()}

  @doc "Builds a validated JSON value. Invalid values raise ArgumentError."
  @spec new(term()) :: t()
  def new(value), do: %__MODULE__{value: normalize!(value)}

  @doc "Returns a JSON null, distinct from a nullable field's SQL NULL."
  @spec null() :: t()
  def null, do: new(nil)

  @doc false
  @spec encode!(term()) :: String.t()
  def encode!(value), do: value |> normalize!() |> JSON.encode!()

  defp normalize!(value)
       when is_integer(value) and
              (value < -9_223_372_036_854_775_808 or value > 18_446_744_073_709_551_615),
       do: raise(ArgumentError, "JSON integers must fit int64 or uint64")

  defp normalize!(value) when is_nil(value) or is_boolean(value) or is_number(value), do: value

  defp normalize!(value) when is_binary(value) do
    if !String.valid?(value), do: raise(ArgumentError, "JSON strings must be UTF-8")
    value
  end

  defp normalize!(values) when is_list(values), do: Enum.map(values, &normalize!/1)

  defp normalize!(value) when is_map(value) and not is_struct(value) do
    normalized = Map.new(value, fn {key, value} -> {key!(key), normalize!(value)} end)

    if map_size(normalized) != map_size(value),
      do: raise(ArgumentError, "JSON object keys must be unique")

    normalized
  end

  defp normalize!(_), do: raise(ArgumentError, "unsupported JSON value")
  defp key!(key) when is_atom(key), do: Atom.to_string(key)
  defp key!(key) when is_binary(key), do: normalize!(key)
  defp key!(_), do: raise(ArgumentError, "JSON object keys must be strings or atoms")
end
