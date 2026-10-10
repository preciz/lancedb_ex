defmodule LanceDB.StorageOptions do
  @moduledoc false
  def normalize!(values) when is_map(values) and not is_struct(values) do
    pairs =
      Enum.map(values, fn {key, value} ->
        key = LanceDB.Options.column_name!(key)
        value = to_string(value)

        if !String.valid?(value),
          do: raise(ArgumentError, "storage option values must be UTF-8 strings")

        {key, value}
      end)

    normalized = Map.new(pairs)

    if map_size(normalized) != map_size(values),
      do: raise(ArgumentError, "storage option keys must be unique")

    normalized
  end

  def normalize!(_), do: raise(ArgumentError, "storage_options must be a map")
end
