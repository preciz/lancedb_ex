defmodule LanceDB.WriteOptions do
  @moduledoc false
  @spec validate_sizes!(keyword(), [atom()]) :: :ok
  def validate_sizes!(opts, keys) do
    for key <- keys, Keyword.has_key?(opts, key) do
      value = opts[key]

      if !(is_integer(value) and value > 0 and value <= 4_294_967_295),
        do: raise(ArgumentError, "#{key} must be a positive uint32 integer")
    end

    :ok
  end
end
