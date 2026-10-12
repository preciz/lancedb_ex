defmodule LanceDB.Index.FTSOptions do
  @moduledoc false
  @keys [:block_size, :memory_limit_mb, :num_workers, :format_version]
  @spec keys() :: [atom()]
  def keys, do: @keys

  @spec parse!(keyword(), atom()) :: map()
  def parse!(opts, type) do
    values = Keyword.take(opts, @keys)

    if values != [] and type != :fts,
      do: raise(ArgumentError, "FTS build options require type: :fts")

    validate_sizes!(values)
    validate_format!(values, opts)
    Map.new(values)
  end

  defp validate_sizes!(values) do
    if Keyword.has_key?(values, :block_size) and values[:block_size] not in [128, 256],
      do: raise(ArgumentError, "block_size must be 128 or 256")

    for key <- [:memory_limit_mb, :num_workers], Keyword.has_key?(values, key) do
      value = values[key]

      if !(is_integer(value) and value > 0 and value <= 4_294_967_295),
        do: raise(ArgumentError, "#{key} must be a positive uint32 integer")
    end
  end

  defp validate_format!(values, opts) do
    if Keyword.has_key?(values, :format_version) and
         values[:format_version] not in [:v1, :v2, :v3],
       do: raise(ArgumentError, "format_version must be :v1, :v2 or :v3")

    if values[:format_version] in [:v1, :v2] and
         (values[:block_size] == 256 or get_in(opts, [:analyzer, :tokenizer]) == :code),
       do:
         raise(ArgumentError, "256-document blocks and code analysis require format_version: :v3")
  end
end
