defmodule LanceDB.Options do
  @moduledoc false

  def batch_size!(opts) do
    {size, opts} = Keyword.pop(opts, :batch_size, 1024)

    if !(is_integer(size) and size > 0 and size <= 4_294_967_295),
      do: raise(ArgumentError, "batch_size must be a positive uint32 integer")

    {size, opts}
  end

  def filter!(nil), do: nil
  def filter!(%LanceDB.Expr{} = expression), do: LanceDB.Expr.to_sql(expression)

  def filter!(filter) when is_binary(filter) do
    if String.valid?(filter) and String.trim(filter) != "",
      do: filter,
      else: raise(ArgumentError, "filter must be an Expr, non-empty SQL string or nil")
  end

  def filter!(_), do: raise(ArgumentError, "filter must be an Expr, non-empty SQL string or nil")

  def column_name!(name) when is_atom(name) and not is_nil(name),
    do: column_name!(Atom.to_string(name))

  def column_name!(name) when is_binary(name) do
    if String.valid?(name) and String.trim(name) != "",
      do: name,
      else: raise(ArgumentError, "column names must be non-empty strings or atoms")
  end

  def column_name!(_),
    do: raise(ArgumentError, "column names must be non-empty strings or atoms")

  def column_names!([_ | _] = names) do
    names = Enum.map(names, &column_name!/1)

    if length(Enum.uniq(names)) != length(names),
      do: raise(ArgumentError, "column names must be unique")

    names
  end

  def column_names!(_), do: raise(ArgumentError, "columns must be a non-empty list")

  def string!(value, label) do
    if !(is_binary(value) and String.valid?(value) and String.trim(value) != ""),
      do: raise(ArgumentError, "#{label} must be a non-empty UTF-8 string")

    value
  end

  def version!(version) do
    if !(is_integer(version) and version > 0 and version <= 18_446_744_073_709_551_615),
      do: raise(ArgumentError, "version must be a positive uint64 integer")

    version
  end

  def timeout_ms!(nil), do: nil

  def timeout_ms!(value) do
    if !(is_integer(value) and value > 0 and value <= 4_294_967_295),
      do: raise(ArgumentError, "timeout_ms must be nil or a positive uint32 integer")

    value
  end

  def metadata!(values, allow_delete \\ false)

  def metadata!(values, allow_delete) when is_map(values) and not is_struct(values) do
    pairs =
      Enum.map(values, fn {key, value} ->
        key = column_name!(key)

        if !((allow_delete and is_nil(value)) or (is_binary(value) and String.valid?(value))),
          do:
            raise(ArgumentError, "metadata values must be UTF-8 strings, or nil to delete a key")

        {key, value}
      end)

    normalized = Map.new(pairs)

    if map_size(normalized) != map_size(values),
      do: raise(ArgumentError, "metadata keys must be unique")

    normalized
  end

  def metadata!(_, _), do: raise(ArgumentError, "metadata must be a map")
end
