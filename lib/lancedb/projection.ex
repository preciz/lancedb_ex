defmodule LanceDB.Projection do
  @moduledoc false
  import LanceDB.Options, only: [column_names!: 1, column_name!: 1]

  @reserved ~w(_rowid _rowoffset _rowaddr _distance _score _relevance_score query_index)

  def options!(opts) do
    columns = Keyword.get(opts, :columns)
    select = Keyword.get(opts, :select)

    if columns != nil and select != nil,
      do: raise(ArgumentError, "select and columns cannot be combined")

    %{
      columns: if(columns != nil, do: column_names!(columns)),
      select: if(select != nil, do: expressions!(select))
    }
  end

  defp expressions!(pairs) when is_map(pairs) and not is_struct(pairs),
    do: expressions!(Map.to_list(pairs))

  defp expressions!([_ | _] = pairs) do
    expressions = Enum.map(pairs, &expression!/1)
    column_names!(Enum.map(expressions, &hd/1))
    expressions
  end

  defp expressions!(_),
    do: raise(ArgumentError, "select must be a non-empty map or list of name/SQL pairs")

  defp expression!({name, %LanceDB.Expr{} = expression}),
    do: expression!({name, LanceDB.Expr.to_sql(expression)})

  defp expression!({name, sql}) when is_binary(sql) do
    name = column_name!(name)

    if name in @reserved,
      do: raise(ArgumentError, "select aliases cannot use reserved metadata names")

    if !(String.valid?(sql) and String.trim(sql) != ""),
      do: raise(ArgumentError, "select expressions must be non-empty SQL strings")

    [name, sql]
  end

  defp expression!(_), do: raise(ArgumentError, "select requires name/SQL string pairs")
end
