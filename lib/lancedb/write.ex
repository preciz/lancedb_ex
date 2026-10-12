defmodule LanceDB.Write do
  @moduledoc false

  import LanceDB.Options, only: [filter!: 1, column_name!: 1, column_names!: 1]
  alias LanceDB.{Backend, Ingestion, Records}

  @spec delete(LanceDB.Table.t(), String.t() | LanceDB.Expr.t(), keyword()) ::
          LanceDB.Native.result(LanceDB.Native.counts_output())
  def delete(table, predicate, opts) do
    Keyword.validate!(opts, [])
    Backend.delete_rows_result(table, filter!(predicate))
  end

  @spec upsert_options!(keyword()) :: keyword()
  def upsert_options!(opts) do
    Keyword.validate!(opts, [:on, :timeout_ms, :use_index]) ++
      [when_matched_update_all: true, when_not_matched_insert_all: true]
  end

  defp merge_options!(opts) do
    opts =
      Keyword.validate!(opts, [
        :on,
        :when_matched_update_all,
        :when_not_matched_insert_all,
        :when_not_matched_by_source_delete,
        :timeout_ms,
        :use_index
      ])

    keys = opts |> Keyword.get(:on, :id) |> List.wrap() |> column_names!()
    {update, update_condition} = merge_action!(Keyword.get(opts, :when_matched_update_all, false))

    {delete, delete_condition} =
      merge_action!(Keyword.get(opts, :when_not_matched_by_source_delete, false))

    insert = Keyword.get(opts, :when_not_matched_insert_all, false)
    use_index = Keyword.get(opts, :use_index, true)

    if !(is_boolean(insert) and is_boolean(use_index)),
      do: raise(ArgumentError, "insert and use_index options must be booleans")

    if !(update or insert or delete), do: raise(ArgumentError, "enable at least one merge action")

    timeout = merge_timeout!(opts)

    options =
      %{
        update: update,
        update_condition: update_condition,
        insert: insert,
        delete: delete,
        delete_condition: delete_condition,
        timeout_ms: timeout,
        use_index: use_index
      }

    {keys, options}
  end

  defp merge_timeout!(opts) do
    timeout = Keyword.get(opts, :timeout_ms)

    if Keyword.has_key?(opts, :timeout_ms) and
         not (is_integer(timeout) and timeout > 0 and timeout <= 4_294_967_295),
       do: raise(ArgumentError, "timeout_ms must be a positive uint32 integer")

    timeout
  end

  defp merge_action!(true), do: {true, nil}
  defp merge_action!(false), do: {false, nil}
  defp merge_action!(%LanceDB.Expr{} = condition), do: {true, filter!(condition)}
  defp merge_action!(condition) when is_binary(condition), do: {true, filter!(condition)}

  defp merge_action!(_),
    do: raise(ArgumentError, "merge actions must be booleans or non-empty SQL conditions")

  @spec update(LanceDB.Table.t(), map(), keyword()) ::
          LanceDB.Native.result(LanceDB.Native.counts_output())
  def update(table, changes, opts) do
    opts = Keyword.validate!(opts, [:filter])
    filter = filter!(Keyword.get(opts, :filter))

    if !(is_map(changes) and not is_struct(changes) and map_size(changes) > 0),
      do: raise(ArgumentError, "changes must be a non-empty map of columns to values")

    changes =
      Enum.map(changes, fn {column, value} ->
        {column_name!(column), update_value!(value)}
      end)

    column_names!(Enum.map(changes, &elem(&1, 0)))
    Backend.update_rows(table, changes, filter)
  end

  defp update_value!(%LanceDB.Expr{} = expression), do: {:sql, filter!(expression)}

  defp update_value!({:sql, expression}) do
    {:sql, filter!(expression) || raise(ArgumentError, "SQL update expressions cannot be nil")}
  end

  defp update_value!(value) do
    case Records.validate([%{value: value}], :typed, false) do
      :ok -> {:literal, value |> Records.json_value!() |> JSON.encode!()}
      {:error, reason} -> raise ArgumentError, reason
    end
  end

  @spec merge(LanceDB.Table.t(), [map()] | LanceDB.Source.t(), keyword()) ::
          LanceDB.Backend.result(LanceDB.Backend.write_output())
          | {:error, map() | LanceDB.Error.t() | String.t()}
  def merge(table, source, opts) do
    {keys, wire} = merge_options!(opts)
    source = LanceDB.Source.coerce(source)

    with :ok <- LanceDB.Source.check_schema(table, source) do
      Ingestion.run_operation(
        fn -> Backend.merge_stream_open(table, keys, wire) end,
        LanceDB.Source.chunks(source)
      )
    end
  end
end
