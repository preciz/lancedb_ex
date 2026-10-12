defmodule LanceDB.Result do
  @moduledoc false

  @spec decode(LanceDB.Native.result(String.t())) ::
          {:ok, term()} | {:error, LanceDB.Native.error() | JSON.decode_error_reason()}
  def decode({:ok, json}), do: JSON.decode(json)
  def decode({:error, reason}), do: {:error, reason}

  @spec rows!(String.t()) :: [map()]
  def rows!(json) do
    case JSON.decode!(json) do
      %{"rows" => rows, "binary_fields" => fields} -> Enum.map(rows, &decode_fields(&1, fields))
      rows when is_list(rows) -> rows
    end
  end

  defp decode_fields(row, fields) do
    Enum.reduce(fields, row, fn {name, type}, row ->
      Map.update!(row, name, &decode_value(&1, type))
    end)
  end

  defp decode_value(nil, _), do: nil
  defp decode_value(value, nil), do: value

  defp decode_value(value, "json") do
    case JSON.decode!(value) do
      nil -> LanceDB.JSON.null()
      value -> value
    end
  end

  defp decode_value(values, %{"map" => info}) do
    Enum.map(values, fn row ->
      {decode_value(Map.fetch!(row, info["key_name"]), info["key"]),
       decode_value(Map.fetch!(row, info["value_name"]), info["value"])}
    end)
  end

  defp decode_value(value, "binary"), do: Base.decode16!(value, case: :mixed)
  defp decode_value(values, %{"list" => type}), do: Enum.map(values, &decode_value(&1, type))
  defp decode_value(value, %{"struct" => fields}), do: decode_fields(value, fields)
end
