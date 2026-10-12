defmodule LanceDB.Records do
  @moduledoc false

  @spec prepare(term(), :infer | :typed, boolean()) :: LanceDB.result([map()])
  def prepare(data, mode, allow_empty \\ false) do
    with :ok <- validate(data, mode, allow_empty), do: {:ok, json_rows(data, mode)}
  rescue
    e -> {:error, %LanceDB.Error{kind: :invalid_data, message: Exception.message(e)}}
  else
    {:error, reason} ->
      {:error, LanceDB.Error.normalize(%{kind: :invalid_data, message: reason}, nil)}

    result ->
      result
  end

  defp json_rows(data, :infer), do: data

  defp json_rows(data, :typed),
    do: Enum.map(data, &Map.new(&1, fn {key, value} -> {key, json_value!(value)} end))

  @spec validate(term(), :infer | :typed, boolean()) :: :ok | {:error, String.t()}
  def validate(records, :infer, _allow_empty), do: validate(records)
  def validate([], :typed, true), do: :ok

  def validate([_ | _] = records, :typed, _) do
    with {:ok, [first | _] = rows} <- normalize(records),
         true <- map_size(first) > 0,
         true <- Enum.all?(rows, &typed_row?(first, &1)) do
      :ok
    else
      _ -> {:error, "Records must have identical fields and supported values"}
    end
  end

  def validate(_, :typed, _), do: {:error, "Records must be a non-empty list of maps"}

  # Prevent Arrow inference from silently dropping fields or replacing mistyped
  # scalar values with null.
  @spec validate(term()) :: :ok | {:error, String.t()}
  def validate([first | _] = records) when is_map(first) and not is_struct(first) do
    with {:ok, normalized} <- normalize(records),
         [sample | _] = normalized,
         true <- map_size(sample) > 0,
         true <- Enum.all?(sample, fn {_, value} -> supported?(value) end),
         true <- Enum.all?(normalized, &compatible?(sample, &1)) do
      :ok
    else
      _ ->
        {:error,
         "Records must have identical fields and consistent scalar or vector types; the first row cannot contain null"}
    end
  end

  def validate(_), do: {:error, "Records must be a non-empty list of maps"}

  @spec json_value!(term()) :: term()
  def json_value!(%LanceDB.Blob.Reference{uri: uri}),
    do: json_value!(%{data: nil, uri: LanceDB.Options.string!(uri, "blob URI")})

  def json_value!(%Time{calendar: Calendar.ISO} = value), do: Time.to_iso8601(value)

  def json_value!(%LanceDB.JSON{value: value}),
    do: %{"__lancedb_json__" => LanceDB.JSON.encode!(value)}

  def json_value!({key, value}), do: [json_value!(key), json_value!(value)]

  def json_value!(%Date{calendar: Calendar.ISO} = value), do: Date.to_iso8601(value)

  def json_value!(value) when is_binary(value),
    do: %{"__lancedb_binary__" => Base.encode16(value, case: :lower)}

  def json_value!(%DateTime{calendar: Calendar.ISO} = value), do: DateTime.to_iso8601(value)

  def json_value!(%NaiveDateTime{calendar: Calendar.ISO} = value),
    do: NaiveDateTime.to_iso8601(value)

  def json_value!(values) when is_list(values), do: Enum.map(values, &json_value!/1)

  def json_value!(value) when is_map(value) and not is_struct(value) do
    case normalize_row(value) do
      {:ok, normalized} -> Map.new(normalized, fn {key, value} -> {key, json_value!(value)} end)
      :error -> raise ArgumentError, "record keys must be unique strings or atoms"
    end
  end

  def json_value!(value), do: value

  defp normalize(records) do
    Enum.reduce_while(records, {:ok, []}, fn row, {:ok, rows} ->
      case normalize_row(row) do
        {:ok, normalized} -> {:cont, {:ok, [normalized | rows]}}
        :error -> {:halt, :error}
      end
    end)
    |> case do
      {:ok, rows} -> {:ok, Enum.reverse(rows)}
      error -> error
    end
  end

  defp typed_row?(first, row) do
    Map.keys(row) == Map.keys(first) and
      Enum.all?(row, fn {_, value} -> typed_value?(value) end)
  end

  defp typed_value?(%LanceDB.Blob.Reference{uri: uri}),
    do: is_binary(uri) and String.valid?(uri) and String.trim(uri) != ""

  defp typed_value?(%Time{calendar: Calendar.ISO}), do: true
  defp typed_value?(%LanceDB.JSON{}), do: true
  defp typed_value?({key, value}), do: typed_value?(key) and typed_value?(value)

  defp typed_value?(nil), do: true
  defp typed_value?(%Date{calendar: Calendar.ISO}), do: true

  defp typed_value?(value) when is_integer(value),
    do: value >= -9_223_372_036_854_775_808 and value <= 18_446_744_073_709_551_615

  defp typed_value?(%DateTime{calendar: Calendar.ISO}), do: true
  defp typed_value?(%NaiveDateTime{calendar: Calendar.ISO}), do: true
  defp typed_value?(values) when is_list(values), do: Enum.all?(values, &typed_value?/1)

  defp typed_value?(value) when is_map(value) and not is_struct(value) do
    case normalize_row(value) do
      {:ok, normalized} -> Enum.all?(normalized, fn {_, value} -> typed_value?(value) end)
      :error -> false
    end
  end

  defp typed_value?(value), do: supported?(value)

  defp normalize_row(row) when is_map(row) and not is_struct(row) do
    if Enum.all?(Map.keys(row), &(is_atom(&1) or is_binary(&1))) do
      normalized = Map.new(row, fn {key, value} -> {to_string(key), value} end)
      if map_size(normalized) == map_size(row), do: {:ok, normalized}, else: :error
    else
      :error
    end
  end

  defp normalize_row(_), do: :error

  defp supported?(value) when is_integer(value),
    do: value >= -9_223_372_036_854_775_808 and value <= 9_223_372_036_854_775_807

  defp supported?(value) when is_binary(value) or is_float(value) or is_boolean(value), do: true

  defp supported?([_ | _] = vector),
    do: Enum.all?(vector, &(is_number(&1) and abs(&1) <= 3.402_823_466_385_288_6e38))

  defp supported?(_), do: false

  defp compatible?(sample, row) do
    Map.keys(sample) == Map.keys(row) and
      Enum.all?(sample, fn {key, value} -> same_type?(value, row[key]) end)
  end

  defp same_type?(sample, nil), do: not is_list(sample)

  defp same_type?(sample, value) when is_list(sample),
    do: is_list(value) and length(sample) == length(value) and supported?(value)

  defp same_type?(sample, value) when is_integer(sample),
    do: is_integer(value) and supported?(value)

  defp same_type?(sample, value) when is_float(sample), do: is_number(value)
  defp same_type?(sample, value) when is_binary(sample), do: is_binary(value)
  defp same_type?(sample, value) when is_boolean(sample), do: is_boolean(value)
end
