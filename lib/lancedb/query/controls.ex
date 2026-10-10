defmodule LanceDB.Query.Controls do
  @moduledoc false
  alias LanceDB.{Options, Projection, SearchControls}
  @common [:columns, :select, :timeout_ms, :with_row_id]
  @read [:filter, :limit, :offset, :order_by, :fast_search] ++ @common
  @vector [
            :distance,
            :vector_column,
            :nprobes,
            :ef,
            :approx_mode,
            :refine_factor,
            :bypass_vector_index,
            :prefilter,
            :distance_range,
            :minimum_nprobes,
            :maximum_nprobes
          ] ++ @read
  @text [:text_column, :wand_factor]
  @hybrid [:rrf_k, :normalize, :reranker] ++ @text
  @max 9_223_372_036_854_775_807

  def normalize!(opts) do
    opts = Keyword.validate!(opts, Enum.uniq(@vector ++ @hybrid))
    Map.new(opts, fn {key, value} -> {key, value!(key, value)} end)
  end

  def validate!(source, controls) do
    kind = if source == :scan, do: :scan, else: elem(source, 0)

    Keyword.validate!(Map.to_list(controls), allowed(kind))
    limit = limit!(kind, controls)

    opts = Map.to_list(controls)
    SearchControls.read!(opts, limit || 0)
    Projection.options!(opts)

    validate_kind!(source, controls, limit)
    :ok
  end

  defp allowed(:scan), do: @read
  defp allowed(:take), do: @common
  defp allowed(:text), do: @read ++ @text
  defp allowed(:hybrid), do: @vector ++ @hybrid
  defp allowed(_), do: @vector

  def default_limit(:scan), do: 100
  def default_limit(_), do: 10

  defp limit!(kind, controls) do
    limit = Map.get(controls, :limit, default_limit(kind))

    if kind not in [:scan, :take] and is_nil(limit),
      do: raise(ArgumentError, "limit must be a positive integer")

    limit
  end

  defp validate_kind!(:scan, _, _), do: :ok
  defp validate_kind!({:text, _}, _, _), do: :ok
  defp validate_kind!({:take, _, _}, _, _), do: :ok

  defp validate_kind!({:hybrid, _, _} = source, controls, limit) do
    validate_vector!(source, controls, limit)

    if Map.get(controls, :reranker, :rrf) != :rrf and Map.has_key?(controls, :rrf_k),
      do: raise(ArgumentError, "rrf_k only applies to the RRF reranker")
  end

  defp validate_kind!(source, controls, limit), do: validate_vector!(source, controls, limit)

  defp validate_vector!(source, controls, limit) do
    SearchControls.vector!(Map.to_list(controls))
    factor = Map.get(controls, :refine_factor, 1)

    if limit + Map.get(controls, :offset, 0) > div(@max, factor),
      do: raise(ArgumentError, "(limit plus offset) times refine_factor is too large")

    kind = elem(source, 0)
    distance = Map.get(controls, :distance, if(kind == :multivector, do: :cosine, else: :l2))

    if kind == :multivector and distance != :cosine,
      do: raise(ArgumentError, "multivector search requires cosine distance")

    validate_representation!(source, distance)
  end

  defp validate_representation!(source, :hamming) do
    vectors =
      if elem(source, 0) in [:batch, :multivector], do: elem(source, 1), else: [elem(source, 1)]

    if !Enum.all?(vectors, &binary_vector?/1),
      do: raise(ArgumentError, "binary query vectors require integers from 0 through 255")
  end

  defp validate_representation!(_, _), do: :ok
  defp binary_vector?(vector), do: Enum.all?(vector, &(is_integer(&1) and &1 >= 0 and &1 <= 255))

  defp value!(:filter, value), do: Options.filter!(value)

  defp value!(key, nil)
       when key in [
              :columns,
              :select,
              :vector_column,
              :text_column,
              :limit,
              :timeout_ms,
              :wand_factor,
              :maximum_nprobes,
              :distance_range,
              :approx_mode
            ],
       do: nil

  defp value!(key, value) when key in [:vector_column, :text_column],
    do: Options.column_name!(value)

  defp value!(:columns, value), do: Options.column_names!(value)

  defp value!(:select, value),
    do: Projection.options!(select: value).select |> Enum.map(&List.to_tuple/1)

  defp value!(:order_by, values) do
    if !Keyword.keyword?(values), do: raise(ArgumentError, "order_by must be a keyword list")

    Enum.map(values, fn {direction, column} ->
      if direction not in [
           :asc,
           :desc,
           :asc_nulls_first,
           :asc_nulls_last,
           :desc_nulls_first,
           :desc_nulls_last
         ],
         do: raise(ArgumentError, "invalid order_by direction")

      {direction, Options.column_name!(column)}
    end)
  end

  defp value!(key, value)
       when key in [:with_row_id, :fast_search, :prefilter, :bypass_vector_index] do
    if not is_boolean(value), do: raise(ArgumentError, "#{key} must be a boolean")
    value
  end

  defp value!(key, value)
       when key in [
              :limit,
              :offset,
              :timeout_ms,
              :nprobes,
              :ef,
              :refine_factor,
              :minimum_nprobes,
              :maximum_nprobes
            ] do
    max = if key in [:limit, :offset], do: @max, else: 4_294_967_295
    min = if key == :offset, do: 0, else: 1

    if !(is_integer(value) and value >= min and value <= max),
      do: raise(ArgumentError, "#{key} must be an integer between #{min} and #{max}")

    value
  end

  defp value!(:distance, value) do
    case Enum.find([:l2, :cosine, :dot, :hamming], &(value in [&1, to_string(&1)])) do
      nil -> raise ArgumentError, "distance must be :l2, :cosine, :dot, or :hamming"
      distance -> distance
    end
  end

  defp value!(:distance_range, value),
    do: SearchControls.vector!(distance_range: value).distance_range |> List.to_tuple()

  defp value!(:approx_mode, value), do: SearchControls.vector!(approx_mode: value).approx_mode
  defp value!(:reranker, value), do: LanceDB.Reranker.validate!(value)
  defp value!(:normalize, value) when value in [:score, :rank], do: value
  defp value!(:normalize, _), do: raise(ArgumentError, "normalize must be :score or :rank")

  defp value!(key, value) when key in [:rrf_k, :wand_factor] do
    min = if key == :rrf_k, do: 1.1754943508222875e-38, else: 1

    if !(is_number(value) and value >= min and value <= 3.402_823_466_385_288_6e38),
      do: raise(ArgumentError, "#{key} must be a float32-compatible number of at least #{min}")

    value / 1
  end
end
