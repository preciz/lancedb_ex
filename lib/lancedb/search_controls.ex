defmodule LanceDB.SearchControls do
  @moduledoc false

  @max_count 9_223_372_036_854_775_807
  @max_float 3.402_823_466_385_288_6e38

  def read!(opts, limit) do
    offset = Keyword.get(opts, :offset, 0)

    if !(is_integer(offset) and offset >= 0 and offset <= @max_count),
      do: raise(ArgumentError, "offset must be a non-negative 64-bit integer")

    if limit > @max_count - offset,
      do: raise(ArgumentError, "limit plus offset is too large")

    %{
      offset: offset,
      prefilter: boolean!(opts, :prefilter, true),
      fast_search: boolean!(opts, :fast_search, false),
      with_row_id: boolean!(opts, :with_row_id, false)
    }
  end

  def vector!(opts) do
    if Keyword.get(opts, :fast_search, false) and Keyword.get(opts, :bypass_vector_index, false),
      do: raise(ArgumentError, "fast_search and bypass_vector_index cannot both be true")

    %{
      probes: probes!(opts),
      distance_range: distance_range!(Keyword.get(opts, :distance_range)),
      approx_mode: approx_mode!(Keyword.get(opts, :approx_mode))
    }
  end

  defp approx_mode!(mode) when mode in [nil, :fast, :normal, :accurate], do: mode

  defp approx_mode!(_),
    do: raise(ArgumentError, "approx_mode must be :fast, :normal, :accurate, or nil")

  defp probes!(opts) do
    if Keyword.has_key?(opts, :minimum_nprobes) or Keyword.has_key?(opts, :maximum_nprobes) do
      if Keyword.has_key?(opts, :nprobes),
        do:
          raise(
            ArgumentError,
            "nprobes cannot be combined with minimum_nprobes or maximum_nprobes"
          )

      minimum = positive_count!(Keyword.get(opts, :minimum_nprobes, 20), :minimum_nprobes)
      maximum = Keyword.get(opts, :maximum_nprobes, 20)
      if maximum != nil, do: positive_count!(maximum, :maximum_nprobes)

      if maximum != nil and maximum < minimum,
        do: raise(ArgumentError, "maximum_nprobes must be at least minimum_nprobes")

      %{minimum: minimum, maximum: maximum}
    end
  end

  defp positive_count!(value, name) do
    if !(is_integer(value) and value > 0 and value <= 4_294_967_295),
      do: raise(ArgumentError, "#{name} must be a positive uint32 integer")

    value
  end

  defp distance_range!(nil), do: nil

  defp distance_range!({lower, upper}) do
    lower = bound!(lower)
    upper = bound!(upper)

    if lower != nil and upper != nil and lower >= upper,
      do:
        raise(
          ArgumentError,
          "distance_range lower bound must be less than upper bound in float32"
        )

    [lower, upper]
  end

  defp distance_range!(_),
    do: raise(ArgumentError, "distance_range must be {lower, upper}, with optional nil bounds")

  defp bound!(nil), do: nil

  defp bound!(value) do
    if !(is_number(value) and abs(value) <= @max_float),
      do: raise(ArgumentError, "distance bounds must be float32-compatible numbers or nil")

    <<rounded::float-32>> = <<value / 1::float-32>>
    rounded
  end

  defp boolean!(opts, name, default) do
    value = Keyword.get(opts, name, default)
    if not is_boolean(value), do: raise(ArgumentError, "#{name} must be a boolean")
    value
  end
end
