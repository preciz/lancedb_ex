defmodule LanceDB.FTS do
  @moduledoc """
  Structured full-text queries for `LanceDB.Query.full_text_search/3`,
  `LanceDB.Query.hybrid_search/4`, and reusable queries built with `LanceDB.Query.text/2`.

  Builders return query values directly; invalid arguments raise `ArgumentError`.
  A leaf's `:column` takes precedence over the search's `:text_column` default.
  Without either, structured queries require exactly one indexed text column.
  `prepare/2` and `prepare/3` perform optional analyzer work and return reusable
  `LanceDB.FTS.Prepared` values for exact and fuzzy term composition.
  """

  import LanceDB.Options, only: [column_name!: 1, column_names!: 1]
  alias LanceDB.FTS.{Preparation, Prepared}

  @enforce_keys [:query]
  defstruct [:query]
  @opaque t :: %__MODULE__{query: map()}
  @type input :: String.t() | t()

  @doc """
  Prepares plain text using an explicit `:analyzer` keyword list (SDK defaults
  when omitted). Only `tokenizer: :simple` and plain-text documents are supported.

  `:max_bytes` defaults to 2048 input bytes; `:max_terms` defaults to 32 distinct
  analyzed terms. Set either limit to nil to disable it. Invalid arguments and
  exceeded limits raise `ArgumentError`; native failures return `LanceDB.Error`.
  Terms removed by stop-word or length filters are omitted. Blank or completely
  filtered input returns an empty preparation; inspect `term_count/1` before
  constructing a match. Existing string-based match calls are unchanged.
  """
  @spec prepare(String.t(), keyword()) :: LanceDB.result(Prepared.t())
  def prepare(text, opts \\ []), do: Preparation.prepare(text, opts)

  @doc """
  Prepares text using a saved index's analyzer. Supply exactly one selector,
  `:index` or `:column`, as a string or atom. Accepts the same limits as `prepare/2`.

  Missing, ambiguous, and non-FTS indexes return errors. Saved analyzers other
  than plain text with the simple tokenizer return an `:unsupported` error.
  Preparation respects snapshots. Prepare again after rebuilding an index with
  different analyzer settings; query construction does not check for changes.
  """
  @spec prepare(LanceDB.Table.t(), String.t(), keyword()) ::
          LanceDB.result(Prepared.t())
  def prepare(table, text, opts), do: Preparation.prepare(table, text, opts)

  @doc "Counts distinct analyzed terms in a preparation."
  @spec term_count(Prepared.t()) :: non_neg_integer()
  def term_count(prepared), do: Prepared.term_count(prepared)

  @doc """
  Matches terms. Options: `:column`, `:operator` (`:or` default, or `:and`),
  `:boost` (positive float32 number, default 1), `:fuzziness` (0 default, 1, or 2),
  `:max_expansions` (positive uint32, default 50), and
  `:prefix_length` (non-negative uint32, default 0).
  Prefix lengths count Unicode code points. Automatic fuzziness is
  not exposed because this SDK treats it as an exact query.

  A `LanceDB.FTS.Prepared` value adds `:exact_numbers` (true by default),
  `:exact_boost` (positive multiplier, default 1), and
  `fuzziness: {:length, one, two}`. Length is measured in Unicode graphemes
  of the analyzed term: below `one` is exact-only, from `one` to below `two`
  allows one edit, and from `two` allows two edits. Fixed distances still work.
  Fuzziness defaults to 0 for both forms.

  Each distinct term gets an exact clause using its first original spelling
  and, when its distance is positive, a fuzzy alternative using analyzed text.
  `:operator` combines those term groups. Decimal-digit terms stay exact when
  `:exact_numbers` is true. `:exact_boost` multiplies `:boost` on exact clauses;
  scoring preferences do not guarantee a global exact-first order.
  `:max_expansions` applies separately to each fuzzy term. Prefix lengths apply
  to analyzed text. An empty preparation raises `ArgumentError`.
  """
  @spec match(String.t() | Prepared.t(), keyword()) :: t()
  def match(text, opts \\ [])

  def match(%Prepared{} = prepared, opts), do: prepared_match(prepared, opts)

  def match(text, opts) do
    wrap(Map.put(match_options!(opts), :text, text!(text)))
  end

  defp match_options!(opts) do
    opts =
      Keyword.validate!(opts, [
        :column,
        :operator,
        :boost,
        :fuzziness,
        :max_expansions,
        :prefix_length
      ])

    %{
      type: :match,
      column: column!(opts),
      operator: operator!(opts),
      boost: positive_float!(Keyword.get(opts, :boost, 1)),
      fuzziness: fuzziness!(Keyword.get(opts, :fuzziness, 0)),
      max_expansions: count!(opts, :max_expansions, 50, 1),
      prefix_length: count!(opts, :prefix_length, 0, 0)
    }
  end

  @doc """
  Matches a phrase. Options: `:column` and non-negative `:slop` (default 0).
  Existing FTS indexes must store positions. An explicit column permits scans
  without an index; unindexed current rows are scanned unless fast search is used.
  """
  @spec phrase(String.t(), keyword()) :: t()
  def phrase(text, opts \\ []) do
    opts = Keyword.validate!(opts, [:column, :slop])

    wrap(%{
      type: :phrase,
      text: text!(text),
      column: column!(opts),
      slop: count!(opts, :slop, 0, 0)
    })
  end

  @doc """
  Matches terms across named columns. Options: `:operator` (`:or` or `:and`)
  and `:boosts` (one positive float32 number per column, default all 1).
  AND applies to terms within each column, not across columns.
  """
  @spec multi_match(String.t(), [atom() | String.t()], keyword()) :: t()
  def multi_match(text, columns, opts \\ []) do
    opts = Keyword.validate!(opts, [:operator, :boosts])
    columns = column_names!(columns)
    boosts = Keyword.get(opts, :boosts, List.duplicate(1, length(columns)))

    if !(is_list(boosts) and length(boosts) == length(columns)),
      do: raise(ArgumentError, "boosts must contain one number per column")

    wrap(%{
      type: :multi_match,
      text: text!(text),
      columns: columns,
      boosts: Enum.map(boosts, &positive_float!/1),
      operator: operator!(opts)
    })
  end

  @doc """
  Combines query lists in `:must`, `:should`, and `:must_not` clauses.
  At least one positive clause is required. With `:must`, `:should` clauses
  contribute scores but are optional; without `:must`, at least one should match.
  Children can be structured queries or plain strings.
  """
  @spec boolean(keyword()) :: t()
  def boolean(opts) do
    opts = Keyword.validate!(opts, [:must, :should, :must_not])
    clauses = Map.new([:must, :should, :must_not], &{&1, children!(Keyword.get(opts, &1, []))})

    if clauses.must == [] and clauses.should == [],
      do: raise(ArgumentError, "boolean queries require at least one must or should clause")

    wrap(Map.put(clauses, :type, :boolean))
  end

  @doc """
  Matches `positive` and subtracts `negative_boost * negative_score` from the
  score of rows also matching `negative`. `:negative_boost` is a non-negative
  float32 number (default 0.5). Scores can become negative; rows are not excluded.
  Use a Boolean `:must_not` clause to exclude matches.
  """
  @spec boost(input(), input(), keyword()) :: t()
  def boost(positive, negative, opts \\ []) do
    opts = Keyword.validate!(opts, [:negative_boost])
    factor = Keyword.get(opts, :negative_boost, 0.5)

    if !(is_number(factor) and factor >= 0 and factor <= 3.402_823_466_385_288_6e38),
      do: raise(ArgumentError, "negative_boost must be a non-negative float32-compatible number")

    wrap(%{
      type: :boost,
      positive: child!(positive),
      negative: child!(negative),
      negative_boost: factor / 1
    })
  end

  @doc """
  Matches a scalar at a dotted path inside an index using `document_tokenizer: :json`.
  Values are strings, numbers, booleans or nil. Accepts string match/2 options; fuzzy
  matching applies to the value while the path and scalar type remain exact.
  The pinned tokenizer cannot escape semicolons in values, or commas/semicolons in paths.
  """
  @spec json_match(String.t(), String.t() | number() | boolean() | nil, keyword()) :: t()
  def json_match(path, value, opts \\ []) do
    path = text!(path)

    if String.contains?(path, [",", ";"]),
      do: raise(ArgumentError, "JSON query paths cannot contain commas or semicolons")

    {type, text} = json_scalar!(value)

    if String.contains?(text, ";"),
      do: raise(ArgumentError, "JSON query values cannot contain semicolons")

    match("#{path},#{type},#{text}", opts)
  end

  defp json_scalar!(value) when is_binary(value), do: {"str", text!(value)}
  defp json_scalar!(nil), do: {"null", "null"}
  defp json_scalar!(value) when is_boolean(value), do: {"bool", to_string(value)}
  defp json_scalar!(value) when is_number(value), do: {"number", JSON.encode!(value)}
  defp json_scalar!(_), do: raise(ArgumentError, "JSON query values must be scalars")

  @doc false
  @spec query!(input()) :: String.t() | map()
  def query!(%__MODULE__{query: query}), do: query
  def query!(text), do: text!(text)

  defp prepared_match(prepared, opts) do
    if !Keyword.keyword?(opts),
      do: raise(ArgumentError, "FTS match options must be a keyword list")

    opts =
      Keyword.validate!(opts, [
        :column,
        :operator,
        :boost,
        :fuzziness,
        :max_expansions,
        :prefix_length,
        :exact_numbers,
        :exact_boost
      ])

    policy = prepared_fuzziness!(Keyword.get(opts, :fuzziness, 0))
    exact_numbers = Keyword.get(opts, :exact_numbers, true)

    if !is_boolean(exact_numbers), do: raise(ArgumentError, "exact_numbers must be a boolean")

    options = match_options!(Keyword.drop(opts, [:exact_numbers, :exact_boost, :fuzziness]))
    factor = positive_float!(Keyword.get(opts, :exact_boost, 1))
    exact_options = %{options | boost: positive_float!(options.boost * factor)}
    terms = Prepared.terms(prepared)
    if terms == [], do: raise(ArgumentError, "cannot match an empty FTS preparation")

    clauses =
      Enum.map(terms, &prepared_term(&1, policy, exact_numbers, options, exact_options))

    case clauses do
      [clause] -> clause
      clauses -> boolean([{if(options.operator == :and, do: :must, else: :should), clauses}])
    end
  end

  defp prepared_term(term, policy, exact_numbers, options, exact_options) do
    # All original spellings produced the same indexed term. One representative
    # avoids inflating scores for repeated input terms.
    exact = wrap(Map.put(exact_options, :text, hd(term.originals)))

    distance =
      if exact_numbers and Enum.any?(term.originals, &Regex.match?(~r/^\p{Nd}+$/u, &1)),
        do: 0,
        else: prepared_distance(policy, term.analyzed)

    if distance == 0,
      do: exact,
      else:
        boolean(
          should: [exact, wrap(Map.merge(options, %{text: term.analyzed, fuzziness: distance}))]
        )
  end

  defp prepared_fuzziness!(value) when value in [0, 1, 2], do: value

  defp prepared_fuzziness!({:length, one, two} = policy)
       when is_integer(one) and is_integer(two) and one > 0 and two > one,
       do: policy

  defp prepared_fuzziness!(_),
    do:
      raise(
        ArgumentError,
        "prepared fuzziness must be 0, 1, 2, or {:length, one, two} with 0 < one < two"
      )

  defp prepared_distance({:length, one, two}, text) do
    length = String.length(text)

    cond do
      length < one -> 0
      length < two -> 1
      true -> 2
    end
  end

  defp prepared_distance(fixed, _text), do: fixed

  defp wrap(query), do: %__MODULE__{query: query}
  defp child!(%__MODULE__{query: query}), do: query
  defp child!(text), do: text |> __MODULE__.match() |> query!()

  defp children!(children) when is_list(children), do: Enum.map(children, &child!/1)
  defp children!(_), do: raise(ArgumentError, "boolean clauses must be lists of queries")

  defp text!(text) do
    if !(is_binary(text) and String.valid?(text) and String.trim(text) != ""),
      do:
        raise(ArgumentError, "search text must be a non-empty UTF-8 string or LanceDB.FTS query")

    text
  end

  defp column!(opts) do
    case Keyword.get(opts, :column) do
      nil -> nil
      name -> column_name!(name)
    end
  end

  defp operator!(opts) do
    value = Keyword.get(opts, :operator, :or)
    if value not in [:and, :or], do: raise(ArgumentError, "operator must be :and or :or")
    value
  end

  defp fuzziness!(value) when value in [0, 1, 2], do: value
  defp fuzziness!(_), do: raise(ArgumentError, "fuzziness must be 0, 1, or 2")

  defp count!(opts, key, default, minimum) do
    value = Keyword.get(opts, key, default)

    if !(is_integer(value) and value >= minimum and value <= 4_294_967_295),
      do: raise(ArgumentError, "#{key} must be an integer between #{minimum} and 4_294_967_295")

    value
  end

  defp positive_float!(value) do
    if !(is_number(value) and value >= 1.1754943508222875e-38 and
           value <= 3.402_823_466_385_288_6e38),
       do: raise(ArgumentError, "boosts must be positive float32-compatible numbers")

    value / 1
  end
end
