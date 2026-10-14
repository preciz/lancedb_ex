defmodule LanceDB.FTS.Preparation do
  @moduledoc false
  alias LanceDB.{Analyzer, Backend, Execution}
  alias LanceDB.FTS.Prepared
  import LanceDB.Options, only: [column_name!: 1]

  @spec prepare(String.t(), keyword()) :: LanceDB.result(Prepared.t())
  def prepare(text, opts) do
    opts = options!(opts, [:analyzer, :max_bytes, :max_terms])
    max_terms = validate_input!(text, opts)
    analyzer = Analyzer.options!(Keyword.get(opts, :analyzer, []))

    if analyzer.tokenizer != :simple or Map.get(analyzer, :document_tokenizer, :text) != :text,
      do: raise(ArgumentError, "FTS preparation requires plain text with the simple tokenizer")

    Execution.run(:prepare_fts, fn ->
      decode(Backend.prepare_fts(text, analyzer, max_terms), max_terms)
    end)
  end

  @spec prepare(LanceDB.Table.t(), String.t(), keyword()) :: LanceDB.result(Prepared.t())
  def prepare(table, text, opts) do
    opts = options!(opts, [:index, :column, :max_bytes, :max_terms])
    max_terms = validate_input!(text, opts)

    {selector, name} =
      case Keyword.take(opts, [:index, :column]) do
        [{selector, name}] -> {to_string(selector), column_name!(name)}
        _ -> raise ArgumentError, "specify exactly one of :index or :column"
      end

    Execution.run(:prepare_fts, fn ->
      decode(Backend.prepare_fts_table(table, text, selector, name, max_terms), max_terms)
    end)
  end

  defp options!(opts, keys) do
    if !Keyword.keyword?(opts),
      do: raise(ArgumentError, "FTS preparation options must be a keyword list")

    Keyword.validate!(opts, keys)
  end

  defp validate_input!(text, opts) do
    if !is_binary(text),
      do: raise(ArgumentError, "FTS preparation text must be a UTF-8 string")

    max_bytes = limit!(opts, :max_bytes, 2048)
    max_terms = limit!(opts, :max_terms, 32)

    if max_bytes && byte_size(text) > max_bytes,
      do: raise(ArgumentError, "FTS preparation text exceeds :max_bytes")

    if !String.valid?(text),
      do: raise(ArgumentError, "FTS preparation text must be a UTF-8 string")

    max_terms
  end

  defp limit!(opts, key, default) do
    value = Keyword.get(opts, key, default)

    if !(is_nil(value) or (is_integer(value) and value > 0 and value <= 4_294_967_295)),
      do: raise(ArgumentError, "#{key} must be nil or a positive uint32 integer")

    value
  end

  defp decode({:ok, json}, max_terms) do
    prepared = json |> JSON.decode!() |> Prepared.new()

    if max_terms && Prepared.term_count(prepared) > max_terms,
      do: raise(ArgumentError, "FTS preparation exceeds :max_terms")

    {:ok, prepared}
  end

  defp decode(error, _max_terms), do: error
end
