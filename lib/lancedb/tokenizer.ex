defmodule LanceDB.Tokenizer do
  @moduledoc "Token inspection using explicit analyzers or a table index."
  alias LanceDB.Analyzer
  alias LanceDB.Backend
  import LanceDB.Options, only: [column_name!: 1]

  def tokenize(text, analyzer \\ []),
    do: LanceDB.Execution.run(:tokenize, fn -> do_tokenize(text, analyzer) end)

  defp do_tokenize(text, analyzer) do
    validate_text!(text)
    options = Analyzer.options!(analyzer)
    decode(Backend.tokenize(text, options))
  end

  def tokenize(table, text, opts),
    do: LanceDB.Execution.run(:tokenize, fn -> do_tokenize(table, text, opts) end)

  defp do_tokenize(table, text, opts) do
    validate_text!(text)
    opts = Keyword.validate!(opts, [:index, :column])

    case opts do
      [{selector, name}] ->
        decode(Backend.tokenize_table(table, text, to_string(selector), column_name!(name)))

      _ ->
        raise ArgumentError, "specify exactly one of :index or :column"
    end
  end

  defp validate_text!(text) do
    if !(is_binary(text) and String.valid?(text)),
      do: raise(ArgumentError, "tokenization text must be a UTF-8 string")
  end

  defp decode({:ok, json}), do: JSON.decode(json)
  defp decode(error), do: error
end
