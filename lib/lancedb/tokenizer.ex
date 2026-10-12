defmodule LanceDB.Tokenizer do
  @moduledoc "Token inspection using explicit analyzers or a table index."
  alias LanceDB.Analyzer
  alias LanceDB.Backend
  import LanceDB.Options, only: [column_name!: 1]
  @type token :: %{String.t() => String.t() | non_neg_integer()}

  @doc """
  Inspects tokens using an analyzer keyword list, with the same options as
  FTS index creation. Returns maps with string keys `"text"` and `"position"`;
  positions start at zero and can have gaps after stop-word removal.
  Empty text is accepted. Invalid UTF-8 or options raise `ArgumentError`.
  """
  @spec tokenize(String.t(), keyword()) :: LanceDB.result([token()])
  def tokenize(text, analyzer \\ []),
    do: LanceDB.Execution.run(:tokenize, fn -> do_tokenize(text, analyzer) end)

  defp do_tokenize(text, analyzer) do
    validate_text!(text)
    options = Analyzer.options!(analyzer)
    decode(Backend.tokenize(text, options))
  end

  @doc """
  Inspects tokens with a table index's saved analyzer, including on a snapshot.
  Supply exactly one selector, `:index` or `:column`, as a string or atom.
  Column selection requires exactly one FTS index. Missing, ambiguous, or
  non-FTS indexes return an error. The operation changes neither table nor index.
  """
  @spec tokenize(LanceDB.Table.t(), String.t(), keyword()) :: LanceDB.result([token()])
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
