defmodule LanceDB.Analyzer do
  @moduledoc false

  def options!(opts) do
    if !Keyword.keyword?(opts), do: raise(ArgumentError, "analyzer must be a keyword list")

    booleans = [
      :with_position,
      :lower_case,
      :stem,
      :remove_stop_words,
      :ascii_folding,
      :ngram_prefix_only,
      :split_identifiers,
      :split_on_numerics,
      :preserve_original,
      :index_operators
    ]

    opts =
      Keyword.validate!(
        opts,
        booleans ++
          [
            :tokenizer,
            :document_tokenizer,
            :custom_stop_words,
            :language,
            :max_token_length,
            :ngram_min_length,
            :ngram_max_length
          ]
      )

    tokenizer = Keyword.get(opts, :tokenizer, :simple)

    if tokenizer not in [:simple, :whitespace, :raw, :ngram, :code],
      do: raise(ArgumentError, "unsupported FTS tokenizer")

    validate_booleans!(opts, booleans)
    validate_lengths!(opts)
    validate_ngram!(opts, tokenizer)
    validate_language!(opts)
    validate_document!(opts)
    validate_code!(opts, tokenizer)

    opts
    |> Map.new()
    |> Map.put(:tokenizer, tokenizer)
    |> Map.put(
      :unlimited_token_length,
      Keyword.has_key?(opts, :max_token_length) and is_nil(Keyword.get(opts, :max_token_length))
    )
  end

  defp validate_booleans!(opts, booleans) do
    for key <- booleans, Keyword.has_key?(opts, key) do
      if not is_boolean(Keyword.fetch!(opts, key)),
        do: raise(ArgumentError, "#{key} must be a boolean")
    end
  end

  defp validate_lengths!(opts) do
    for key <- [:ngram_min_length, :ngram_max_length, :max_token_length],
        Keyword.has_key?(opts, key) do
      value = Keyword.fetch!(opts, key)

      if !((key == :max_token_length and is_nil(value)) or
             (is_integer(value) and value > 0 and value <= 4_294_967_295)),
         do:
           raise(
             ArgumentError,
             "#{key} must be a positive uint32 integer (max_token_length also accepts nil)"
           )
    end
  end

  defp validate_ngram!(opts, tokenizer) do
    if tokenizer != :ngram and
         Enum.any?(
           [:ngram_min_length, :ngram_max_length, :ngram_prefix_only],
           &Keyword.has_key?(opts, &1)
         ),
       do: raise(ArgumentError, "ngram options require the ngram tokenizer")

    if tokenizer == :ngram do
      if Keyword.get(opts, :ngram_min_length, 3) > Keyword.get(opts, :ngram_max_length, 3),
        do: raise(ArgumentError, "ngram_min_length cannot exceed ngram_max_length")

      if Keyword.get(opts, :with_position, false),
        do: raise(ArgumentError, "ngram does not support positions")
    end
  end

  defp validate_document!(opts) do
    if Keyword.has_key?(opts, :document_tokenizer) and
         opts[:document_tokenizer] not in [:text, :json],
       do: raise(ArgumentError, "document_tokenizer must be :text or :json")

    if Keyword.has_key?(opts, :custom_stop_words) do
      words = opts[:custom_stop_words]

      if !(is_nil(words) or
             (is_list(words) and
                Enum.all?(words, &(is_binary(&1) and String.valid?(&1) and &1 != "")))),
         do:
           raise(
             ArgumentError,
             "custom_stop_words must be nil or a list of non-empty UTF-8 strings"
           )
    end
  end

  defp validate_code!(opts, tokenizer) do
    if tokenizer != :code and
         Enum.any?(
           [:split_identifiers, :split_on_numerics, :preserve_original, :index_operators],
           &Keyword.has_key?(opts, &1)
         ),
       do: raise(ArgumentError, "code analysis options require tokenizer: :code")
  end

  defp validate_language!(opts) do
    if Keyword.has_key?(opts, :language) do
      language = Keyword.fetch!(opts, :language)

      if !(is_binary(language) and String.valid?(language) and String.trim(language) != ""),
        do: raise(ArgumentError, "language must be a non-empty string, such as English")
    end
  end
end
