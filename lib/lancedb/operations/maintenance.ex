defmodule LanceDB.Operations.Maintenance do
  alias LanceDB.Backend.ResultCodec
  @moduledoc false
  alias LanceDB.{Backend, Options, Result}

  def table_stats(table), do: Result.decode(Backend.table_stats(table))

  def optimize_indices(table, opts) do
    opts = Keyword.validate!(opts, [:index_names, :num_indices_to_merge, :retrain])
    validate_booleans!(opts, [:retrain])
    validate_optional_count!(opts, :num_indices_to_merge)

    if opts[:retrain] == true and Keyword.has_key?(opts, :num_indices_to_merge),
      do: raise(ArgumentError, "retrain and num_indices_to_merge cannot be combined")

    opts =
      if Keyword.has_key?(opts, :index_names),
        do: Keyword.update!(opts, :index_names, &Options.column_names!/1),
        else: opts

    Backend.optimize_indices(table, encode(opts))
  end

  @compact_sizes [
    :target_rows_per_fragment,
    :max_rows_per_group,
    :max_bytes_per_file,
    :num_threads,
    :batch_size,
    :io_buffer_size,
    :binary_copy_read_batch_bytes,
    :max_source_fragments
  ]
  def compact_files(table, opts) do
    opts =
      Keyword.validate!(
        opts,
        @compact_sizes ++
          [
            :materialize_deletions,
            :materialize_deletions_threshold,
            :defer_index_remap,
            :compaction_mode,
            :index_remap_mode,
            :max_overlays_per_fragment
          ]
      )

    for key <- @compact_sizes, Keyword.has_key?(opts, key), do: count!(opts[key], key, 1)
    validate_optional_count!(opts, :max_overlays_per_fragment)
    validate_booleans!(opts, [:materialize_deletions, :defer_index_remap])

    if Keyword.has_key?(opts, :materialize_deletions_threshold) do
      threshold = opts[:materialize_deletions_threshold]

      if !(is_number(threshold) and threshold >= 0 and threshold <= 1),
        do: raise(ArgumentError, "materialize_deletions_threshold must be between 0 and 1")
    end

    enum!(opts, :compaction_mode, [:reencode, :try_binary_copy, :force_binary_copy])
    enum!(opts, :index_remap_mode, [:direct, :compact])

    ResultCodec.decode(
      Backend.compact_files(table, encode(opts)),
      LanceDB.Maintenance.Compaction,
      :one
    )
  end

  def cleanup_old_versions(table, opts) do
    opts =
      Keyword.validate!(opts, [
        :older_than_seconds,
        :delete_unverified,
        :error_if_tagged_old_versions
      ])

    seconds = Keyword.get(opts, :older_than_seconds, 604_800)

    if !(is_integer(seconds) and seconds >= 0 and seconds <= 31_536_000_000),
      do:
        raise(ArgumentError, "older_than_seconds must be an integer between 0 and 31_536_000_000")

    validate_booleans!(opts, [:delete_unverified, :error_if_tagged_old_versions])

    Result.decode(
      Backend.cleanup_old_versions(table, encode(Keyword.put(opts, :older_than_seconds, seconds)))
    )
  end

  defp encode(opts), do: Map.new(opts)

  defp count!(value, key, min) do
    if !(is_integer(value) and value >= min and value <= 4_294_967_295),
      do: raise(ArgumentError, "#{key} must be an integer between #{min} and 4_294_967_295")
  end

  defp validate_optional_count!(opts, key) do
    if Keyword.has_key?(opts, key) and not is_nil(opts[key]), do: count!(opts[key], key, 0)
  end

  defp validate_booleans!(opts, keys) do
    for key <- keys, Keyword.has_key?(opts, key) do
      if not is_boolean(opts[key]), do: raise(ArgumentError, "#{key} must be a boolean")
    end
  end

  defp enum!(opts, key, allowed) do
    if Keyword.has_key?(opts, key) and opts[key] not in allowed,
      do: raise(ArgumentError, "invalid #{key}")
  end
end
