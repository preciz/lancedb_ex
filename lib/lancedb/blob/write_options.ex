defmodule LanceDB.Blob.WriteOptions do
  @moduledoc false
  @keys [:external_blob_mode, :allow_external_blob_outside_bases, :blob_pack_file_size_threshold]
  @spec keys() :: [atom()]
  def keys, do: @keys

  @spec validate!(keyword()) :: nil
  def validate!(opts) do
    mode = Keyword.get(opts, :external_blob_mode, :reference)

    if mode not in [:reference, :ingest],
      do: raise(ArgumentError, "external_blob_mode must be :reference or :ingest")

    allow = Keyword.get(opts, :allow_external_blob_outside_bases, false)

    if not is_boolean(allow),
      do: raise(ArgumentError, "allow_external_blob_outside_bases must be a boolean")

    if allow and mode == :ingest,
      do:
        raise(ArgumentError, "allow_external_blob_outside_bases only applies to :reference mode")

    if Keyword.has_key?(opts, :blob_pack_file_size_threshold) do
      size = opts[:blob_pack_file_size_threshold]

      if !(is_integer(size) and size > 0 and size <= 4_294_967_295),
        do:
          raise(ArgumentError, "blob_pack_file_size_threshold must be a positive uint32 integer")
    end
  end
end
