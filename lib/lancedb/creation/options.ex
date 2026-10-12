defmodule LanceDB.Creation.Options do
  alias LanceDB.Blob.WriteOptions
  alias LanceDB.Options
  @moduledoc false
  @sizes [:max_rows_per_file, :max_rows_per_group, :max_bytes_per_file]
  @booleans [:enable_stable_row_ids, :enable_v2_manifest_paths]
  @keys [:mode, :schema, :schema_metadata, :location, :storage_options, :data_storage_version] ++
          @sizes ++ @booleans ++ WriteOptions.keys()

  @spec parse!(keyword()) :: keyword()
  def parse!(opts) do
    opts = Keyword.validate!(opts, @keys)
    WriteOptions.validate!(opts)
    mode = Keyword.get(opts, :mode, :create)

    if mode not in [:create, :overwrite, :exist_ok],
      do: raise(ArgumentError, "invalid creation mode")

    validate_sizes!(opts)
    validate_booleans!(opts)
    validate_version!(opts)

    Enum.map(opts, fn
      {:location, location} ->
        {:location, LanceDB.Options.string!(location, "location")}

      {:storage_options, options} ->
        {:storage_options, LanceDB.StorageOptions.normalize!(options)}

      {:schema_metadata, metadata} ->
        {:schema_metadata, Options.metadata!(metadata)}

      pair ->
        pair
    end)
  end

  defp validate_sizes!(opts), do: LanceDB.WriteOptions.validate_sizes!(opts, @sizes)

  defp validate_booleans!(opts) do
    for key <- @booleans, Keyword.has_key?(opts, key) do
      if not is_boolean(opts[key]), do: raise(ArgumentError, "#{key} must be a boolean")
    end
  end

  defp validate_version!(opts) do
    if Keyword.has_key?(opts, :data_storage_version) and
         opts[:data_storage_version] not in ["legacy", "2.0", "2.1", "2.2", "2.3", :stable, :next],
       do: raise(ArgumentError, "invalid data_storage_version")
  end

  @spec native_options(keyword()) :: map()
  def native_options(opts) do
    opts
    |> Keyword.drop([:schema, :schema_metadata])
    |> Keyword.update(:mode, :create, fn
      :exist_ok -> :create
      mode -> mode
    end)
    |> Map.new()
  end

  @spec open_options(keyword()) :: map()
  def open_options(opts),
    do: opts |> Keyword.take([:location, :storage_options]) |> Map.new()
end
