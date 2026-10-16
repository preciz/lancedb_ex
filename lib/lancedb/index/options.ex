defmodule LanceDB.Index.Options do
  alias LanceDB.Index.FTSOptions
  @moduledoc false
  @vector_types [:ivf_flat, :ivf_pq, :ivf_sq, :ivf_rq, :hnsw_flat, :hnsw_pq, :hnsw_sq]
  @hnsw_types [:hnsw_flat, :hnsw_pq, :hnsw_sq]
  @training_options [:num_partitions, :sample_rate, :max_iterations, :target_partition_size]
  @count_options @training_options ++ [:num_sub_vectors, :m, :ef_construction]

  @spec parse!(keyword()) :: map()
  def parse!(opts) do
    opts =
      Keyword.validate!(
        opts,
        [
          :type,
          :name,
          :replace,
          :train,
          :distance,
          :num_partitions,
          :num_sub_vectors,
          :sample_rate,
          :max_iterations,
          :target_partition_size,
          :num_bits,
          :m,
          :ef_construction,
          :analyzer
        ] ++ FTSOptions.keys()
      )

    type = Keyword.get(opts, :type, :auto)

    if type not in ([:auto, :btree, :bitmap, :label_list, :fm, :zonemap, :fts] ++ @vector_types),
      do: raise(ArgumentError, "unsupported index type")

    train = train!(opts, type)

    replace = Keyword.get(opts, :replace, false)
    if not is_boolean(replace), do: raise(ArgumentError, "replace must be a boolean")
    name = Keyword.get(opts, :name)

    validate_name!(name)

    distance = Keyword.get(opts, :distance, :l2)

    if distance not in [:l2, :cosine, :dot, :hamming],
      do: raise(ArgumentError, "invalid distance")

    validate_vector_options!(opts, type)
    validate_counts!(opts)
    validate_quantization!(opts, type)

    if Keyword.has_key?(opts, :analyzer) and type != :fts,
      do: raise(ArgumentError, "analyzer options require an FTS index")

    analyzer =
      if Keyword.has_key?(opts, :analyzer),
        do: LanceDB.Analyzer.options!(Keyword.fetch!(opts, :analyzer)),
        else: nil

    %{
      type: type,
      name: name,
      replace: replace,
      train: train,
      distance: distance,
      num_partitions: Keyword.get(opts, :num_partitions),
      num_sub_vectors: Keyword.get(opts, :num_sub_vectors),
      sample_rate: Keyword.get(opts, :sample_rate),
      max_iterations: Keyword.get(opts, :max_iterations),
      target_partition_size: Keyword.get(opts, :target_partition_size),
      num_bits: Keyword.get(opts, :num_bits),
      m: Keyword.get(opts, :m),
      ef_construction: Keyword.get(opts, :ef_construction),
      analyzer: analyzer,
      fts: FTSOptions.parse!(opts, type)
    }
  end

  defp train!(opts, type) do
    train = Keyword.get(opts, :train, true)
    if not is_boolean(train), do: raise(ArgumentError, "train must be a boolean")

    if not train and type not in [:btree, :bitmap, :label_list, :fm, :zonemap, :fts],
      do: raise(ArgumentError, "train: false requires an explicit scalar index type")

    train
  end

  defp validate_name!(name) do
    if !(is_nil(name) or (is_binary(name) and String.valid?(name) and String.trim(name) != "")),
      do: raise(ArgumentError, "index name must be a non-empty string")
  end

  defp validate_vector_options!(opts, type) do
    if !(type in @vector_types or
           not Enum.any?(
             [:distance, :num_sub_vectors, :num_bits] ++ @training_options,
             &Keyword.has_key?(opts, &1)
           )),
       do: raise(ArgumentError, "vector options require an explicit vector index type")

    if Keyword.has_key?(opts, :num_sub_vectors) and type not in [:ivf_pq, :hnsw_pq],
      do: raise(ArgumentError, "num_sub_vectors requires :ivf_pq or :hnsw_pq")

    if Enum.any?([:m, :ef_construction], &Keyword.has_key?(opts, &1)) and
         type not in @hnsw_types,
       do: raise(ArgumentError, "m and ef_construction require an HNSW index")
  end

  defp validate_counts!(opts) do
    if Keyword.has_key?(opts, :num_partitions) and Keyword.has_key?(opts, :target_partition_size),
      do: raise(ArgumentError, "num_partitions and target_partition_size cannot be combined")

    for key <- @count_options,
        Keyword.has_key?(opts, key) do
      value = Keyword.fetch!(opts, key)

      if !(is_integer(value) and value > 0 and value <= 4_294_967_295),
        do: raise(ArgumentError, "#{key} must be a positive uint32 integer")
    end
  end

  defp validate_quantization!(opts, type) do
    if Keyword.has_key?(opts, :num_bits) do
      bits = Keyword.fetch!(opts, :num_bits)
      valid = if type == :ivf_rq, do: 1..9, else: [4, 8]

      if !(type in [:ivf_pq, :hnsw_pq, :ivf_rq] and is_integer(bits) and bits in valid),
        do: raise(ArgumentError, "num_bits requires PQ (4 or 8) or IVF-RQ (1 through 9)")

      if bits == 4 and type in [:ivf_pq, :hnsw_pq] and
           rem(Keyword.get(opts, :num_sub_vectors, 2), 2) != 0,
         do: raise(ArgumentError, "4-bit PQ requires an even num_sub_vectors")
    end
  end
end
