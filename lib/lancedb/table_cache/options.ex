defmodule LanceDB.TableCache.Options do
  @moduledoc false
  alias LanceDB.{Connection, Options, StorageOptions}

  defstruct [
    :uri,
    :storage_options,
    :connect_options,
    :open_options,
    :max_tables,
    :max_requests,
    :max_opens,
    :max_warmers,
    :idle_timeout_ms
  ]

  @type t :: %__MODULE__{
          uri: String.t(),
          storage_options: %{String.t() => String.t()},
          connect_options: keyword(),
          open_options: keyword(),
          max_tables: pos_integer(),
          max_requests: pos_integer(),
          max_opens: pos_integer(),
          max_warmers: pos_integer(),
          idle_timeout_ms: pos_integer() | nil
        }

  @connection [:read_consistency_interval_ms, :index_cache_size_bytes, :metadata_cache_size_bytes]

  @spec new(keyword()) :: t()
  def new(opts) do
    opts =
      Keyword.validate!(
        opts,
        @connection ++
          [
            :name,
            :uri,
            :storage_options,
            :max_tables,
            :max_requests,
            :max_opens,
            :max_warmers,
            :idle_timeout_ms,
            :connect_timeout_ms,
            :open_timeout_ms
          ]
      )

    connection =
      opts
      |> Keyword.take(@connection)
      |> Keyword.put(
        :timeout_ms,
        Options.timeout_ms!(Keyword.get(opts, :connect_timeout_ms, 5_000))
      )
      |> Connection.connect_options!()
      |> Map.to_list()

    idle = Keyword.get(opts, :idle_timeout_ms, 3_600_000)
    if idle != nil, do: positive!(idle, :idle_timeout_ms)

    %__MODULE__{
      uri: Options.string!(Keyword.get(opts, :uri), "uri"),
      storage_options: StorageOptions.normalize!(Keyword.get(opts, :storage_options, %{})),
      connect_options: connection,
      open_options: [timeout_ms: Options.timeout_ms!(Keyword.get(opts, :open_timeout_ms, 5_000))],
      max_tables: positive!(Keyword.get(opts, :max_tables, 64), :max_tables),
      max_requests: positive!(Keyword.get(opts, :max_requests, 256), :max_requests),
      max_opens: positive!(Keyword.get(opts, :max_opens, 4), :max_opens),
      max_warmers: positive!(Keyword.get(opts, :max_warmers, 2), :max_warmers),
      idle_timeout_ms: idle
    }
  end

  defp positive!(value, key) do
    if !(is_integer(value) and value > 0 and value <= 4_294_967_295),
      do: raise(ArgumentError, "#{key} must be a positive uint32 integer")

    value
  end
end
