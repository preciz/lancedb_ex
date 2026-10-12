defmodule LanceDB.Ingestion.Plan do
  alias LanceDB.Blob.WriteOptions
  @moduledoc false
  defstruct [:options, :progress, :mode, :commit]

  @type t :: %__MODULE__{
          options: map(),
          progress: {pid(), term()} | nil,
          mode: :append | :overwrite,
          commit: :once | :batch
        }
  @sizes [:write_parallelism, :max_rows_per_file, :max_rows_per_group, :max_bytes_per_file]
  @spec new(keyword()) :: t()
  def new(opts) do
    {commit, opts} = Keyword.pop(opts, :commit, :once)
    if commit not in [:batch, :once], do: raise(ArgumentError, "commit must be :batch or :once")

    opts =
      Keyword.validate!(opts, [:mode, :progress] ++ @sizes ++ WriteOptions.keys())

    mode = Keyword.get(opts, :mode, :append)
    progress = Keyword.get(opts, :progress)

    if mode not in [:append, :overwrite],
      do: raise(ArgumentError, "mode must be :append or :overwrite")

    if commit == :batch and mode == :overwrite,
      do: raise(ArgumentError, "overwrite requires commit: :once")

    validate_progress!(progress)
    validate_sizes!(opts)
    WriteOptions.validate!(opts)

    options =
      opts
      |> Keyword.drop([:progress])
      |> Map.new()

    %__MODULE__{options: options, progress: progress, mode: mode, commit: commit}
  end

  defp validate_progress!(nil), do: :ok

  defp validate_progress!({pid, _tag}) when is_pid(pid) and node(pid) == node(), do: :ok

  defp validate_progress!(_), do: raise(ArgumentError, "progress must be {local_pid, tag}")

  defp validate_sizes!(opts), do: LanceDB.WriteOptions.validate_sizes!(opts, @sizes)
end
