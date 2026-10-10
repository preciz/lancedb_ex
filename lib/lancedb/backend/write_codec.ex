defmodule LanceDB.Backend.WriteCodec do
  @moduledoc false
  alias LanceDB.Blob.WriteOptions

  def encode(options) do
    keys = WriteOptions.keys()

    options
    |> Map.drop(keys)
    |> Map.put(:blob, Map.take(options, keys))
    |> JSON.encode!()
  end
end
