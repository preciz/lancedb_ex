defmodule LanceDB.TableCache.Warming do
  @moduledoc """
  A recent warming attempt for an index retained by `LanceDB.TableCache`.
  `:completed` records successful prewarming for `version` and `index_uuid`;
  it does not promise that cache pages remain resident. Identity fields describe
  the last successful attempt while another request is queued or running.
  Failed attempts contain a `LanceDB.Error` and clear the completed identity.
  Finished records may be discarded to make room for new warming requests.
  """
  defstruct status: :queued, version: nil, index_uuid: nil, duration_ms: nil, error: nil

  @type t :: %__MODULE__{
          status: :queued | :running | :completed | :failed,
          version: pos_integer() | nil,
          index_uuid: String.t() | nil,
          duration_ms: non_neg_integer() | nil,
          error: LanceDB.Error.t() | nil
        }
end
