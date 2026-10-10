defmodule LanceDB.Index.Description do
  @moduledoc "An index description. Type names remain strings so newer SDK index families do not create atoms."
  defstruct [
    :name,
    :index_type,
    :columns,
    :num_indexed_rows,
    :num_unindexed_rows,
    :index_uuid,
    :size_bytes
  ]

  @type t :: %__MODULE__{
          name: String.t() | nil,
          index_type: String.t() | nil,
          columns: [String.t()],
          num_indexed_rows: non_neg_integer() | nil,
          num_unindexed_rows: non_neg_integer() | nil,
          index_uuid: String.t() | nil,
          size_bytes: non_neg_integer() | nil
        }
end
