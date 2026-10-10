defmodule LanceDB.Maintenance.Compaction do
  @moduledoc "Measured changes made by file compaction; every count is reported by the SDK."
  defstruct [:fragments_removed, :fragments_added, :files_removed, :files_added]

  @type t :: %__MODULE__{
          fragments_removed: non_neg_integer(),
          fragments_added: non_neg_integer(),
          files_removed: non_neg_integer(),
          files_added: non_neg_integer()
        }
end
