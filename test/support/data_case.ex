defmodule LanceDB.DataCase do
  @moduledoc false
  use ExUnit.CaseTemplate

  using do
    quote do
      import LanceDB.TestSupport, only: [with_database: 1, assert_eventually: 1]
    end
  end

  setup do
    path = LanceDB.TestSupport.unique_path()
    on_exit(fn -> File.rm_rf!(path) end)
    {:ok, path: path}
  end
end
