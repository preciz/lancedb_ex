defmodule LanceDB.Query.Deadline do
  @moduledoc false
  def start(timeout), do: {System.monotonic_time(:millisecond), timeout}
  def elapsed({started, _}), do: System.monotonic_time(:millisecond) - started
  def remaining({_, nil}), do: :infinity
  def remaining({_, timeout} = deadline), do: max(timeout - elapsed(deadline), 0)
end
