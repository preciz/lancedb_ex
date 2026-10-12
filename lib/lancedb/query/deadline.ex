defmodule LanceDB.Query.Deadline do
  @moduledoc false
  @type t :: {integer(), pos_integer() | nil}
  @spec start(pos_integer() | nil) :: t()
  def start(timeout), do: {System.monotonic_time(:millisecond), timeout}
  @spec elapsed(t()) :: non_neg_integer()
  def elapsed({started, _}), do: System.monotonic_time(:millisecond) - started
  @spec remaining(t()) :: timeout()
  def remaining({_, nil}), do: :infinity
  def remaining({_, timeout} = deadline), do: max(timeout - elapsed(deadline), 0)
end
