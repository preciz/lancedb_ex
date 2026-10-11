defmodule LanceDB.Execution do
  @moduledoc """
  Native operation admission. Limits apply across the BEAM VM: at most 64 active reads
  and 16 writes by default. A read cursor holds its slot until completion or close;
  read and write budgets are separate so batches can flow between them. Excess
  operations fail immediately with `Error.kind == :busy`. Existing operations
  finish if limits are lowered. Limits bound concurrency, not memory per operation.

  Query deadlines are cooperative, include cursor idle time, and cannot preempt
  synchronous codecs. Connect and open also accept cooperative native deadlines
  and cancel on caller death. Their storage waits run on Tokio and retain no
  BEAM scheduler. Cursor owner death cancels reads. Writer owner death errors
  input and awaits the native result; cancellation never promises rollback after
  final commit has begun. No table process or connection pool is created.
  """
  @doc "Sets positive read and write concurrency limits across the BEAM VM. Omitted limits revert to their defaults, 64 reads and 16 writes."
  def configure(opts) do
    opts = Keyword.validate!(opts, [:reads, :writes])
    reads = Keyword.get(opts, :reads, 64)
    writes = Keyword.get(opts, :writes, 16)

    if !(is_integer(reads) and reads > 0 and is_integer(writes) and writes > 0),
      do: raise(ArgumentError, "execution limits must be positive integers")

    LanceDB.Backend.execution_limits(reads, writes)
  end

  @doc false
  def run(operation, fun, metadata \\ %{}) do
    LanceDB.Telemetry.span(
      operation,
      fn ->
        case fun.() do
          {:error, reason} -> {:error, LanceDB.Error.normalize(reason, operation)}
          result -> result
        end
      end,
      metadata
    )
  end

  @doc false
  def write(operation, fun, decode) do
    run(operation, fn ->
      case fun.() do
        {:ok, value} -> {:ok, decode.(value, operation)}
        error -> error
      end
    end)
  end
end
