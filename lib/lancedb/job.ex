defmodule LanceDB.Job do
  @moduledoc """
  A shareable native index job. Jobs continue if the caller exits or the handle
  is garbage collected. Use cancel/1 explicitly; cancellation is best effort and
  does not undo a completed index. A job holds a write admission slot until it
  completes. Waiting with a deadline does not cancel the job.
  """
  @enforce_keys [:resource]
  defstruct [:resource]
  @opaque t :: %__MODULE__{resource: reference()}

  @doc "Returns the native status: running, finished, failed, or cancelled."
  @spec status(t()) :: LanceDB.result(String.t())
  def status(%__MODULE__{} = job),
    do: LanceDB.Execution.run(:job_status, fn -> LanceDB.Backend.job_status(job) end)

  @doc """
  Waits for completion, returning `{:ok, :created}` or the job's terminal error.
  `:timeout_ms` is nil (wait indefinitely) or a non-negative uint32 millisecond
  count; zero checks without waiting. A timeout bounds this wait and leaves
  the job running. Native waits use Tokio and reply without retaining a BEAM
  scheduler. A completed result is available to subsequent waits.
  """
  @spec wait(t(), keyword()) :: LanceDB.result(:created)
  def wait(%__MODULE__{} = job, opts \\ []) do
    opts = Keyword.validate!(opts, [:timeout_ms])
    timeout = opts[:timeout_ms]

    if !(is_nil(timeout) or (is_integer(timeout) and timeout >= 0 and timeout <= 4_294_967_295)),
      do: raise(ArgumentError, "timeout_ms must be nil or a non-negative uint32 integer")

    LanceDB.Execution.run(:job_wait, fn -> LanceDB.Backend.job_wait(job, timeout) end)
  end

  @doc "Requests cancellation. Call wait/2 to observe the terminal result."
  @spec cancel(t()) :: LanceDB.result(:ok)
  def cancel(%__MODULE__{} = job),
    do: LanceDB.Execution.run(:job_cancel, fn -> LanceDB.Backend.job_cancel(job) end)
end
