defmodule LanceDB.Error do
  @moduledoc """
  An operational failure. Match on `kind`, not on the human-readable message.

  `details` may contain confirmed ingestion progress (`inserted`, `commits`, `version`),
  native `processed_rows`, `phase` (:validation, :input, :cleanup, :execution, :waiting), and
  `commit_outcome` (:not_started or :unknown). Processing is not proof of commit.
  Missing context is unspecified; unknown never promises rollback.
  A secondary source finalizer failure is recorded in `details.cleanup_error`
  without replacing the primary failure or its confirmed write progress.
  Invalid API arguments raise `ArgumentError` before execution. Lazy query failures
  raise this exception when the stream is enumerated.
  """
  defexception [:kind, :operation, :message, details: %{}]

  @type t :: %__MODULE__{
          kind: atom(),
          operation: atom() | nil,
          message: String.t(),
          details: map()
        }

  @doc false
  def normalize(%__MODULE__{} = error, operation),
    do: %{error | operation: operation || error.operation}

  def normalize(%{kind: kind, message: message} = error, operation),
    do: %__MODULE__{
      kind: kind,
      operation: operation,
      message: message,
      details: Map.get(error, :details, %{})
    }

  def normalize(%{reason: reason} = progress, operation) do
    error = normalize(reason, operation)
    %{error | details: Map.merge(error.details, Map.delete(progress, :reason))}
  end

  def normalize(message, operation) when is_binary(message),
    do: %__MODULE__{kind: :execution, operation: operation, message: message}
end
