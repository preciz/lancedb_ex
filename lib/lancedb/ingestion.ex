defmodule LanceDB.Ingestion do
  @moduledoc false
  alias LanceDB.Backend
  alias LanceDB.{Error, Records, WriteResult}
  alias LanceDB.Ingestion.{Plan, Source}

  @spec add(LanceDB.Table.t(), [map()] | LanceDB.Source.t(), keyword()) ::
          {:ok, WriteResult.t()} | {:error, map() | Error.t() | String.t()}
  def add(table, input, opts) do
    source = LanceDB.Source.coerce(input)
    plan = Plan.new(opts)
    batches = LanceDB.Source.chunks(source)

    with :ok <- LanceDB.Source.check_schema(table, source) do
      if plan.commit == :once,
        do: add_once(table, batches, plan),
        else: commit_batches(table, batches, plan)
    end
  end

  defp execute_batch(table, batch, plan), do: add_once(table, [batch], plan)

  defp commit_batches(table, batches, options) do
    reduce_batches(batches, {:ok, %WriteResult{inserted: 0, commits: 0}}, fn item, {:ok, total} ->
      outcome =
        case item do
          {:ok, batch} -> execute_batch(table, batch, options)
          {:error, reason} -> {:error, reason}
        end

      case outcome do
        {:ok, result} ->
          {:cont,
           {:ok,
            %{
              total
              | inserted: total.inserted + result.inserted,
                version: result.version || total.version,
                commits: total.commits + result.commits
            }}}

        {:error, reason} ->
          {:halt,
           {:error,
            %{
              inserted: total.inserted,
              version: total.version,
              commits: total.commits,
              reason: reason
            }}}
      end
    end)
  end

  defp add_once(table, batches, options) do
    case Backend.ingest_open(table, options.options, options.progress) do
      {:ok, writer} -> run_writer(writer, batches, options.mode)
      {:error, reason} -> {:error, %{inserted: 0, reason: reason}}
    end
  end

  @spec run_operation((-> Backend.result(reference())), Enumerable.t()) ::
          Backend.result(Backend.write_output()) | {:error, map() | Error.t() | String.t()}
  def run_operation(open, batches) do
    case open.() do
      {:ok, writer} -> run_writer(writer, batches, :operation)
      error -> error
    end
  end

  defp run_writer(writer, batches, mode) do
    batches
    |> reduce_batches({:ok, 0}, &push_batch(writer, &1, &2))
    |> finish_writer(writer, mode)
  after
    Backend.ingest_abort(writer)
  end

  defp reduce_batches(batches, initial, reducer) do
    case Source.reduce_while(batches, initial, reducer) do
      {result, nil} -> result
      {result, cleanup_error} -> attach_cleanup_error(result, cleanup_error)
    end
  end

  defp attach_cleanup_error({:error, %{reason: reason} = progress}, cleanup_error),
    do: {:error, %{progress | reason: with_cleanup_error(reason, cleanup_error)}}

  defp attach_cleanup_error({:error, reason}, cleanup_error),
    do: {:error, with_cleanup_error(reason, cleanup_error)}

  defp with_cleanup_error(reason, cleanup_error) do
    error = Error.normalize(reason, nil)
    %{error | details: Map.put(error.details, :cleanup_error, cleanup_error)}
  end

  defp push_batch(_writer, {:error, reason}, _count),
    do: {:halt, {:error, %{inserted: 0, commits: 0, version: nil, reason: reason}}}

  defp push_batch(writer, {:ok, %LanceDB.Batch{} = batch}, {:ok, count}) do
    case Backend.ingest_push_batch(writer, batch) do
      {:ok, :ok} -> {:cont, {:ok, count + LanceDB.Batch.size(batch)}}
      {:error, reason} -> {:halt, {:error, %{inserted: 0, reason: reason}}}
    end
  end

  defp push_batch(writer, {:ok, batch}, {:ok, count}) do
    with {:ok, records_wire} <- Records.prepare(batch, :typed, true),
         {:ok, :ok} <- Backend.ingest_push(writer, records_wire) do
      {:cont, {:ok, count + length(batch)}}
    else
      {:error, reason} -> {:halt, {:error, %{inserted: 0, reason: reason}}}
    end
  end

  defp finish_writer({:ok, _count}, writer, :operation), do: Backend.ingest_finish(writer)
  defp finish_writer(error, _writer, :operation), do: error
  defp finish_writer({:ok, 0}, _writer, :append), do: {:ok, %WriteResult{inserted: 0, commits: 0}}

  defp finish_writer(result, writer, mode) when mode in [:append, :overwrite],
    do: finish_writer(result, writer)

  defp finish_writer({:ok, _}, writer) do
    case Backend.ingest_finish(writer) do
      {:error, reason} -> {:error, %{inserted: 0, reason: reason}}
      {:ok, result} -> {:ok, %{WriteResult.counts(result, :add) | commits: 1}}
    end
  end

  defp finish_writer(error, _writer), do: error
end
