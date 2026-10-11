defmodule LanceDB.Blob do
  @moduledoc """
  Seekable handles for blob values stored in a table.

  Obtain handles with
  `LanceDB.Blob.open/3`, using `_rowid` values from a query with
  `with_row_id: true`. Offsets and lengths are bytes, relative to the blob.

  Reads return Elixir binaries. `read/2` and `seek/2` share a synchronized cursor
  when a handle is passed between processes; range reads and `stream/2` leave
  that cursor unchanged. Separate fetches return independent handles.
  `close/1` closes the handle for every process sharing it and is idempotent.
  Native references are also released when the handle is garbage collected.
  Keep the table version's files available while reading; version cleanup can
  invalidate handles. External references require the source object to remain available.
  """
  alias LanceDB.{Backend, Execution, Options}
  @enforce_keys [:resource]
  defstruct [:resource]
  @opaque t :: %__MODULE__{resource: reference()}
  @max_uint64 18_446_744_073_709_551_615

  @doc "Returns blob column paths in schema order, including dotted paths for nested blobs."
  def columns(table),
    do: LanceDB.Execution.run(:blob_columns, fn -> Backend.blob_columns(table) end)

  @doc """
  Materializes blobs as binaries in row ID order, preserving duplicates and nulls.
  Obtain IDs from a query with `with_row_id: true`; these are not row offsets or
  application primary keys. Supports top-level and struct-nested blob columns;
  the pinned SDK cannot address individual list elements by row ID.
  Invalid/missing IDs fail. Use range reads or file
  handles to limit memory for large payloads.
  """
  def fetch(table, column, row_ids) do
    LanceDB.Execution.run(:fetch_blobs, fn ->
      Backend.fetch_blobs(
        table,
        LanceDB.Options.column_name!(column),
        LanceDB.Blob.row_ids!(row_ids)
      )
    end)
  end

  @doc "Reads `{row_id, offset, length}` blob ranges in request order, preserving nulls. Ranges must fit each blob."
  def fetch_ranges(table, column, requests) do
    LanceDB.Execution.run(:fetch_blob_ranges, fn ->
      Backend.fetch_blob_ranges(
        table,
        LanceDB.Options.column_name!(column),
        LanceDB.Blob.requests!(requests)
      )
    end)
  end

  @doc "Opens seekable `LanceDB.Blob` handles in row ID order; null values return nil."
  def open(table, column, row_ids) do
    LanceDB.Execution.run(:fetch_blob_files, fn ->
      Backend.fetch_blob_files(
        table,
        LanceDB.Options.column_name!(column),
        LanceDB.Blob.row_ids!(row_ids)
      )
    end)
  end

  @doc "A blob write value naming an external object URI. Writes retain the reference by default; external_blob_mode: :ingest copies its bytes."
  def reference(uri), do: %LanceDB.Blob.Reference{uri: Options.string!(uri, "blob URI")}

  @doc "Reads from the current cursor to EOF, advancing the cursor."
  def read(%__MODULE__{} = blob),
    do: Execution.run(:blob_read, fn -> Backend.blob_read(blob, nil) end)

  @doc "Reads up to length bytes and advances the cursor. EOF returns an empty binary."
  def read(%__MODULE__{} = blob, length) do
    Execution.run(:blob_read, fn -> Backend.blob_read(blob, uint64!(length, "length")) end)
  end

  @doc "Reads exactly length bytes at offset without moving the cursor. Out-of-bounds ranges fail."
  def read_range(%__MODULE__{} = blob, offset, length) do
    Execution.run(:blob_read_range, fn ->
      with {:ok, [bytes]} <- Backend.blob_read_ranges(blob, [range!({offset, length})]),
           do: {:ok, bytes}
    end)
  end

  @doc "Reads `{offset, length}` ranges in input order without moving the cursor."
  def read_ranges(%__MODULE__{} = blob, ranges) do
    Execution.run(:blob_read_ranges, fn ->
      Backend.blob_read_ranges(blob, list!(ranges, &range!/1, "ranges"))
    end)
  end

  @doc "Sets the cursor's absolute byte offset. Seeking beyond EOF is allowed."
  def seek(%__MODULE__{} = blob, offset),
    do: Execution.run(:blob_seek, fn -> Backend.blob_seek(blob, uint64!(offset, "offset")) end)

  @doc "Returns the current cursor offset."
  def tell(%__MODULE__{} = blob),
    do: Execution.run(:blob_tell, fn -> Backend.blob_tell(blob) end)

  @doc "Returns the byte length; remains available after close."
  def size(%__MODULE__{} = blob), do: Backend.blob_size(blob)

  @doc "Returns storage metadata with string keys: size, position, data_path, kind, and uri."
  def info(%__MODULE__{} = blob),
    do: Execution.run(:blob_info, fn -> {:ok, JSON.decode!(Backend.blob_info(blob))} end)

  @doc "Closes the shared handle, returning `{:ok, :ok}`. Repeated closes succeed."
  def close(%__MODULE__{} = blob),
    do: Execution.run(:blob_close, fn -> Backend.blob_close(blob) end)

  @doc "Whether the shared handle is closed."
  def closed?(%__MODULE__{} = blob), do: Backend.blob_is_closed(blob)

  @doc """
  Lazily reads the entire blob in binary chunks (default `chunk_size: 65_536`).
  Enumeration starts at byte zero, leaves the cursor unchanged, and raises
  `LanceDB.Error` on failure. It borrows the handle: halting does not close it.
  Each chunk performs a bounded range read; repeated enumeration is supported.
  """
  def stream(%__MODULE__{} = blob, opts \\ []) do
    opts = Keyword.validate!(opts, [:chunk_size])
    chunk_size = uint64!(Keyword.get(opts, :chunk_size, 65_536), "chunk_size")
    if chunk_size == 0, do: raise(ArgumentError, "chunk_size must be positive")
    size = size(blob)

    Stream.unfold(0, &stream_next(blob, &1, size, chunk_size))
  end

  defp stream_next(_blob, offset, size, _chunk_size) when offset >= size, do: nil

  defp stream_next(blob, offset, size, chunk_size) do
    length = min(chunk_size, size - offset)

    case read_range(blob, offset, length) do
      {:ok, bytes} -> {bytes, offset + length}
      {:error, error} -> raise error
    end
  end

  @doc false
  def row_ids!(ids), do: list!(ids, &uint64!(&1, "row ID"), "row IDs")

  @doc false
  def requests!(requests), do: list!(requests, &request!/1, "blob range requests")

  defp request!({id, offset, length}) do
    {offset, length} = range!({offset, length})
    {uint64!(id, "row ID"), offset, length}
  end

  defp request!(_),
    do: raise(ArgumentError, "blob range requests must be {row_id, offset, length}")

  defp range!({offset, length}) do
    offset = uint64!(offset, "offset")
    length = uint64!(length, "length")
    if offset + length > @max_uint64, do: raise(ArgumentError, "blob range overflows uint64")
    {offset, length}
  end

  defp range!(_), do: raise(ArgumentError, "blob ranges must be {offset, length}")

  defp uint64!(value, _label) when is_integer(value) and value >= 0 and value <= @max_uint64,
    do: value

  defp uint64!(_value, label),
    do: raise(ArgumentError, "#{label} must be a non-negative uint64 integer")

  defp list!(values, fun, _label) when is_list(values), do: Enum.map(values, fun)
  defp list!(_values, _fun, label), do: raise(ArgumentError, "#{label} must be a list")
end
