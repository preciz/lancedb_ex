defmodule LanceDB.Backend do
  alias LanceDB.Backend.WriteCodec
  @moduledoc false
  alias LanceDB.{Connection, Native, Table}
  @type result(value) :: Native.result(value)
  @type write_output :: Native.counts_output() | Native.version_output() | Table.t()

  @spec blob_columns(Table.t()) :: result([String.t()])
  def blob_columns(table), do: call(:blob_columns, [table])
  @spec fetch_blobs(Table.t(), String.t(), [non_neg_integer()]) :: result([binary() | nil])
  def fetch_blobs(table, column, row_ids), do: call(:fetch_blobs, [table, column, row_ids])

  @spec fetch_blob_ranges(Table.t(), String.t(), [LanceDB.Blob.request()]) ::
          result([binary() | nil])
  def fetch_blob_ranges(table, column, requests),
    do: call(:fetch_blob_ranges, [table, column, requests])

  @spec fetch_blob_files(Table.t(), String.t(), [non_neg_integer()]) ::
          result([LanceDB.Blob.t() | nil])
  def fetch_blob_files(table, column, row_ids),
    do: call(:fetch_blob_files, [table, column, row_ids])

  @spec blob_read(LanceDB.Blob.t(), non_neg_integer() | nil) :: result(binary())
  def blob_read(blob, length), do: call(:blob_read, [blob, length])
  @spec blob_read_ranges(LanceDB.Blob.t(), [LanceDB.Blob.byte_range()]) :: result([binary()])
  def blob_read_ranges(blob, ranges), do: call(:blob_read_ranges, [blob, ranges])
  @spec blob_seek(LanceDB.Blob.t(), non_neg_integer()) :: result(:ok)
  def blob_seek(blob, offset), do: call(:blob_seek, [blob, offset])
  @spec blob_tell(LanceDB.Blob.t()) :: result(non_neg_integer())
  def blob_tell(blob), do: call(:blob_tell, [blob])
  @spec blob_close(LanceDB.Blob.t()) :: result(:ok)
  def blob_close(blob), do: call(:blob_close, [blob])
  @spec blob_is_closed(LanceDB.Blob.t()) :: boolean()
  def blob_is_closed(blob), do: call(:blob_is_closed, [blob])
  @spec blob_size(LanceDB.Blob.t()) :: non_neg_integer()
  def blob_size(blob), do: call(:blob_size, [blob])
  @spec blob_info(LanceDB.Blob.t()) :: String.t()
  def blob_info(blob), do: call(:blob_info, [blob])

  @spec create_index_async(Table.t(), String.t(), LanceDB.Index.Definition.t()) ::
          result(LanceDB.Job.t())
  def create_index_async(table, column, options),
    do: call(:create_index_async, [table, column, JSON.encode!(options.options)])

  @spec job_status(LanceDB.Job.t()) :: result(String.t())
  def job_status(job), do: call(:job_status, [job])
  @spec job_wait(LanceDB.Job.t(), non_neg_integer() | nil) :: result(:created)
  def job_wait(job, timeout), do: call(:job_wait, [job, timeout])
  @spec job_cancel(LanceDB.Job.t()) :: result(:ok)
  def job_cancel(job), do: call(:job_cancel, [job])

  @spec create_stream_open(
          Connection.t(),
          String.t(),
          String.t(),
          %{String.t() => String.t()},
          map()
        ) :: result(reference())
  def create_stream_open(conn, name, schema, metadata, options),
    do: call(:create_stream_open, [conn, name, schema, metadata, WriteCodec.encode(options)])

  @spec merge_stream_open(Table.t(), [String.t()], map()) :: result(reference())
  def merge_stream_open(table, keys, options),
    do: call(:merge_stream_open, [table, keys, JSON.encode!(options)])

  @spec columns_stream_open(Table.t(), String.t(), pos_integer() | nil) :: result(reference())
  def columns_stream_open(table, schema, version),
    do: call(:columns_stream_open, [table, schema, version])

  @spec execution_limits(pos_integer(), pos_integer()) :: result(:ok)
  def execution_limits(reads, writes), do: call(:execution_limits, [reads, writes])
  @spec metrics_enable() :: result(boolean())
  def metrics_enable, do: call(:metrics_enable, [])
  @spec metrics_catalog() :: String.t()
  def metrics_catalog, do: call(:metrics_catalog, [])
  @spec metrics_snapshot() :: String.t()
  def metrics_snapshot, do: call(:metrics_snapshot, [])

  @spec connect(String.t(), %{String.t() => String.t()}, map()) :: result(Connection.t())
  def connect(uri, storage_options, options),
    do: call(:connect, [uri, storage_options, JSON.encode!(options)])

  @spec table_names(Connection.t(), String.t() | nil, pos_integer() | nil) :: result([String.t()])
  def table_names(conn, start_after, limit), do: call(:table_names, [conn, start_after, limit])
  @spec open_table(Connection.t(), String.t(), map()) :: result(Table.t())
  def open_table(conn, name, options), do: call(:open_table, [conn, name, JSON.encode!(options)])
  @spec drop_table(Connection.t(), String.t()) :: result(:dropped)
  def drop_table(conn, name), do: call(:drop_table, [conn, name])

  @spec batch_infer([map()]) :: result(LanceDB.Batch.t())
  def batch_infer(rows), do: call(:batch_infer, [rows])
  @spec schema_validate(String.t()) :: result(:ok)
  def schema_validate(schema), do: call(:schema_validate, [schema])

  @spec create_index(Table.t(), String.t(), LanceDB.Index.Definition.t()) :: result(:created)
  def create_index(table, column, options),
    do: call(:create_index, [table, column, JSON.encode!(options.options)])

  @spec list_indices(Table.t()) :: result(String.t())
  def list_indices(table), do: call(:list_indices, [table])
  @spec table_stats(Table.t()) :: result(String.t())
  def table_stats(table), do: call(:table_stats, [table])
  @spec cache_stats(Table.t()) :: result(String.t())
  def cache_stats(table), do: call(:cache_stats, [table])
  @spec index_stats(Table.t(), String.t()) :: result(String.t())
  def index_stats(table, name), do: call(:index_stats, [table, name])
  @spec index_readiness(Table.t(), [String.t()], pos_integer()) :: result([String.t()])
  def index_readiness(table, names, timeout), do: call(:index_readiness, [table, names, timeout])
  @spec drop_index(Table.t(), String.t()) :: result(:dropped)
  def drop_index(table, name), do: call(:drop_index, [table, name])

  @spec optimize(Table.t()) :: result(String.t())
  def optimize(table), do: call(:optimize, [table])

  @spec optimize_indices(Table.t(), map()) :: result(:optimized)
  def optimize_indices(table, options),
    do: call(:optimize_indices, [table, JSON.encode!(options)])

  @spec compact_files(Table.t(), map()) :: result(String.t())
  def compact_files(table, options), do: call(:compact_files, [table, JSON.encode!(options)])

  @spec cleanup_old_versions(Table.t(), map()) :: result(String.t())
  def cleanup_old_versions(table, options),
    do: call(:cleanup_old_versions, [table, JSON.encode!(options)])

  @spec uses_v2_manifest_paths(Table.t()) :: result(boolean())
  def uses_v2_manifest_paths(table), do: call(:uses_v2_manifest_paths, [table])
  @spec migrate_manifest_paths_v2(Table.t()) :: result(:migrated)
  def migrate_manifest_paths_v2(table), do: call(:migrate_manifest_paths_v2, [table])

  @spec ingest_open(Table.t(), map(), {pid(), term()} | nil) :: result(reference())
  def ingest_open(table, options, progress),
    do: call(:ingest_open, [table, WriteCodec.encode(options), progress])

  @spec ingest_push(reference(), [map()]) :: result(:ok)
  def ingest_push(writer, records), do: call(:ingest_push, [writer, records])
  @spec ingest_finish(reference()) :: result(write_output())
  def ingest_finish(writer), do: call(:ingest_finish, [writer])
  @spec ingest_abort(reference()) :: result(:ok)
  def ingest_abort(writer), do: call(:ingest_abort, [writer])
  @spec delete_rows_result(Table.t(), String.t()) :: result(Native.counts_output())
  def delete_rows_result(table, predicate), do: call(:delete_rows_result, [table, predicate])
  @spec count_rows(Table.t(), String.t() | nil) :: result(non_neg_integer())
  def count_rows(table, filter), do: call(:count_rows, [table, filter])

  @spec query_execute(Table.t(), LanceDB.Query.Plan.t(), Native.query_operation()) ::
          result(String.t())
  def query_execute(table, request, operation),
    do: call(:query_execute, [table, JSON.encode!(request.request), operation])

  @spec rerank_candidates(Table.t(), LanceDB.Query.Plan.t(), boolean()) ::
          result({reference(), String.t(), String.t(), String.t()})
  def rerank_candidates(table, request, analyze),
    do: call(:rerank_candidates, [table, JSON.encode!(request.request), analyze])

  @spec rerank_finish(reference(), [map()], :analyze | :stream, pos_integer()) ::
          result(non_neg_integer() | reference())
  def rerank_finish(resource, scores, operation, batch_size),
    do: call(:rerank_finish, [resource, JSON.encode!(scores), operation, batch_size])

  @spec stream_open(Table.t(), LanceDB.Query.Plan.t(), pos_integer()) :: result(reference())
  def stream_open(table, request, batch_size),
    do: call(:stream_open, [table, JSON.encode!(request.request), batch_size])

  @spec stream_close(reference()) :: result(:ok)
  def stream_close(cursor), do: call(:stream_close, [cursor])
  @spec add_columns(Table.t(), [{String.t(), String.t()}]) :: result(Native.version_output())
  def add_columns(table, expressions), do: call(:add_columns, [table, expressions])
  @spec alter_columns(Table.t(), [map()]) :: result(Native.version_output())
  def alter_columns(table, changes), do: call(:alter_columns, [table, JSON.encode!(changes)])
  @spec drop_columns(Table.t(), [String.t()]) :: result(Native.version_output())
  def drop_columns(table, columns), do: call(:drop_columns, [table, columns])
  @spec tokenize(String.t(), map()) :: result(String.t())
  def tokenize(text, analyzer), do: call(:tokenize, [text, JSON.encode!(analyzer)])

  @spec tokenize_table(Table.t(), String.t(), String.t(), String.t()) :: result(String.t())
  def tokenize_table(table, text, selector, name),
    do: call(:tokenize_table, [table, text, selector, name])

  @spec prepare_fts(String.t(), map(), pos_integer() | nil) :: result(String.t())
  def prepare_fts(text, analyzer, max_terms),
    do: call(:prepare_fts, [text, JSON.encode!(analyzer), max_terms])

  @spec prepare_fts_table(Table.t(), String.t(), String.t(), String.t(), pos_integer() | nil) ::
          result(String.t())
  def prepare_fts_table(table, text, selector, name, max_terms),
    do: call(:prepare_fts_table, [table, text, selector, name, max_terms])

  @spec prewarm_index(Table.t(), String.t()) :: result(:prewarmed)
  def prewarm_index(table, name), do: call(:prewarm_index, [table, name])
  @spec list_tags(Table.t()) :: result(String.t())
  def list_tags(table), do: call(:list_tags, [table])
  @spec tag_version(Table.t(), String.t()) :: result(pos_integer())
  def tag_version(table, name), do: call(:tag_version, [table, name])

  @spec tag_action(Table.t(), String.t(), String.t(), pos_integer() | nil) ::
          result(:created | :updated | :deleted)
  def tag_action(table, action, name, version),
    do: call(:tag_action, [table, action, name, version])

  @spec table_version(Table.t()) :: result(pos_integer())
  def table_version(table), do: call(:table_version, [table])
  @spec list_versions(Table.t()) :: result(String.t())
  def list_versions(table), do: call(:list_versions, [table])
  @spec snapshot_table(Table.t(), pos_integer() | nil, String.t() | nil) :: result(Table.t())
  def snapshot_table(table, version, tag), do: call(:snapshot_table, [table, version, tag])
  @spec restore_version(Table.t(), pos_integer()) :: result(Native.version_output())
  def restore_version(table, version), do: call(:restore_version, [table, version])

  @spec get_schema_json(Table.t()) :: result(String.t())
  def get_schema_json(table), do: call(:get_schema_json, [table])

  @spec update_rows(Table.t(), [{String.t(), {:literal | :sql, String.t()}}], String.t() | nil) ::
          result(Native.counts_output())
  def update_rows(table, changes, filter), do: call(:update_rows, [table, changes, filter])

  @spec create_branch(Table.t(), String.t(), map()) :: result(Table.t())
  def create_branch(table, name, source),
    do: call(:create_branch, [table, name, JSON.encode!(source)])

  @spec checkout_branch(Table.t(), String.t(), pos_integer() | nil) :: result(Table.t())
  def checkout_branch(table, name, version), do: call(:checkout_branch, [table, name, version])
  @spec list_branches(Table.t()) :: result(String.t())
  def list_branches(table), do: call(:list_branches, [table])
  @spec current_branch(Table.t()) :: result(String.t())
  def current_branch(table), do: call(:current_branch, [table])
  @spec delete_branch(Table.t(), String.t()) :: result(:deleted)
  def delete_branch(table, name), do: call(:delete_branch, [table, name])
  @spec table_uri(Table.t()) :: result(String.t())
  def table_uri(table), do: call(:table_uri, [table])

  @spec clone_table(Connection.t(), String.t(), String.t(), pos_integer() | nil, String.t() | nil) ::
          result(Table.t())
  def clone_table(conn, name, source_uri, version, tag),
    do: call(:clone_table, [conn, name, source_uri, version, tag])

  @spec schema_metadata(Table.t()) :: result(String.t())
  def schema_metadata(table), do: call(:schema_metadata, [table])

  @spec update_schema_metadata(Table.t(), %{String.t() => String.t() | nil}, boolean()) ::
          result(Native.version_output())
  def update_schema_metadata(table, changes, replace),
    do: call(:update_schema_metadata, [table, changes, replace])

  @spec update_field_metadata(Table.t(), String.t(), %{String.t() => String.t() | nil}, boolean()) ::
          result(Native.version_output())
  def update_field_metadata(table, path, changes, replace),
    do: call(:update_field_metadata, [table, path, changes, replace])

  @spec stream_next_batch(reference()) :: result(LanceDB.Batch.t() | nil)
  def stream_next_batch(cursor), do: call(:stream_next_batch, [cursor])
  @spec batch_rows(LanceDB.Batch.t()) :: result(String.t())
  def batch_rows(batch), do: call(:batch_rows, [batch])
  @spec batch_size(LanceDB.Batch.t()) :: non_neg_integer()
  def batch_size(batch), do: call(:batch_size, [batch])
  @spec batch_schema(LanceDB.Batch.t()) :: String.t()
  def batch_schema(batch), do: call(:batch_schema, [batch])

  @spec ingest_push_batch(reference(), LanceDB.Batch.t()) :: result(:ok)
  def ingest_push_batch(writer, batch), do: call(:ingest_push_batch, [writer, batch])

  @spec batch_to_ipc(LanceDB.Batch.t()) :: result(binary())
  def batch_to_ipc(batch), do: call(:batch_to_ipc, [batch])
  @spec batch_from_ipc(binary()) :: result([LanceDB.Batch.t()])
  def batch_from_ipc(bytes), do: call(:batch_from_ipc, [bytes])

  @spec batch_from_terms([map()], String.t(), %{String.t() => String.t()}) ::
          result(LanceDB.Batch.t())
  def batch_from_terms(records, schema_json, metadata),
    do: call(:batch_from_terms, [records, schema_json, metadata])

  defp call(operation, args) do
    result = apply(Native, operation, Enum.map(args, &unwrap/1))
    wrap(result, operation)
  end

  defp unwrap(%LanceDB.Blob{resource: resource}), do: resource
  defp unwrap(%LanceDB.Job{resource: resource}), do: resource
  defp unwrap(%Connection{resource: resource}), do: resource
  defp unwrap(%LanceDB.Batch{resource: resource}), do: resource
  defp unwrap(%Table{resource: resource}), do: resource
  defp unwrap(value), do: value

  defp wrap({:ok, resource}, :create_index_async), do: {:ok, %LanceDB.Job{resource: resource}}

  defp wrap({:ok, resource}, :connect), do: {:ok, %Connection{resource: resource}}

  defp wrap({:ok, resource}, operation)
       when operation in [
              :open_table,
              :clone_table,
              :create_branch,
              :checkout_branch,
              :snapshot_table
            ] do
    {:ok, %Table{resource: resource}}
  end

  defp wrap({:ok, {:table, resource}}, :ingest_finish), do: {:ok, %Table{resource: resource}}

  defp wrap({:ok, nil}, :stream_next_batch), do: {:ok, nil}

  defp wrap({:ok, resource}, op)
       when op in [:stream_next_batch, :batch_from_terms, :batch_infer],
       do: {:ok, %LanceDB.Batch{resource: resource}}

  defp wrap({:ok, resources}, :batch_from_ipc),
    do: {:ok, Enum.map(resources, &%LanceDB.Batch{resource: &1})}

  defp wrap({:ok, resources}, :fetch_blob_files),
    do:
      {:ok,
       Enum.map(resources, fn
         nil -> nil
         resource -> %LanceDB.Blob{resource: resource}
       end)}

  defp wrap(result, _), do: result
end
