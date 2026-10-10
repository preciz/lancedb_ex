defmodule LanceDB.Backend do
  alias LanceDB.Backend.WriteCodec
  @moduledoc false
  alias LanceDB.{Connection, Native, Table}

  def blob_columns(table), do: call(:blob_columns, [table])
  def fetch_blobs(table, column, row_ids), do: call(:fetch_blobs, [table, column, row_ids])

  def fetch_blob_ranges(table, column, requests),
    do: call(:fetch_blob_ranges, [table, column, requests])

  def fetch_blob_files(table, column, row_ids),
    do: call(:fetch_blob_files, [table, column, row_ids])

  def blob_read(blob, length), do: call(:blob_read, [blob, length])
  def blob_read_ranges(blob, ranges), do: call(:blob_read_ranges, [blob, ranges])
  def blob_seek(blob, offset), do: call(:blob_seek, [blob, offset])
  def blob_tell(blob), do: call(:blob_tell, [blob])
  def blob_close(blob), do: call(:blob_close, [blob])
  def blob_is_closed(blob), do: call(:blob_is_closed, [blob])
  def blob_size(blob), do: call(:blob_size, [blob])
  def blob_info(blob), do: call(:blob_info, [blob])

  def create_index_async(table, column, options),
    do: call(:create_index_async, [table, column, JSON.encode!(options.options)])

  def job_status(job), do: call(:job_status, [job])
  def job_wait(job, timeout), do: call(:job_wait, [job, timeout])
  def job_cancel(job), do: call(:job_cancel, [job])

  def create_stream_open(conn, name, schema, metadata, options),
    do: call(:create_stream_open, [conn, name, schema, metadata, WriteCodec.encode(options)])

  def merge_stream_open(table, keys, options),
    do: call(:merge_stream_open, [table, keys, JSON.encode!(options)])

  def columns_stream_open(table, schema, version),
    do: call(:columns_stream_open, [table, schema, version])

  def execution_limits(reads, writes), do: call(:execution_limits, [reads, writes])
  def metrics_enable, do: call(:metrics_enable, [])
  def metrics_catalog, do: call(:metrics_catalog, [])
  def metrics_snapshot, do: call(:metrics_snapshot, [])

  def connect(uri, storage_options, read_consistency_interval_ms),
    do: call(:connect, [uri, storage_options, read_consistency_interval_ms])

  def table_names(conn, start_after, limit), do: call(:table_names, [conn, start_after, limit])
  def open_table(conn, name, options), do: call(:open_table, [conn, name, JSON.encode!(options)])
  def drop_table(conn, name), do: call(:drop_table, [conn, name])

  def batch_infer(rows), do: call(:batch_infer, [rows])
  def schema_validate(schema), do: call(:schema_validate, [schema])

  def create_index(table, column, options),
    do: call(:create_index, [table, column, JSON.encode!(options.options)])

  def list_indices(table), do: call(:list_indices, [table])
  def table_stats(table), do: call(:table_stats, [table])
  def index_stats(table, name), do: call(:index_stats, [table, name])
  def index_readiness(table, names, timeout), do: call(:index_readiness, [table, names, timeout])
  def drop_index(table, name), do: call(:drop_index, [table, name])

  def optimize_indices(table, options),
    do: call(:optimize_indices, [table, JSON.encode!(options)])

  def compact_files(table, options), do: call(:compact_files, [table, JSON.encode!(options)])

  def cleanup_old_versions(table, options),
    do: call(:cleanup_old_versions, [table, JSON.encode!(options)])

  def uses_v2_manifest_paths(table), do: call(:uses_v2_manifest_paths, [table])
  def migrate_manifest_paths_v2(table), do: call(:migrate_manifest_paths_v2, [table])

  def ingest_open(table, options, progress),
    do: call(:ingest_open, [table, WriteCodec.encode(options), progress])

  def ingest_push(writer, records), do: call(:ingest_push, [writer, records])
  def ingest_finish(writer), do: call(:ingest_finish, [writer])
  def ingest_abort(writer), do: call(:ingest_abort, [writer])
  def delete_rows_result(table, predicate), do: call(:delete_rows_result, [table, predicate])
  def count_rows(table, filter), do: call(:count_rows, [table, filter])

  def query_execute(table, request, operation),
    do: call(:query_execute, [table, JSON.encode!(request.request), operation])

  def rerank_candidates(table, request, analyze),
    do: call(:rerank_candidates, [table, JSON.encode!(request.request), analyze])

  def rerank_finish(resource, scores, operation, batch_size),
    do: call(:rerank_finish, [resource, JSON.encode!(scores), operation, batch_size])

  def stream_open(table, request, batch_size),
    do: call(:stream_open, [table, JSON.encode!(request.request), batch_size])

  def stream_close(cursor), do: call(:stream_close, [cursor])
  def add_columns(table, expressions), do: call(:add_columns, [table, expressions])
  def alter_columns(table, changes), do: call(:alter_columns, [table, JSON.encode!(changes)])
  def drop_columns(table, columns), do: call(:drop_columns, [table, columns])
  def tokenize(text, analyzer), do: call(:tokenize, [text, JSON.encode!(analyzer)])

  def tokenize_table(table, text, selector, name),
    do: call(:tokenize_table, [table, text, selector, name])

  def prewarm_index(table, name), do: call(:prewarm_index, [table, name])
  def list_tags(table), do: call(:list_tags, [table])
  def tag_version(table, name), do: call(:tag_version, [table, name])

  def tag_action(table, action, name, version),
    do: call(:tag_action, [table, action, name, version])

  def table_version(table), do: call(:table_version, [table])
  def list_versions(table), do: call(:list_versions, [table])
  def snapshot_table(table, version, tag), do: call(:snapshot_table, [table, version, tag])
  def restore_version(table, version), do: call(:restore_version, [table, version])

  def get_schema_json(table), do: call(:get_schema_json, [table])

  def update_rows(table, changes, filter), do: call(:update_rows, [table, changes, filter])

  def create_branch(table, name, source),
    do: call(:create_branch, [table, name, JSON.encode!(source)])

  def checkout_branch(table, name, version), do: call(:checkout_branch, [table, name, version])
  def list_branches(table), do: call(:list_branches, [table])
  def current_branch(table), do: call(:current_branch, [table])
  def delete_branch(table, name), do: call(:delete_branch, [table, name])
  def table_uri(table), do: call(:table_uri, [table])

  def clone_table(conn, name, source_uri, version, tag),
    do: call(:clone_table, [conn, name, source_uri, version, tag])

  def schema_metadata(table), do: call(:schema_metadata, [table])

  def update_schema_metadata(table, changes, replace),
    do: call(:update_schema_metadata, [table, changes, replace])

  def update_field_metadata(table, path, changes, replace),
    do: call(:update_field_metadata, [table, path, changes, replace])

  def stream_next_batch(cursor), do: call(:stream_next_batch, [cursor])
  def batch_rows(batch), do: call(:batch_rows, [batch])
  def batch_size(batch), do: call(:batch_size, [batch])
  def batch_schema(batch), do: call(:batch_schema, [batch])

  def ingest_push_batch(writer, batch), do: call(:ingest_push_batch, [writer, batch])

  def batch_to_ipc(batch), do: call(:batch_to_ipc, [batch])
  def batch_from_ipc(bytes), do: call(:batch_from_ipc, [bytes])

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
