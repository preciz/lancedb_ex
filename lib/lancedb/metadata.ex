defmodule LanceDB.Metadata do
  alias LanceDB.{Backend, Options, Result}
  @moduledoc "Schema and field metadata operations."
  @type table :: LanceDB.Table.t()
  @type result(value) :: LanceDB.result(value)
  @doc "Returns schema metadata as a map with string keys. An empty map returns `%{}`."
  @spec schema(table()) :: result(map())
  def schema(table) do
    LanceDB.Execution.run(:schema_metadata, fn ->
      Result.decode(Backend.schema_metadata(table))
    end)
  end

  @doc """
  Merges schema metadata and returns a WriteResult with the new version.

  Keys can be strings or atoms. Values must be UTF-8 strings. Use `nil` to delete
  a key. Use `replace: true` to replace the complete map. An empty replacement
  map clears all schema metadata. Historical handles reject updates.
  """
  @spec update_schema(table(), map(), keyword()) :: result(LanceDB.WriteResult.t())
  def update_schema(table, changes, opts \\ []) do
    LanceDB.Execution.write(
      :update_schema_metadata,
      fn ->
        replace = replace!(opts)
        Backend.update_schema_metadata(table, Options.metadata!(changes, true), replace)
      end,
      &LanceDB.WriteResult.version/2
    )
  end

  @doc """
  Updates field metadata and returns a WriteResult with the new version.

  The field path can select a nested field. Merge and deletion use the same
  options as `update_schema/3`. Use `replace: true` to replace user metadata.
  An empty replacement map clears user metadata.

  Replacement keeps extension markers, primary-key markers, computed declarations,
  and refresh tracking metadata.
  An attempt to change these engine-owned markers returns an unsupported error.
  Use `LanceDB.Table.set_unenforced_primary_key/2` to designate a primary key.
  Nested metadata edits on computed containers or their inputs are unsupported
  by the pinned SDK; edit top-level user metadata instead.
  `LanceDB.Table.schema/1` returns the metadata for each field.
  """
  @spec update_field(table(), String.t() | atom(), map(), keyword()) ::
          result(LanceDB.WriteResult.t())
  def update_field(table, path, changes, opts \\ []) do
    LanceDB.Execution.write(
      :update_field_metadata,
      fn ->
        path = Options.column_name!(path)
        replace = replace!(opts)
        Backend.update_field_metadata(table, path, Options.metadata!(changes, true), replace)
      end,
      &LanceDB.WriteResult.version/2
    )
  end

  defp replace!(opts) do
    opts = Keyword.validate!(opts, [:replace])
    replace = Keyword.get(opts, :replace, false)
    if not is_boolean(replace), do: raise(ArgumentError, "replace must be a boolean")
    replace
  end
end
