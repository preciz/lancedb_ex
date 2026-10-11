defmodule LanceDB.History do
  @moduledoc "Version history, snapshots, restore and version tags."
  alias LanceDB.{Backend, Options, Result}
  alias LanceDB.Backend.ResultCodec
  @type table :: LanceDB.Table.t()
  @type t :: LanceDB.Table.t()
  @type result(value) :: LanceDB.result(value)
  @doc "Returns the version currently visible through this table handle."
  @spec version(table()) :: result(pos_integer())
  def version(table) do
    LanceDB.Execution.run(:version, fn -> Backend.table_version(table) end)
  end

  @doc "Lists retained History.Version structs with version, UTC timestamp and string-keyed metadata."
  @spec list_versions(table()) :: result([LanceDB.History.Version.t()])
  def list_versions(table) do
    LanceDB.Execution.run(:list_versions, fn ->
      ResultCodec.decode(Backend.list_versions(table), LanceDB.History.Version, :many)
    end)
  end

  @doc """
  Returns an independent read-only handle pinned to the given version.
  The source handle and other processes using it are unchanged.
  The snapshot retains the source session and shares its caches and counters.
  """
  @spec snapshot(table(), pos_integer()) :: result(table())
  def snapshot(table, version) do
    LanceDB.Execution.run(:checkout, fn ->
      Backend.snapshot_table(table, Options.version!(version), nil)
    end)
  end

  @doc """
  Lists tags as a map keyed by tag name. Values contain string keys
  "version", "manifest_size", and "branch" (nil for the main branch).
  Tags are durable names for retained versions, not moving aliases for latest.
  """
  @spec list_tags(table()) :: result(map())
  def list_tags(table) do
    LanceDB.Execution.run(:list_tags, fn -> Result.decode(Backend.list_tags(table)) end)
  end

  @doc "Resolves a tag to its version number. Missing tags return an error."
  @spec tag_version(table(), String.t()) :: result(pos_integer())
  def tag_version(table, name) do
    LanceDB.Execution.run(:tag_version, fn ->
      Backend.tag_version(table, Options.string!(name, "tag name"))
    end)
  end

  @doc """
  Creates a tag pointing to an existing version. Duplicate names return an error.
  Tagged versions are protected: cleanup fails if it would prune one.
  Tag mutations require a handle that is not checked out.
  """
  @spec create_tag(table(), String.t(), pos_integer()) :: result(:created)
  def create_tag(table, name, version) do
    LanceDB.Execution.run(:create_tag, fn ->
      name = Options.string!(name, "tag name")
      Backend.tag_action(table, "create", name, Options.version!(version))
    end)
  end

  @doc "Moves an existing tag to an existing version, returning an error if either is missing."
  @spec update_tag(table(), String.t(), pos_integer()) :: result(:updated)
  def update_tag(table, name, version) do
    LanceDB.Execution.run(:update_tag, fn ->
      name = Options.string!(name, "tag name")
      Backend.tag_action(table, "update", name, Options.version!(version))
    end)
  end

  @doc "Deletes a tag without deleting its version. Missing tags return an error."
  @spec delete_tag(table(), String.t()) :: result(:deleted)
  def delete_tag(table, name) do
    LanceDB.Execution.run(:delete_tag, fn ->
      Backend.tag_action(table, "delete", Options.string!(name, "tag name"), nil)
    end)
  end

  @doc """
  Resolves a tag and returns an independent read-only handle. Moving or deleting
  the tag later does not change that snapshot. It retains the source session.
  """
  @spec snapshot_tag(table(), String.t()) :: result(table())
  def snapshot_tag(table, name) do
    LanceDB.Execution.run(:checkout_tag, fn ->
      Backend.snapshot_table(table, nil, Options.string!(name, "tag name"))
    end)
  end

  @doc """
  Returns a new live handle on the same branch, observing the latest version.
  The source handle remains unchanged, including when it is a pinned snapshot.
  The new handle retains the source session and read-consistency configuration.
  """
  @spec latest(table()) :: result(table())
  def latest(table) do
    LanceDB.Execution.run(:checkout_latest, fn -> Backend.snapshot_table(table, nil, nil) end)
  end

  @doc """
  Restores an explicit historical version onto a live table handle as a new commit.
  Returns a WriteResult containing the committed version. The target must be live;
  existing snapshot handles remain pinned. Other live handles follow their read-consistency policy.
  """
  @spec restore(table(), pos_integer()) :: result(LanceDB.WriteResult.t())
  def restore(table, version) do
    LanceDB.Execution.write(
      :restore,
      fn -> Backend.restore_version(table, Options.version!(version)) end,
      &LanceDB.WriteResult.version/2
    )
  end
end
