defmodule LanceDB.Branch do
  @moduledoc "Independent branch handles and branch management."
  alias LanceDB.{Backend, Options, Result}
  @type table :: LanceDB.Table.t()
  @type result(value) :: LanceDB.result(value)
  @doc """
  Creates a writable branch and returns a new handle without changing `table`.
  `:from` defaults to `:current` (the handle's visible version), or accepts a
  version on the current branch, `{:tag, name}`, `{:branch, name}`, or
  `{:branch, name, version}`. Historical handles cannot create branches.
  The pinned SDK creates a new session with default cache budgets for the
  returned branch; it does not retain the source session's configured budgets.
  """
  @spec create(table(), String.t(), keyword()) :: result(table())
  def create(table, name, opts \\ []) do
    LanceDB.Execution.run(:create_branch, fn ->
      opts = Keyword.validate!(opts, [:from])
      name = Options.string!(name, "branch name")
      source = source!(Keyword.get(opts, :from, :current))
      Backend.create_branch(table, name, source)
    end)
  end

  @doc """
  Opens a separate handle on a branch, leaving `table` unchanged. `:version`
  pins the new handle read-only; omitted, it opens a live handle at the branch's
  latest version. Later changes follow the handle's read-consistency policy.
  Use `"main"` to return a handle on the main branch. Checkout preserves the
  source handle's native session and cache budgets.
  """
  @spec open(table(), String.t(), keyword()) :: result(table())
  def open(table, name, opts \\ []) do
    LanceDB.Execution.run(:checkout_branch, fn ->
      opts = Keyword.validate!(opts, [:version])
      name = Options.string!(name, "branch name")
      version = if Keyword.has_key?(opts, :version), do: Options.version!(opts[:version])
      Backend.checkout_branch(table, name, version)
    end)
  end

  @doc "Lists branches by name with parent branch/version, creation timestamp, manifest size and metadata."
  @spec list(table()) :: result(map())
  def list(table) do
    LanceDB.Execution.run(:list_branches, fn -> Result.decode(Backend.list_branches(table)) end)
  end

  @doc "Returns the handle's branch name, using `\"main\"` for the default branch."
  @spec current(table()) :: result(String.t())
  def current(table) do
    LanceDB.Execution.run(:current_branch, fn -> Backend.current_branch(table) end)
  end

  @doc "Deletes a branch. Historical handles cannot delete branches; SDK reference restrictions apply."
  @spec delete(table(), String.t()) :: result(:deleted)
  def delete(table, name) do
    LanceDB.Execution.run(:delete_branch, fn ->
      Backend.delete_branch(table, Options.string!(name, "branch name"))
    end)
  end

  defp source!(:current), do: %{kind: :current}

  defp source!(version) when is_integer(version),
    do: %{kind: :version, version: Options.version!(version)}

  defp source!({:tag, name}), do: %{kind: :tag, name: Options.string!(name, "tag name")}

  defp source!({:branch, name}),
    do: %{kind: :branch, name: Options.string!(name, "branch name"), version: nil}

  defp source!({:branch, name, version}),
    do: %{
      kind: :branch,
      name: Options.string!(name, "branch name"),
      version: Options.version!(version)
    }

  defp source!(_),
    do:
      raise(
        ArgumentError,
        "from must be :current, a version, {:tag, name}, or {:branch, name[, version]}"
      )
end
