defmodule LanceDB.BranchPropertyTest do
  use ExUnit.Case, async: true
  use ExUnitProperties
  import LanceDB.TestSupport, only: [with_database: 1]

  property "branch and clone writes follow independent models while pinned snapshots stay unchanged" do
    row = fixed_map(%{id: integer(0..4), text: string(:utf8, max_length: 15)})

    command =
      one_of([
        tuple({constant(:upsert), row}),
        tuple({constant(:update), row}),
        tuple({constant(:delete), integer(0..4)})
      ])

    operation =
      tuple(
        {member_of([:main, :branch, :copy]), command, member_of([:existing, :reopen, :refresh])}
      )

    check all(
            texts <- list_of(string(:utf8, max_length: 15), min_length: 1, max_length: 5),
            source <- member_of([:current, :version, :tag, :branch_version]),
            operations <- list_of(operation, min_length: 1, max_length: 8),
            max_runs: 35
          ) do
      with_database(fn conn, path ->
        rows = Enum.with_index(texts, fn text, id -> %{id: id, text: text} end)
        {handles, models} = fork_tables(conn, rows, source)

        {handles, models, snapshots} =
          Enum.reduce(operations, {handles, models, []}, fn operation, state ->
            apply_operation(conn, operation, state)
          end)

        {:ok, fresh} = LanceDB.connect(path)
        assert_tables(fresh, handles, models)

        for {snapshot, version, expected, branch} <- snapshots do
          assert {:ok, ^version} = LanceDB.History.version(snapshot)
          assert {:ok, ^branch} = LanceDB.Branch.current(snapshot)
          assert_rows(snapshot, expected)
        end
      end)
    end
  end

  defp fork_tables(conn, rows, source) do
    {:ok, main} = LanceDB.Connection.create_table(conn, "docs", rows)
    {:ok, version} = LanceDB.History.version(main)
    {:ok, :created} = LanceDB.History.create_tag(main, "saved", version)
    {:ok, uri} = LanceDB.Table.uri(main)
    {:ok, %{inserted: 1}} = LanceDB.Table.add(main, [%{id: 99, text: "after saved version"}])

    {from, clone_opts} =
      case source do
        :current -> {:current, []}
        :version -> {version, [source_version: version]}
        :tag -> {{:tag, "saved"}, [source_tag: "saved"]}
        :branch_version -> {{:branch, "main", version}, [source_version: version]}
      end

    {:ok, branch} = LanceDB.Branch.create(main, "work", from: from)
    {:ok, copy} = LanceDB.Connection.clone_table(conn, "copy", uri, clone_opts)
    original = Map.new(rows, &{&1.id, &1.text})
    latest = Map.put(original, 99, "after saved version")
    forked = if source == :current, do: latest, else: original
    handles = %{main: main, branch: branch, copy: copy}
    models = %{main: latest, branch: forked, copy: forked}
    assert_tables(conn, handles, models)
    {handles, models}
  end

  defp apply_operation(conn, {target, command, mode}, {handles, models, snapshots}) do
    table = if mode == :reopen, do: open(conn, target), else: Map.fetch!(handles, target)
    {:ok, version} = LanceDB.History.version(table)
    {:ok, branch} = LanceDB.Branch.current(table)
    {:ok, snapshot} = LanceDB.Branch.open(table, branch, version: version)
    expected = Map.fetch!(models, target)
    assert_rows(snapshot, expected)
    assert {:error, _} = write(snapshot, command)
    assert {:ok, ^version} = LanceDB.History.version(snapshot)
    assert_rows(snapshot, expected)

    table =
      if mode == :refresh do
        {:ok, refreshed} = LanceDB.Branch.open(table, branch, version: version)
        assert {:ok, refreshed} = LanceDB.History.latest(refreshed)
        assert {:ok, ^branch} = LanceDB.Branch.current(refreshed)
        refreshed
      else
        table
      end

    assert {:ok, _} = write(table, command)
    models = Map.update!(models, target, &model_write(&1, command))
    handles = Map.put(handles, target, table)
    assert_tables(conn, handles, models)
    assert_rows(snapshot, expected)
    {handles, models, [{snapshot, version, expected, branch} | snapshots]}
  end

  defp write(table, {:upsert, row}), do: LanceDB.Table.upsert(table, [row])

  defp write(table, {:update, row}),
    do: LanceDB.Table.update(table, %{text: row.text}, filter: "id = #{row.id}")

  defp write(table, {:delete, id}), do: LanceDB.Table.delete(table, "id = #{id}")

  defp model_write(model, {:upsert, row}), do: Map.put(model, row.id, row.text)
  defp model_write(model, {:delete, id}), do: Map.delete(model, id)

  defp model_write(model, {:update, row}) do
    if Map.has_key?(model, row.id), do: Map.put(model, row.id, row.text), else: model
  end

  defp open(conn, target) do
    {name, branch} = location(target)
    {:ok, table} = LanceDB.Connection.open_table(conn, name, branch: branch)
    table
  end

  defp location(:main), do: {"docs", "main"}
  defp location(:branch), do: {"docs", "work"}
  defp location(:copy), do: {"copy", "main"}

  defp assert_tables(conn, handles, models) do
    for {target, table} <- handles do
      {_, branch} = location(target)
      assert {:ok, ^branch} = LanceDB.Branch.current(table)
      assert_rows(table, Map.fetch!(models, target))
      assert_rows(open(conn, target), Map.fetch!(models, target))
    end
  end

  defp assert_rows(table, model) do
    expected =
      model |> Enum.sort() |> Enum.map(fn {id, text} -> %{"id" => id, "text" => text} end)

    assert {:ok, ^expected} = LanceDB.Query.query(table, order_by: [asc: :id])
  end
end
