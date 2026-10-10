defmodule LanceDB.BranchTest do
  use LanceDB.DataCase, async: true

  setup %{path: path} do
    {:ok, conn} = LanceDB.connect(path)
    {:ok, table} = LanceDB.Connection.create_table(conn, "docs", [%{id: 1, text: "elixir"}])
    %{conn: conn, table: table, path: path}
  end

  test "branches have isolated handles and durable independent writes", %{
    conn: conn,
    table: table
  } do
    assert {:ok, "main"} = LanceDB.Branch.current(table)
    assert {:ok, parent} = LanceDB.History.version(table)
    assert {:ok, branch} = LanceDB.Branch.create(table, "experiment")
    assert {:ok, "experiment"} = LanceDB.Branch.current(branch)
    assert {:ok, "main"} = LanceDB.Branch.current(table)
    assert {:ok, %{inserted: 1}} = LanceDB.Table.add(branch, [%{id: 2, text: "branch"}])
    assert {:ok, 1} = LanceDB.Table.count_rows(table)
    assert {:ok, 2} = LanceDB.Table.count_rows(branch)
    assert {:ok, %{"experiment" => info}} = LanceDB.Branch.list(table)
    assert info["parent_version"] == parent
    assert is_integer(info["created_at"])
    assert {:ok, reopened} = LanceDB.Connection.open_table(conn, "docs", branch: "experiment")
    assert {:ok, 2} = LanceDB.Table.count_rows(reopened)
    assert {:ok, main} = LanceDB.Branch.open(branch, "main")
    assert {:ok, "main"} = LanceDB.Branch.current(main)
    assert {:ok, 1} = LanceDB.Table.count_rows(main)
    assert {:ok, "experiment"} = LanceDB.Branch.current(branch)
  end

  test "branch versions are read-only until refreshed on the same branch", %{table: table} do
    {:ok, branch} = LanceDB.Branch.create(table, "work")
    {:ok, initial} = LanceDB.History.version(branch)
    {:ok, %{inserted: 1}} = LanceDB.Table.add(branch, [%{id: 2, text: "later"}])
    assert {:ok, snapshot} = LanceDB.Branch.open(table, "work", version: initial)
    assert {:ok, 1} = LanceDB.Table.count_rows(snapshot)
    assert {:error, _} = LanceDB.Table.add(snapshot, [%{id: 3, text: "blocked"}])
    assert {:error, _} = LanceDB.Branch.create(snapshot, "blocked")
    assert {:error, _} = LanceDB.Branch.delete(snapshot, "work")
    assert {:ok, snapshot} = LanceDB.History.latest(snapshot)
    assert {:ok, "work"} = LanceDB.Branch.current(snapshot)
    assert {:ok, 2} = LanceDB.Table.count_rows(snapshot)
    assert {:ok, %{updated: 2}} = LanceDB.Table.update(snapshot, %{text: "changed"})
    assert {:ok, [%{"text" => "elixir"}]} = LanceDB.Query.query(table)
  end

  test "branch sources support versions, tags, and branch references", %{table: table} do
    {:ok, initial} = LanceDB.History.version(table)
    {:ok, :created} = LanceDB.History.create_tag(table, "saved", initial)
    {:ok, %{inserted: 1}} = LanceDB.Table.add(table, [%{id: 2, text: "new"}])

    for {name, source} <- [{"old", initial}, {"tagged", {:tag, "saved"}}] do
      assert {:ok, branch} = LanceDB.Branch.create(table, name, from: source)
      assert {:ok, 1} = LanceDB.Table.count_rows(branch)
    end

    {:ok, old} = LanceDB.Branch.open(table, "old")
    {:ok, old_version} = LanceDB.History.version(old)

    for {name, source} <- [
          {"child", {:branch, "old"}},
          {"child_version", {:branch, "old", old_version}}
        ] do
      assert {:ok, branch} = LanceDB.Branch.create(table, name, from: source)
      assert {:ok, 1} = LanceDB.Table.count_rows(branch)
    end

    assert {:ok, :deleted} = LanceDB.Branch.delete(table, "child_version")
    assert {:error, _} = LanceDB.Branch.open(table, "child_version")
    assert {:error, _} = LanceDB.Branch.create(table, "old")
    assert {:error, _} = LanceDB.Branch.delete(table, "missing")
    assert {:error, _} = LanceDB.Branch.delete(table, "main")
  end

  test "shallow clones preserve snapshots and indexes with independent writes", %{
    conn: conn,
    table: table
  } do
    {:ok, :created} = LanceDB.Index.create(table, :text, type: :fts)
    {:ok, initial} = LanceDB.History.version(table)
    {:ok, :created} = LanceDB.History.create_tag(table, "saved", initial)
    {:ok, source} = LanceDB.Table.uri(table)
    {:ok, %{inserted: 1}} = LanceDB.Table.add(table, [%{id: 2, text: "later"}])

    for {name, opts, count} <- [
          {"copy", [], 2},
          {"old", [source_version: initial], 1},
          {"tagged", [source_tag: "saved"], 1}
        ] do
      assert {:ok, copy} = LanceDB.Connection.clone_table(conn, name, source, opts)
      assert {:ok, ^count} = LanceDB.Table.count_rows(copy)
      assert {:ok, [%{"id" => 1}]} = LanceDB.Query.full_text_search(copy, "elixir")
      assert {:ok, %{inserted: 1}} = LanceDB.Table.add(copy, [%{id: 3, text: "cloned"}])
      assert {:ok, reopened} = LanceDB.Connection.open_table(conn, name)
      assert {:ok, actual} = LanceDB.Table.count_rows(reopened)
      assert actual == count + 1
    end

    assert {:ok, 2} = LanceDB.Table.count_rows(table)
    assert {:error, _} = LanceDB.Connection.clone_table(conn, "copy", source)

    assert {:error, _} =
             LanceDB.Connection.clone_table(conn, "bad", source, source_version: 999_999)
  end

  test "invalid branch and clone arguments fail before native calls" do
    for name <- [nil, :branch, "", "  ", <<255>>] do
      assert_raise ArgumentError, fn -> LanceDB.Branch.create(:unused, name) end
      assert_raise ArgumentError, fn -> LanceDB.Branch.open(:unused, name) end
      assert_raise ArgumentError, fn -> LanceDB.Branch.delete(:unused, name) end
    end

    for source <- [nil, 0, -1, 1.5, {:tag, ""}, {:branch, ""}, {:branch, "b", 0}, :bad] do
      assert_raise ArgumentError, fn ->
        LanceDB.Branch.create(:unused, "new", from: source)
      end
    end

    assert_raise ArgumentError, fn -> LanceDB.Branch.open(:unused, "b", version: nil) end

    for opts <- [
          [source_tag: "t", source_version: 1],
          [source_tag: ""],
          [source_version: 0],
          [shallow: false]
        ] do
      assert_raise ArgumentError, fn ->
        LanceDB.Connection.clone_table(:unused, "copy", "source", opts)
      end
    end

    assert_raise ArgumentError, fn -> LanceDB.Connection.clone_table(:unused, "copy", "") end
  end
end
