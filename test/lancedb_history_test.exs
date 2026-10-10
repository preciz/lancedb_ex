defmodule LanceDB.HistoryTest do
  use LanceDB.DataCase, async: true

  setup %{path: path} do
    {:ok, conn} = LanceDB.connect(path)
    {:ok, table} = LanceDB.Connection.create_table(conn, "docs", [%{id: 1, text: "original"}])
    %{conn: conn, table: table, path: path}
  end

  test("history, checkout isolation, write protection, and refresh", %{conn: conn, table: table}) do
    assert {:ok, first} = LanceDB.History.version(table)
    assert {:ok, %{inserted: 1}} = LanceDB.Table.add(table, [%{id: 2, text: "later"}])
    assert {:ok, second} = LanceDB.History.version(table)
    assert second > first
    assert {:ok, history} = LanceDB.History.list_versions(table)
    assert Enum.sort(Enum.map(history, & &1.version)) == [first, second]

    for item <- history do
      assert {:ok, _, _} = DateTime.from_iso8601(item.timestamp)
      assert is_map(item.metadata)
    end

    {:ok, other} = LanceDB.Connection.open_table(conn, "docs")
    assert {:ok, table} = LanceDB.History.snapshot(table, first)
    assert {:ok, ^first} = LanceDB.History.version(table)
    assert {:ok, 1} = LanceDB.Table.count_rows(table)
    assert {:ok, 2} = LanceDB.Table.count_rows(other)
    assert {:error, _} = LanceDB.Table.add(table, [%{id: 3, text: "blocked"}])
    assert {:error, _} = LanceDB.Table.update(table, %{text: "blocked"})

    assert {:error, _} =
             LanceDB.Table.merge(table, [%{id: 1, text: "blocked"}],
               when_matched_update_all: true
             )

    assert {:error, _} = LanceDB.Table.upsert(table, [%{id: 1, text: "blocked"}])
    assert {:error, _} = LanceDB.SchemaEvolution.add_columns(table, copy: "text")
    assert {:error, _} = LanceDB.Maintenance.cleanup_old_versions(table)
    assert {:ok, table} = LanceDB.History.latest(table)
    assert {:ok, ^second} = LanceDB.History.version(table)
    assert {:ok, %{inserted: 1}} = LanceDB.Table.add(table, [%{id: 3, text: "allowed"}])
    assert {:ok, 3} = LanceDB.Table.count_rows(table)
  end

  test("restore promotes old data, schema and indexes as a new durable version", %{
    table: table,
    path: path
  }) do
    assert {:ok, :created} = LanceDB.Index.create(table, :text, type: :fts, name: "words")
    assert {:ok, snapshot} = LanceDB.History.version(table)
    assert {:ok, %{updated: 1}} = LanceDB.Table.update(table, %{text: "changed"})
    assert {:ok, %{version: _}} = LanceDB.SchemaEvolution.add_columns(table, extra: "id + 1")
    assert {:ok, :dropped} = LanceDB.Index.drop(table, "words")
    assert {:ok, before_restore} = LanceDB.History.version(table)
    assert {:ok, table} = LanceDB.History.snapshot(table, snapshot)
    {:ok, table} = LanceDB.History.latest(table)
    assert {:ok, %LanceDB.WriteResult{}} = LanceDB.History.restore(table, snapshot)
    assert {:ok, restored} = LanceDB.History.version(table)
    assert restored > before_restore
    assert {:ok, [%{"id" => 1, "text" => "original"} = row]} = LanceDB.Query.query(table)
    refute Map.has_key?(row, "extra")
    assert {:ok, [%LanceDB.Index.Description{name: "words"}]} = LanceDB.Index.list(table)
    assert {:ok, [%{"id" => 1}]} = LanceDB.Query.full_text_search(table, "original")
    {:ok, conn} = LanceDB.connect(path)
    {:ok, reopened} = LanceDB.Connection.open_table(conn, "docs")
    assert {:ok, ^restored} = LanceDB.History.version(reopened)
    assert {:ok, [^row]} = LanceDB.Query.query(reopened)
    assert {:ok, table} = LanceDB.History.snapshot(table, before_restore)
    assert {:ok, [%{"text" => "changed", "extra" => 2}]} = LanceDB.Query.query(table)
    assert {:ok, table} = LanceDB.History.latest(table)
    assert {:ok, %{inserted: 1}} = LanceDB.Table.add(table, [%{id: 2, text: "writable"}])
  end

  test("invalid and pruned versions fail without changing the visible snapshot", %{table: table}) do
    assert {:error, _} = LanceDB.History.restore(table, 999_999)
    assert {:ok, first} = LanceDB.History.version(table)
    assert {:error, _} = LanceDB.History.snapshot(table, 999_999)
    assert {:ok, ^first} = LanceDB.History.version(table)
    assert {:ok, %{inserted: 1}} = LanceDB.Table.add(table, [%{id: 2, text: "second"}])
    assert {:ok, latest} = LanceDB.History.version(table)
    assert {:ok, _} = LanceDB.Maintenance.cleanup_old_versions(table, older_than_seconds: 0)
    assert {:error, _} = LanceDB.History.snapshot(table, first)
    assert {:ok, ^latest} = LanceDB.History.version(table)

    assert {:ok, [%LanceDB.History.Version{version: ^latest}]} =
             LanceDB.History.list_versions(table)

    for version <- [0, -1, 1.0, nil, "1", 18_446_744_073_709_551_616] do
      assert_raise ArgumentError, fn -> LanceDB.History.snapshot(table, version) end
    end
  end
end
