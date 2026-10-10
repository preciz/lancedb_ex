defmodule LanceDB.TagsTest do
  use LanceDB.DataCase, async: true

  setup %{path: path} do
    {:ok, conn} = LanceDB.connect(path)
    {:ok, table} = LanceDB.Connection.create_table(conn, "docs", [%{id: 1}])
    %{conn: conn, table: table, path: path}
  end

  test "durable tag lifecycle, checkout isolation and restore", %{table: table, path: path} do
    assert {:ok, %{}} = LanceDB.History.list_tags(table)
    {:ok, first} = LanceDB.History.version(table)
    assert {:ok, :created} = LanceDB.History.create_tag(table, "before_import", first)
    assert {:ok, %{inserted: 1}} = LanceDB.Table.add(table, [%{id: 2}])
    {:ok, second} = LanceDB.History.version(table)
    {:ok, conn} = LanceDB.connect(path)
    {:ok, other} = LanceDB.Connection.open_table(conn, "docs")

    assert {:ok, %{"before_import" => %{"version" => ^first, "manifest_size" => size}}} =
             LanceDB.History.list_tags(other)

    assert size > 0
    assert {:ok, ^first} = LanceDB.History.tag_version(other, "before_import")
    assert {:ok, table} = LanceDB.History.snapshot_tag(table, "before_import")
    assert {:ok, 1} = LanceDB.Table.count_rows(table)
    assert {:ok, 2} = LanceDB.Table.count_rows(other)
    assert {:error, _} = LanceDB.Table.add(table, [%{id: 3}])
    assert {:error, _} = LanceDB.History.create_tag(table, "blocked", first)
    assert {:error, _} = LanceDB.History.update_tag(table, "before_import", second)
    assert {:error, _} = LanceDB.History.delete_tag(table, "before_import")
    assert {:ok, :updated} = LanceDB.History.update_tag(other, "before_import", second)
    assert {:ok, ^second} = LanceDB.History.tag_version(other, "before_import")
    assert {:ok, ^first} = LanceDB.History.version(table)
    assert {:error, _} = LanceDB.History.snapshot_tag(table, "missing")
    assert {:ok, ^first} = LanceDB.History.version(table)
    {:ok, table} = LanceDB.History.latest(table)
    assert {:ok, %LanceDB.WriteResult{}} = LanceDB.History.restore(table, first)
    assert {:ok, restored} = LanceDB.History.version(table)
    assert restored > second
    assert {:ok, 1} = LanceDB.Table.count_rows(table)
    assert {:ok, :deleted} = LanceDB.History.delete_tag(table, "before_import")
    assert {:ok, tags} = LanceDB.History.list_tags(table)
    assert tags == %{}
    assert {:ok, table} = LanceDB.History.snapshot(table, second)
    assert {:ok, 2} = LanceDB.Table.count_rows(table)
    assert {:ok, %LanceDB.Table{}} = LanceDB.History.latest(table)
  end

  test "missing and duplicate tags and missing versions fail without changing tags", %{
    table: table
  } do
    {:ok, version} = LanceDB.History.version(table)
    assert {:error, _} = LanceDB.History.tag_version(table, "missing")
    assert {:error, _} = LanceDB.History.delete_tag(table, "missing")
    assert {:error, _} = LanceDB.History.update_tag(table, "missing", version)
    assert {:error, _} = LanceDB.History.create_tag(table, "bad", 999_999)
    assert {:ok, :created} = LanceDB.History.create_tag(table, "saved", version)
    assert {:error, _} = LanceDB.History.create_tag(table, "saved", version)
    assert {:error, _} = LanceDB.History.update_tag(table, "saved", 999_999)
    assert {:ok, ^version} = LanceDB.History.tag_version(table, "saved")
  end

  test "tags protect old versions from cleanup until deleted", %{table: table} do
    {:ok, version} = LanceDB.History.version(table)
    assert {:ok, :created} = LanceDB.History.create_tag(table, "keep", version)
    assert {:ok, %{inserted: 1}} = LanceDB.Table.add(table, [%{id: 2}])
    assert {:error, _} = LanceDB.Maintenance.cleanup_old_versions(table, older_than_seconds: 0)
    assert {:ok, table} = LanceDB.History.snapshot_tag(table, "keep")
    assert {:ok, 1} = LanceDB.Table.count_rows(table)
    assert {:ok, table} = LanceDB.History.latest(table)
    assert {:ok, :deleted} = LanceDB.History.delete_tag(table, "keep")
    assert {:ok, _} = LanceDB.Maintenance.cleanup_old_versions(table, older_than_seconds: 0)
    assert {:error, _} = LanceDB.History.snapshot(table, version)
  end

  test "validates tag names and version numbers before native calls", %{table: table} do
    for name <- ["", "  ", nil, :tag, 123, <<255>>] do
      assert_raise ArgumentError, fn -> LanceDB.History.create_tag(table, name, 1) end
      assert_raise ArgumentError, fn -> LanceDB.History.update_tag(table, name, 1) end
      assert_raise ArgumentError, fn -> LanceDB.History.delete_tag(table, name) end
      assert_raise ArgumentError, fn -> LanceDB.History.snapshot_tag(table, name) end
      assert_raise ArgumentError, fn -> LanceDB.History.tag_version(table, name) end
    end

    for version <- [0, -1, 1.0, nil, "1", 18_446_744_073_709_551_616] do
      assert_raise ArgumentError, fn -> LanceDB.History.create_tag(table, "saved", version) end
      assert_raise ArgumentError, fn -> LanceDB.History.update_tag(table, "saved", version) end
    end
  end
end
