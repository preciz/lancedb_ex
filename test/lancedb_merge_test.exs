defmodule LanceDB.MergeTest do
  use LanceDB.DataCase, async: true

  setup %{path: path} do
    {:ok, conn} = LanceDB.connect(path)

    {:ok, table} =
      LanceDB.Connection.create_table(conn, "docs", [
        %{id: 1, revision: 2, tenant: "a"},
        %{id: 2, revision: 2, tenant: "a"},
        %{id: 3, revision: 2, tenant: "b"}
      ])

    %{table: table}
  end

  test "conditional updates keep stale matches, insert new rows, and return commit statistics", %{
    table: table
  } do
    assert {:ok, :created} = LanceDB.Index.create(table, :id, type: :btree)

    rows = [
      %{id: 1, revision: 1, tenant: "a"},
      %{id: 2, revision: 3, tenant: "a"},
      %{id: 4, revision: 1, tenant: "a"}
    ]

    opts = [
      when_matched_update_all: "source.revision > target.revision",
      when_not_matched_insert_all: true,
      timeout_ms: 30_000,
      use_index: false
    ]

    assert {:ok, %{inserted: 1, updated: 1, deleted: 0, version: version, attempts: attempts}} =
             LanceDB.Table.merge(table, rows, opts)

    assert attempts >= 1
    assert {:ok, ^version} = LanceDB.History.version(table)

    assert {:ok, [%{"revision" => 2}, %{"revision" => 3}, %{"revision" => 2}, %{"revision" => 1}]} =
             LanceDB.Query.query(table, order_by: [asc: :id])

    assert {:ok, %{inserted: 0, updated: 0, deleted: 0}} = LanceDB.Table.merge(table, rows, opts)
  end

  test "source-missing deletion can be scoped to a tenant", %{table: table} do
    assert {:ok, %{inserted: 0, updated: 1, deleted: 1}} =
             LanceDB.Table.merge(table, [%{id: 1, revision: 5, tenant: "a"}],
               when_matched_update_all: true,
               when_not_matched_by_source_delete: "tenant = 'a'"
             )

    assert {:ok, [%{"id" => 1}, %{"id" => 3}]} = LanceDB.Query.query(table, order_by: [asc: :id])

    assert {:ok, %{deleted: 1}} =
             LanceDB.Table.merge(table, [], when_not_matched_by_source_delete: "tenant = 'a'")

    assert {:ok, [%{"id" => 3}]} = LanceDB.Query.query(table)

    assert {:ok, %{deleted: 1}} =
             LanceDB.Table.merge(table, [], when_not_matched_by_source_delete: true)

    assert {:ok, 0} = LanceDB.Table.count_rows(table)
  end

  test "insert-only and update-only merges support composite keys", %{table: table} do
    rows = [%{id: 1, revision: 9, tenant: "a"}, %{id: 1, revision: 9, tenant: "b"}]

    assert {:ok, %{inserted: 1, updated: 0}} =
             LanceDB.Table.merge(table, rows,
               on: [:tenant, :id],
               when_not_matched_insert_all: true
             )

    assert {:ok, %{updated: 2, inserted: 0}} =
             LanceDB.Table.merge(table, rows, on: [:tenant, :id], when_matched_update_all: true)

    assert {:ok, 4} = LanceDB.Table.count_rows(table)
  end

  test "bad records, missing keys, and SQL errors do not alter data", %{table: table} do
    row = %{id: 1, revision: 1, tenant: "a"}

    for rows <- [
          [row, row],
          [%{row | id: nil}],
          [%{row | revision: "bad"}],
          [Map.delete(row, :tenant)]
        ] do
      assert {:error, _} = LanceDB.Table.merge(table, rows, when_matched_update_all: true)
    end

    assert {:error, _} =
             LanceDB.Table.merge(table, [],
               on: :missing,
               when_not_matched_by_source_delete: true
             )

    assert {:error, _} =
             LanceDB.Table.merge(table, [row], when_matched_update_all: "source.missing > 0")

    assert {:error, _} =
             LanceDB.Table.merge(table, [], when_not_matched_by_source_delete: "target.id =")

    assert {:ok, 3} = LanceDB.Table.count_rows(table)
  end

  test "merge actions are explicit and validated" do
    for opts <- [
          [],
          [when_matched_update_all: nil],
          [when_matched_update_all: ""],
          [when_not_matched_by_source_delete: 1],
          [when_not_matched_insert_all: :yes],
          [when_matched_update_all: true, use_index: nil],
          [when_matched_update_all: true, timeout_ms: 0],
          [when_matched_update_all: true, timeout_ms: nil],
          [when_matched_update_all: true, timeout_ms: 4_294_967_296],
          [when_matched_update_all: true, on: []],
          [typo: 1]
        ] do
      assert_raise ArgumentError, fn -> LanceDB.Table.merge(:unused, [], opts) end
    end
  end

  test "invalid options raise before mismatched source schema checks", %{table: table} do
    schema = LanceDB.Schema.new(id: :string)
    source = LanceDB.Source.rows([], schema: schema)

    for opts <- [[unknown: true], [], [when_matched_update_all: true, timeout_ms: 0]] do
      assert_raise ArgumentError, fn -> LanceDB.Table.merge(table, source, opts) end
    end

    assert {:ok, 3} = LanceDB.Table.count_rows(table)
  end
end
