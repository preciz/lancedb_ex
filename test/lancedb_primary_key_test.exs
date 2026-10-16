defmodule LanceDB.PrimaryKeyTest do
  use LanceDB.DataCase, async: true
  @moduletag :native_operation_contract

  alias LanceDB.{Connection, Error, History, Index, Metadata, Query, SchemaEvolution, Table}

  @position "lance-schema:unenforced-primary-key:position"
  @legacy "lance-schema:unenforced-primary-key"

  setup %{path: path} do
    {:ok, conn} = LanceDB.connect(path)
    %{conn: conn}
  end

  test "supported key types persist without checking uniqueness or building an index", %{
    conn: conn,
    path: path
  } do
    for {type, value} <- [
          {:int32, 42},
          {:int64, 42},
          {:string, "key"},
          {:large_string, "key"},
          {:binary, <<0, 255>>},
          {:large_binary, <<0, 255>>},
          {{:fixed_size_binary, 2}, <<0, 255>>}
        ] do
      name = "keys_#{System.unique_integer([:positive])}"

      {:ok, table} =
        Connection.create_table(conn, name, [%{key: value}, %{key: value}],
          schema: [%{name: :key, type: type, nullable: false, metadata: %{label: "Key"}}]
        )

      {:ok, initial} = History.version(table)
      {:ok, before_key} = History.snapshot(table, initial)

      assert {:ok,
              %LanceDB.WriteResult{
                operation: :set_unenforced_primary_key,
                version: nil,
                inserted: nil,
                updated: nil,
                deleted: nil,
                attempts: nil,
                commits: nil
              }} = Table.set_unenforced_primary_key(table, :key)

      assert {:ok, version} = History.version(table)
      assert version == initial + 1
      assert key_metadata(table) == %{@position => "1", "label" => "Key"}
      assert key_metadata(before_key) == %{"label" => "Key"}
      assert {:ok, []} = Index.list(table)

      assert {:ok, %{inserted: 1}} = Table.add(table, [%{key: value}])
      assert {:ok, 3} = Table.count_rows(table)
      assert {:error, %Error{}} = Table.add(table, [%{key: nil}])

      {:ok, other} = LanceDB.connect(path)
      {:ok, reopened} = Connection.open_table(other, name)
      assert key_metadata(reopened) == key_metadata(table)
      assert {:ok, 3} = Table.count_rows(reopened)
    end
  end

  test "empty tables can declare a key before inserting", %{conn: conn} do
    {:ok, table} =
      Connection.create_empty_table(conn, "empty", [
        %{name: :key, type: :string, nullable: false}
      ])

    assert {:ok, _} = Table.set_unenforced_primary_key(table, "key")
    assert {:ok, 0} = Table.count_rows(table)
    assert {:ok, %{inserted: 1}} = Table.add(table, [%{key: "first"}])
    assert key_metadata(table) == %{@position => "1"}
  end

  test "setting an existing key again or choosing another column never commits", %{conn: conn} do
    {:ok, table} =
      Connection.create_empty_table(conn, "immutable", [
        %{name: :key, type: :string, nullable: false},
        %{name: :other, type: :int64, nullable: false}
      ])

    assert {:ok, _} = Table.set_unenforced_primary_key(table, "key")
    {:ok, version} = History.version(table)

    for column <- ["key", :other] do
      assert {:error,
              %Error{
                kind: :invalid_data,
                operation: :set_unenforced_primary_key,
                details: %{phase: :execution, commit_outcome: :unknown}
              }} = Table.set_unenforced_primary_key(table, column)
    end

    assert {:ok, ^version} = History.version(table)
    assert key_metadata(table) == %{@position => "1"}
  end

  test "missing and unsupported columns fail without changing the table", %{conn: conn} do
    {:ok, table} =
      Connection.create_empty_table(conn, "invalid", [
        %{name: :value, type: :float64, nullable: false},
        %{name: :small, type: :int16, nullable: false},
        %{name: :unsigned, type: :uint64, nullable: false},
        %{name: :flag, type: :boolean, nullable: false}
      ])

    {:ok, version} = History.version(table)

    for column <- [:missing, :value, :small, :unsigned, :flag] do
      assert {:error,
              %Error{
                kind: :invalid_data,
                details: %{phase: :execution, commit_outcome: :unknown}
              }} = Table.set_unenforced_primary_key(table, column)
    end

    assert {:ok, ^version} = History.version(table)
  end

  test "nullable fields require an explicit null-checked schema change", %{conn: conn} do
    {:ok, table} = Connection.create_table(conn, "nullable", [%{key: "a"}, %{key: nil}])
    {:ok, version} = History.version(table)
    assert {:error, %Error{kind: :invalid_data}} = Table.set_unenforced_primary_key(table, :key)
    assert {:ok, ^version} = History.version(table)
    refute Map.has_key?(key_metadata(table), @position)

    assert {:error, %Error{}} = SchemaEvolution.alter_columns(table, key: [nullable: false])
    assert {:ok, _} = Table.delete(table, "key IS NULL")
    assert {:ok, _} = SchemaEvolution.alter_columns(table, key: [nullable: false])
    assert {:ok, _} = Table.set_unenforced_primary_key(table, :key)
    assert {:error, %Error{}} = SchemaEvolution.alter_columns(table, key: [nullable: true])
    assert {:ok, [%{"key" => "a"}]} = Query.query(table)
  end

  test "nested keys require non-nullable ancestors", %{conn: conn} do
    {:ok, table} =
      Connection.create_empty_table(conn, "nested", [
        %{name: :data, type: {:struct, [%{name: :key, type: :int64, nullable: false}]}}
      ])

    {:ok, version} = History.version(table)

    assert {:error, %Error{kind: :invalid_data}} =
             Table.set_unenforced_primary_key(table, "data.key")

    assert {:ok, ^version} = History.version(table)
    assert {:ok, _} = SchemaEvolution.alter_columns(table, data: [nullable: false])
    assert {:ok, _} = Table.set_unenforced_primary_key(table, "data.key")
    assert {:ok, %{fields: [data]}} = Table.schema(table)
    assert hd(elem(data.type, 1)).metadata == %{@position => "1"}
  end

  test "user metadata can merge and replace without rewriting the immutable key", %{conn: conn} do
    {:ok, table} =
      Connection.create_empty_table(conn, "metadata", [
        %{name: :key, type: :int64, nullable: false, metadata: %{label: "Key"}},
        {:other, :string}
      ])

    assert {:ok, _} = Table.set_unenforced_primary_key(table, :key)
    assert {:ok, _} = Metadata.update_field(table, :key, %{note: "hello"})
    assert key_metadata(table) == %{@position => "1", "label" => "Key", "note" => "hello"}
    assert {:ok, _} = Metadata.update_field(table, :key, %{unit: "id"}, replace: true)
    assert key_metadata(table) == %{@position => "1", "unit" => "id"}
    assert {:ok, _} = Metadata.update_field(table, :key, %{}, replace: true)
    assert key_metadata(table) == %{@position => "1"}

    # A read/modify/write of an unchanged marker is harmless and must not be
    # sent to Lance, which rejects even identical marker writes.
    assert {:ok, _} = Metadata.update_field(table, :key, %{@position => "1", "label" => "Key"})
    assert {:ok, _} = Metadata.update_field(table, :key, key_metadata(table), replace: true)
    {:ok, version} = History.version(table)

    for {column, changes} <- [
          {:key, %{@position => nil}},
          {:key, %{@position => "2"}},
          {:key, %{@legacy => "true"}},
          {:other, %{@position => "1"}}
        ] do
      assert {:error,
              %Error{
                kind: :unsupported,
                details: %{phase: :validation, commit_outcome: :not_started}
              }} = Metadata.update_field(table, column, changes)
    end

    assert {:ok, ^version} = History.version(table)
    assert key_metadata(table) == %{@position => "1", "label" => "Key"}
  end

  test "stale handles cannot replace a concurrently committed key", %{path: path} do
    {:ok, conn} = LanceDB.connect(path, %{}, read_consistency_interval_ms: 3_600_000)

    {:ok, first} =
      Connection.create_empty_table(conn, "race", [
        %{name: :key, type: :int64, nullable: false},
        %{name: :other, type: :int64, nullable: false}
      ])

    {:ok, stale} = Connection.open_table(conn, "race")
    assert {:ok, _} = Table.set_unenforced_primary_key(first, :key)

    assert {:error, %Error{details: %{phase: :execution, commit_outcome: :unknown}}} =
             Table.set_unenforced_primary_key(stale, :other)

    {:ok, fresh} = Connection.open_table(conn, "race")
    assert key_metadata(fresh) == %{@position => "1"}
    assert {:ok, version} = History.version(fresh)
    assert version == 2
  end

  test "existing legacy keys survive user metadata replacement", %{conn: conn} do
    {:ok, table} =
      Connection.create_empty_table(conn, "legacy", [
        %{name: :key, type: :int64, nullable: false, metadata: %{@legacy => "true"}}
      ])

    markers = key_metadata(table)
    assert {:error, %Error{}} = Table.set_unenforced_primary_key(table, :key)
    assert {:ok, _} = Metadata.update_field(table, :key, %{label: "Legacy"}, replace: true)
    assert key_metadata(table) == Map.put(markers, "label", "Legacy")
    assert {:ok, _} = Metadata.update_field(table, :key, markers, replace: true)
    assert key_metadata(table) == markers
  end

  test "renaming a key preserves its designation and unrelated schema edits still work", %{
    conn: conn
  } do
    {:ok, table} =
      Connection.create_table(conn, "rename", [%{key: 1}],
        schema: [%{name: :key, type: :int64, nullable: false}]
      )

    assert {:ok, _} = Table.set_unenforced_primary_key(table, :key)
    assert {:ok, _} = SchemaEvolution.add_columns(table, extra: "key + 1")
    assert {:ok, _} = SchemaEvolution.drop_columns(table, [:extra])
    assert {:ok, _} = SchemaEvolution.alter_columns(table, key: [rename: :document_id])
    assert {:ok, %{fields: [field]}} = Table.schema(table)
    assert field.name == "document_id"
    assert field.metadata == %{@position => "1"}

    assert {:ok, %{updated: 1, inserted: 1}} =
             Table.upsert(table, [%{document_id: 1}, %{document_id: 2}], on: :document_id)
  end

  test "merge and upsert retain their explicit join keys and default id", %{conn: conn} do
    {:ok, table} =
      Connection.create_table(conn, "merges", [%{id: 1, key: "a", text: "old"}],
        schema: [{:id, :int64}, %{name: :key, type: :string, nullable: false}, {:text, :string}]
      )

    assert {:ok, _} = Table.set_unenforced_primary_key(table, :key)

    assert {:ok, %{inserted: 1, updated: 1}} =
             Table.upsert(table, [%{id: 2, key: "a", text: "new"}, %{id: 3, key: "b", text: "b"}],
               on: :key
             )

    assert {:ok, %{updated: 1}} =
             Table.merge(table, [%{id: 4, key: "a", text: "merged"}],
               on: :key,
               when_matched_update_all: true
             )

    assert {:ok, %{inserted: 0, updated: 1}} =
             Table.upsert(table, [%{id: 4, key: "c", text: "by id"}])

    assert {:ok, 2} = Table.count_rows(table)

    assert {:ok, [%{"id" => 4, "key" => "c", "text" => "by id"}]} =
             Query.query(table, filter: "id = 4")

    assert key_metadata(table) == %{@position => "1"}
  end

  defp key_metadata(table) do
    {:ok, %{fields: fields}} = Table.schema(table)
    Enum.find(fields, &(&1.name == "key")).metadata
  end
end
