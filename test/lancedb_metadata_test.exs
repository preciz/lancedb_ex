defmodule LanceDB.MetadataTest do
  use LanceDB.DataCase, async: true

  setup %{path: path} do
    {:ok, conn} = LanceDB.connect(path)

    schema = [
      %{name: :id, type: :int64, metadata: %{label: "ID"}},
      {:data, {:struct, [%{name: :value, type: :string, metadata: %{lang: "en"}}]}}
    ]

    {:ok, table} =
      LanceDB.Connection.create_table(conn, "docs", [%{id: 1, data: %{value: "hello"}}],
        schema: schema,
        schema_metadata: %{owner: "team"}
      )

    %{conn: conn, table: table}
  end

  test "creation metadata survives reads, appends, and reopening", %{conn: conn, table: table} do
    assert {:ok, %{"owner" => "team"}} = LanceDB.Metadata.schema(table)

    assert {:ok, %LanceDB.Schema{metadata: %{"owner" => "team"}, fields: [id, data]}} =
             LanceDB.Table.schema(table)

    assert id.metadata == %{"label" => "ID"}
    assert hd(elem(data.type, 1)).metadata == %{"lang" => "en"}
    assert {:ok, %{inserted: 1}} = LanceDB.Table.add(table, [%{id: 2, data: %{value: "next"}}])
    {:ok, reopened} = LanceDB.Connection.open_table(conn, "docs")
    assert LanceDB.Table.schema(reopened) == LanceDB.Table.schema(table)

    assert {:ok, empty} =
             LanceDB.Connection.create_empty_table(conn, "empty", [id: :int64],
               schema_metadata: %{note: "empty"}
             )

    assert {:ok, %{"note" => "empty"}} = LanceDB.Metadata.schema(empty)

    assert {:ok, inferred} =
             LanceDB.Connection.create_table(conn, "inferred", [%{id: 1}],
               schema_metadata: %{source: "inferred"}
             )

    assert {:ok, %{"source" => "inferred"}} = LanceDB.Metadata.schema(inferred)
  end

  test "schema metadata merges, deletes and replaces with versioned history", %{table: table} do
    {:ok, initial} = LanceDB.History.version(table)

    assert {:ok, %{version: next}} =
             LanceDB.Metadata.update_schema(table, %{owner: nil, note: ""})

    assert next > initial
    assert {:ok, %{"note" => ""}} = LanceDB.Metadata.schema(table)

    assert {:ok, %{version: _}} =
             LanceDB.Metadata.update_schema(table, %{new: "yes"}, replace: true)

    assert {:ok, %{"new" => "yes"}} = LanceDB.Metadata.schema(table)
    assert {:ok, %{version: _}} = LanceDB.Metadata.update_schema(table, %{}, replace: true)
    assert {:ok, %{}} = LanceDB.Metadata.schema(table)
    assert {:ok, table} = LanceDB.History.snapshot(table, initial)
    assert {:ok, %{"owner" => "team"}} = LanceDB.Metadata.schema(table)
    assert {:error, _} = LanceDB.Metadata.update_schema(table, %{blocked: "yes"})
    assert {:error, _} = LanceDB.Metadata.update_field(table, :id, %{blocked: "yes"})
  end

  test "nested field metadata can be merged, replaced, deleted and cleared", %{table: table} do
    assert {:ok, %{version: _}} =
             LanceDB.Metadata.update_field(table, "data.value", %{
               lang: nil,
               description: "value"
             })

    assert {:ok, %LanceDB.Schema{fields: [_, data]}} = LanceDB.Table.schema(table)
    assert hd(elem(data.type, 1)).metadata == %{"description" => "value"}

    assert {:ok, %{version: _}} =
             LanceDB.Metadata.update_field(table, :id, %{unit: "count"}, replace: true)

    assert {:ok, %LanceDB.Schema{fields: [id, _]}} = LanceDB.Table.schema(table)
    assert id.metadata == %{"unit" => "count"}

    assert {:ok, %{version: _}} =
             LanceDB.Metadata.update_field(table, :id, %{}, replace: true)

    assert {:ok, %LanceDB.Schema{fields: [id, _]}} = LanceDB.Table.schema(table)
    refute Map.has_key?(id, "metadata")

    assert {:error, _} =
             LanceDB.Metadata.update_field(table, "data.missing", %{key: "value"})

    assert {:ok, 1} = LanceDB.Table.count_rows(table)
  end

  test "metadata is isolated across branches and shallow clones", %{conn: conn, table: table} do
    {:ok, branch} = LanceDB.Branch.create(table, "branch")
    {:ok, source} = LanceDB.Table.uri(table)
    {:ok, copy} = LanceDB.Connection.clone_table(conn, "copy", source)

    for handle <- [branch, copy] do
      assert {:ok, %{version: _}} =
               LanceDB.Metadata.update_schema(handle, %{owner: "other"})

      assert {:ok, %{version: _}} =
               LanceDB.Metadata.update_field(handle, :id, %{label: "other"})
    end

    assert {:ok, %{"owner" => "team"}} = LanceDB.Metadata.schema(table)
    assert {:ok, %LanceDB.Schema{fields: [id, _]}} = LanceDB.Table.schema(table)
    assert id.metadata == %{"label" => "ID"}
  end

  test "metadata validates maps, normalized keys, values and options" do
    for changes <- [
          nil,
          [],
          %URI{},
          %{nil => "x"},
          %{key: 1},
          %{key: <<255>>},
          %{:key => "a", "key" => "b"}
        ] do
      assert_raise ArgumentError, fn -> LanceDB.Metadata.update_schema(:unused, changes) end

      assert_raise ArgumentError, fn ->
        LanceDB.Metadata.update_field(:unused, :id, changes)
      end
    end

    assert_raise ArgumentError, fn ->
      LanceDB.Metadata.update_schema(:unused, %{}, replace: nil)
    end

    assert_raise ArgumentError, fn -> LanceDB.Metadata.update_field(:unused, "", %{}) end

    assert_raise ArgumentError, fn ->
      LanceDB.Connection.create_empty_table(:unused, "t", [id: :int64],
        schema_metadata: %{key: nil}
      )
    end

    assert_raise ArgumentError, fn ->
      LanceDB.Connection.create_empty_table(:unused, "t", [
        %{name: :id, type: :int64, metadata: []}
      ])
    end
  end
end
