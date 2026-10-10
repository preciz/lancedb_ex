defmodule LanceDB.NestedEvolutionTest do
  use LanceDB.DataCase, async: true

  setup %{path: path} do
    {:ok, conn} = LanceDB.connect(path)

    schema = [
      id: :int32,
      data: {:struct, [count: :int32, label: :string, optional: :string]},
      other: {:struct, [count: :int32]},
      values: {:list, :int16}
    ]

    row = %{
      id: 1,
      data: %{count: 10, label: "hello", optional: nil},
      other: %{count: 20},
      values: [1, nil, -1]
    }

    {:ok, table} = LanceDB.Connection.create_table(conn, "docs", [row], schema: schema)
    %{conn: conn, table: table, path: path}
  end

  test "nested renames, nullability and drops preserve siblings and subsequent writes", %{
    table: table,
    path: path
  } do
    assert {:ok, %{version: _}} =
             LanceDB.SchemaEvolution.alter_columns(table, [
               {"data.count", [rename: :total, nullable: false]},
               {"other.count", [rename: :total]}
             ])

    assert {:ok, [%{"data" => %{"total" => 10, "label" => "hello"}, "other" => %{"total" => 20}}]} =
             LanceDB.Query.query(table)

    assert {:ok, before} = LanceDB.Table.schema(table)

    assert {:error, _} =
             LanceDB.SchemaEvolution.alter_columns(table, [{"data.optional", [nullable: false]}])

    assert {:ok, ^before} = LanceDB.Table.schema(table)

    assert {:ok, %{version: _}} =
             LanceDB.SchemaEvolution.drop_columns(table, ["data.optional", "data.label"])

    assert {:ok, conn} = LanceDB.connect(path)
    assert {:ok, reopened} = LanceDB.Connection.open_table(conn, "docs")

    assert {:ok, %{inserted: 1}} =
             LanceDB.Table.add(reopened, [
               %{id: 2, data: %{total: 11}, other: %{total: 21}, values: []}
             ])

    assert {:error, _} =
             LanceDB.Table.add(reopened, [
               %{id: 3, data: %{total: nil}, other: %{total: 22}, values: []}
             ])

    assert {:ok, [%{"data" => %{"total" => 10}}, %{"data" => %{"total" => 11}}]} =
             LanceDB.Query.query(reopened, order_by: [asc: :id])
  end

  test "converts primitive and collection widths while preserving nulls", %{table: table} do
    assert {:ok, %{version: _}} =
             LanceDB.SchemaEvolution.alter_columns(table,
               id: [type: :uint64, rename: :key],
               values: [type: {:list, :int64}]
             )

    assert {:ok, [%{"key" => 1, "values" => [1, nil, -1]}]} = LanceDB.Query.query(table)
    assert {:ok, %{version: _}} = LanceDB.SchemaEvolution.alter_columns(table, key: [type: :int8])
    assert {:ok, %LanceDB.Schema{fields: fields}} = LanceDB.Table.schema(table)
    assert Enum.find(fields, &(&1.name == "key")).type == :int8
  end

  test "nested type conversions reject the upstream panic before mutation", %{table: table} do
    assert {:ok, before} = LanceDB.History.version(table)

    assert {:error, reason} =
             LanceDB.SchemaEvolution.alter_columns(table, [{"data.count", [type: :int64]}])

    assert reason.message =~ "nested field"
    assert {:ok, ^before} = LanceDB.History.version(table)
    assert {:ok, [%{"data" => %{"count" => 10}}]} = LanceDB.Query.query(table)
  end

  test "quoted nested names resolve without confusing literal dots with paths", %{conn: conn} do
    schema = [
      id: :int64,
      data: {:struct, [{"a.b", :int16}, {"tick`name", :string}, {:keep, :boolean}]}
    ]

    {:ok, table} =
      LanceDB.Connection.create_table(
        conn,
        "quoted",
        [%{id: 1, data: %{"a.b" => 2, "tick`name" => "hello", "keep" => true}}],
        schema: schema
      )

    assert {:ok, %{version: _}} =
             LanceDB.SchemaEvolution.alter_columns(table, [{"data.`a.b`", [rename: :count]}])

    assert {:ok, %{version: _}} =
             LanceDB.SchemaEvolution.drop_columns(table, ["data.`tick``name`"])

    assert {:ok, [%{"data" => %{"count" => 2, "keep" => true}}]} = LanceDB.Query.query(table)
  end

  test "temporal and floating conversions work on populated and empty tables", %{conn: conn} do
    schema = [day: :date32, score: :float32]

    {:ok, table} =
      LanceDB.Connection.create_table(conn, "temporal", [%{day: ~D[2024-02-29], score: 1.5}],
        schema: schema
      )

    assert {:ok, %{version: _}} =
             LanceDB.SchemaEvolution.alter_columns(table,
               day: [type: :date64],
               score: [type: :float64]
             )

    assert {:ok, [%{"day" => "2024-02-29", "score" => 1.5}]} = LanceDB.Query.query(table)

    assert {:ok, %{version: _}} =
             LanceDB.SchemaEvolution.alter_columns(table, day: [type: {:timestamp, :millisecond}])

    assert {:ok, [%{"day" => "2024-02-29T00:00:00"}]} = LanceDB.Query.query(table)
    {:ok, empty} = LanceDB.Connection.create_empty_table(conn, "empty_cast", schema)

    assert {:ok, %{version: _}} =
             LanceDB.SchemaEvolution.alter_columns(empty,
               day: [type: :date64],
               score: [type: :float64]
             )

    assert {:ok, %{inserted: 1}} = LanceDB.Table.add(empty, [%{day: ~D[1969-12-31], score: 2.5}])
    assert {:ok, [%{"day" => "1969-12-31", "score" => 2.5}]} = LanceDB.Query.query(empty)
  end

  test "invalid conversions and path conflicts do not commit partial changes", %{table: table} do
    assert {:ok, original} = LanceDB.Table.schema(table)

    for changes <- [
          [{"data.count", [rename: :label]}],
          [{"data.count", [rename: :same]}, {"data.label", [rename: :same]}],
          [{"data", [rename: :renamed]}, {"data.count", [nullable: false]}],
          [{"data.count", [nullable: false]}, {"`data`.`count`", [nullable: true]}],
          [{"data.missing", [rename: :new]}],
          [id: [rename: :key], values: [type: :string]],
          [id: [type: :float64]]
        ] do
      assert {:error, _} = LanceDB.SchemaEvolution.alter_columns(table, changes)
      assert {:ok, ^original} = LanceDB.Table.schema(table)
    end

    for columns <- [
          ["data", "data.count"],
          ["data.count", "`data`.`count`"],
          ["data.missing"],
          ["id", "data", "other", "values"]
        ] do
      assert {:error, _} = LanceDB.SchemaEvolution.drop_columns(table, columns)
      assert {:ok, ^original} = LanceDB.Table.schema(table)
    end
  end

  test "overflow and indexed casts fail without changing versions or values", %{conn: conn} do
    {:ok, table} =
      LanceDB.Connection.create_table(conn, "limits", [%{id: 256, value: -1}],
        schema: [id: :int32, value: :int32]
      )

    assert {:ok, before} = LanceDB.History.version(table)

    assert {:error, _} =
             LanceDB.SchemaEvolution.alter_columns(table,
               id: [type: :uint8],
               value: [rename: :new]
             )

    assert {:ok, ^before} = LanceDB.History.version(table)
    assert {:error, _} = LanceDB.SchemaEvolution.alter_columns(table, value: [type: :uint32])
    assert {:ok, ^before} = LanceDB.History.version(table)
    assert {:ok, :created} = LanceDB.Index.create(table, :id, type: :btree, name: "ids")
    assert {:error, reason} = LanceDB.SchemaEvolution.alter_columns(table, id: [type: :int64])
    assert reason.message =~ "index"
    assert {:ok, :dropped} = LanceDB.Index.drop(table, "ids")
    assert {:ok, %{version: _}} = LanceDB.SchemaEvolution.alter_columns(table, id: [type: :int64])
    assert {:ok, [%{"id" => 256, "value" => -1}]} = LanceDB.Query.query(table)
  end
end
