defmodule LanceDB.ArrowValidationTest do
  use LanceDB.DataCase, async: true
  @moduletag :native_value_validation

  alias LanceDB.{Batch, Connection, Error, History, Query, Schema, SchemaEvolution, Source, Table}

  setup %{path: path} do
    {:ok, conn} = Connection.connect(path)
    %{conn: conn}
  end

  @cases [
    {:jsonb, :json, %{"architecture_jsonb_sentinel" => "value"}, "architecture_jsonb_sentinel",
     :binary.copy(<<255>>, 27)},
    {:nan_vector, {:vector, 1}, [42.125], <<42.125::little-float-32>>, <<0, 0, 192, 127>>},
    {:infinite_vector, {:vector, 1}, [42.125], <<42.125::little-float-32>>, <<0, 0, 128, 127>>},
    {:nan_scalar, :float32, 42.125, <<42.125::little-float-32>>, <<0, 0, 192, 127>>},
    {:time, {:time32, :second}, 12_345, <<12_345::little-signed-32>>,
     <<100_000::little-signed-32>>},
    {:date, :date64, 86_400_000, <<86_400_000::little-signed-64>>,
     <<86_400_001::little-signed-64>>},
    {:timestamp, {:timestamp, :second}, 123_456, <<123_456::little-signed-64>>,
     <<253_402_300_800::little-signed-64>>},
    {:decimal, {:decimal128, 2, 0}, "42", <<42::little-signed-128>>, <<100::little-signed-128>>}
  ]

  for {name, type, value, pattern, replacement} <- @cases do
    @case {name, type, value, pattern, replacement}

    test "#{name}: creation, append and merge reject invalid imported values", %{conn: conn} do
      {name, type, value, pattern, replacement} = @case
      schema = Schema.new(id: :int64, value: type)
      bad = imported([%{id: 1, value: value}], schema, pattern, replacement)
      source = Source.batches([bad], schema: schema)
      check_imported_rows(name, bad)
      creation_name = "create_#{name}"

      assert {:error, %Error{kind: :invalid_data}} =
               Connection.create_table(conn, creation_name, source)

      assert {:ok, names} = Connection.table_names(conn)
      refute creation_name in names

      {:ok, table} =
        Connection.create_table(conn, "write_#{name}", [%{id: 0, value: value}], schema: schema)

      {:ok, expected} = Query.query(table)
      {:ok, version} = History.version(table)

      for write <- [
            &Table.add(&1, &2),
            &Table.upsert(&1, &2),
            &Table.merge(&1, &2, when_not_matched_insert_all: true)
          ] do
        assert {:error, %Error{kind: :invalid_data}} = write.(table, source)
        assert {:ok, ^version} = History.version(table)
        assert {:ok, ^expected} = Query.query(table)
      end
    end

    test "#{name}: column sources and backfills reject invalid imported values", %{conn: conn} do
      {name, type, value, pattern, replacement} = @case
      schema = Schema.new(value: type)
      bad = imported([%{value: value}], schema, pattern, replacement)
      {:ok, table} = Connection.create_table(conn, "columns_#{name}", [%{id: 0}])
      {:ok, version} = History.version(table)

      assert {:error, %Error{kind: :invalid_data}} =
               SchemaEvolution.add_from_source(table, Source.batches([bad], schema: schema))

      assert {:error, %Error{kind: :invalid_data}} =
               SchemaEvolution.backfill_columns(table, schema, fn _ -> bad end)

      assert {:ok, ^version} = History.version(table)
      assert {:ok, [%{"id" => 0}]} = Query.query(table)
      assert {:ok, %Schema{fields: [%{name: "id"}]}} = Table.schema(table)
    end
  end

  test "invalid later batches preserve the documented commit boundary", %{conn: conn} do
    schema = Schema.new(id: :int64, value: {:vector, 1})
    {:ok, good} = Batch.from_rows([%{id: 1, value: [1.0]}], schema)

    bad =
      imported(
        [%{id: 2, value: [42.125]}],
        schema,
        <<42.125::little-float-32>>,
        <<0, 0, 128, 127>>
      )

    {:ok, table} = Connection.create_empty_table(conn, "boundaries", schema)
    source = Source.batches([good, bad])

    assert {:error, %Error{kind: :invalid_data, details: %{inserted: 0}}} =
             Table.add(table, source)

    assert {:ok, 0} = Table.count_rows(table)
    assert {:ok, 1} = History.version(table)

    assert {:error, %Error{kind: :invalid_data, details: %{inserted: 1, commits: 1, version: 2}}} =
             Table.add(table, source, commit: :batch)

    assert {:ok, [%{"id" => 1, "value" => [1.0]}]} = Query.query(table)
    assert {:ok, 2} = History.version(table)
  end

  test "malformed JSONB beneath a null parent is masked during row decoding and table writes", %{
    conn: conn
  } do
    schema = Schema.new(id: :int64, value: {:struct, [data: :json]})
    sentinel = %{"architecture_jsonb_sentinel" => "value"}

    {:ok, batch} =
      Batch.from_rows(
        [
          %{id: 1, value: %{data: sentinel}},
          %{id: 2, value: nil},
          %{id: 3, value: %{data: %{"ok" => true}}}
        ],
        schema
      )

    {:ok, ipc} = Batch.to_ipc(batch)
    [{json_offset, 27}] = :binary.matches(ipc, "architecture_jsonb_sentinel")
    <<prefix::binary-size(^json_offset), _::binary-size(27), suffix::binary>> = ipc
    corrupted_ipc = prefix <> :binary.copy(<<255>>, 27) <> suffix

    # When the parent struct on row 0 is non-null, decoding and writes reject the malformed JSONB.
    {:ok, [unmasked_batch]} = Batch.from_ipc(corrupted_ipc)
    assert {:error, %Error{kind: :invalid_data}} = Batch.to_rows(unmasked_batch)

    assert {:error, %Error{kind: :invalid_data}} =
             Connection.create_table(
               conn,
               "unmasked_null_parent",
               Source.batches([unmasked_batch], schema: schema)
             )

    # Mask row 0 on the parent StructArray validity bitmap (0b101 -> 0b110, keeping null_count = 1)
    # while leaving the child LargeBinaryArray slot 0 non-null (0b101) with the corrupted JSONB bytes.
    [{mask_offset, 8}, _child_mask] = :binary.matches(corrupted_ipc, <<5, 0, 0, 0, 0, 0, 0, 0>>)
    <<mask_prefix::binary-size(^mask_offset), 5, mask_suffix::binary>> = corrupted_ipc
    masked_ipc = mask_prefix <> <<6>> <> mask_suffix

    assert {:ok, [masked_batch]} = Batch.from_ipc(masked_ipc)

    expected = [
      %{"id" => 1, "value" => nil},
      %{"id" => 2, "value" => %{"data" => nil}},
      %{"id" => 3, "value" => %{"data" => %{"ok" => true}}}
    ]

    assert {:ok, ^expected} = Batch.to_rows(masked_batch)

    assert {:ok, table} =
             Connection.create_table(
               conn,
               "masked_null_parent",
               Source.batches([masked_batch], schema: schema)
             )

    assert {:ok, ^expected} =
             table |> Query.new(limit: nil, order_by: [asc: :id]) |> Query.all()
  end

  # Alter one valid IPC value buffer to represent input from another Arrow producer.
  # Metadata, offsets, null masks, and the physical schema remain structurally valid.
  defp imported(rows, schema, pattern, replacement) do
    {:ok, batch} = Batch.from_rows(rows, schema)
    {:ok, ipc} = Batch.to_ipc(batch)
    [{offset, width}] = :binary.matches(ipc, pattern)
    assert byte_size(replacement) == width
    <<prefix::binary-size(^offset), _::binary-size(^width), suffix::binary>> = ipc
    {:ok, [batch]} = Batch.from_ipc(prefix <> replacement <> suffix)
    batch
  end

  defp check_imported_rows(:jsonb, batch),
    do: assert({:error, %Error{kind: :invalid_data}} = Batch.to_rows(batch))

  defp check_imported_rows(_, _), do: :ok
end
