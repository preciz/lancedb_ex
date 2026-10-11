defmodule LanceDB.IPCSafetyTest do
  use ExUnit.Case, async: true
  alias LanceDB.{Batch, Error, Schema}

  @panic_message "Invalid Arrow IPC stream: parser rejected malformed data"

  setup do
    {:ok, batch} = Batch.from_rows([%{id: 1}], Schema.new(id: :int64))
    {:ok, ipc} = Batch.to_ipc(batch)
    %{ipc: ipc}
  end

  test "malformed schema fields and integer types return typed errors", %{ipc: ipc} do
    for malformed <- [without_schema_fields(ipc), with_invalid_integer_width(ipc)] do
      assert {:error,
              %Error{
                kind: :invalid_data,
                operation: :batch_from_ipc,
                message: @panic_message
              }} = Batch.from_ipc(malformed)

      assert {:ok, [batch]} = Batch.from_ipc(ipc)
      assert {:ok, [%{"id" => 1}]} = Batch.to_rows(batch)
    end
  end

  test "a parser panic after a valid batch returns an error for the entire import", %{ipc: ipc} do
    schema_size = schema_message_size(ipc)
    malformed_schema = binary_part(without_schema_fields(ipc), 0, schema_size)
    <<255, 255, 255, 255, 0, 0, 0, 0>> = binary_part(ipc, byte_size(ipc) - 8, 8)

    malformed =
      binary_part(ipc, 0, byte_size(ipc) - 8) <>
        malformed_schema <> <<255, 255, 255, 255, 0, 0, 0, 0>>

    assert {:error, %Error{kind: :invalid_data, message: @panic_message}} =
             Batch.from_ipc(malformed)

    assert {:ok, [batch]} = Batch.from_ipc(ipc)
    assert {:ok, [%{"id" => 1}]} = Batch.to_rows(batch)
  end

  test "ordinary parsing failures retain their original messages", %{ipc: ipc} do
    truncated = binary_part(ipc, 0, schema_message_size(ipc) + 8)

    for malformed <- ["invalid", truncated] do
      assert {:error, %Error{kind: :invalid_data, message: message}} = Batch.from_ipc(malformed)
      refute message == @panic_message
    end
  end

  test "valid streams import zero or multiple batches", %{ipc: ipc} do
    schema_size = schema_message_size(ipc)
    schema = binary_part(ipc, 0, schema_size)
    batch = binary_part(ipc, schema_size, byte_size(ipc) - schema_size - 8)
    eof = <<255, 255, 255, 255, 0, 0, 0, 0>>

    assert {:ok, []} = Batch.from_ipc(schema <> eof)
    assert {:ok, [first, second]} = Batch.from_ipc(schema <> batch <> batch <> eof)
    assert {:ok, [%{"id" => 1}]} = Batch.to_rows(first)
    assert {:ok, [%{"id" => 1}]} = Batch.to_rows(second)
  end

  defp without_schema_fields(ipc) do
    schema = schema_table(ipc)
    slot = vtable(ipc, schema) + 6
    assert u16(ipc, slot) > 0
    replace(ipc, slot, <<0::little-16>>)
  end

  defp with_invalid_integer_width(ipc) do
    schema = schema_table(ipc)
    fields = indirect(ipc, schema + u16(ipc, vtable(ipc, schema) + 6))
    field = indirect(ipc, fields + 4)
    integer = indirect(ipc, field + u16(ipc, vtable(ipc, field) + 10))
    width = integer + u16(ipc, vtable(ipc, integer) + 4)
    assert u32(ipc, width) == 64
    replace(ipc, width, <<48::little-32>>)
  end

  # IPC starts with a continuation marker and metadata length. FlatBuffers
  # tables reference vtables with signed offsets and children with unsigned
  # offsets. The mutations change schema metadata without changing buffer sizes.
  defp schema_message_size(ipc), do: 8 + u32(ipc, 4)

  defp schema_table(ipc) do
    <<255, 255, 255, 255, _::binary>> = ipc
    message = indirect(ipc, 8)
    indirect(ipc, message + u16(ipc, vtable(ipc, message) + 8))
  end

  defp vtable(bytes, table) do
    <<offset::little-signed-32>> = binary_part(bytes, table, 4)
    table - offset
  end

  defp indirect(bytes, position), do: position + u32(bytes, position)

  defp u16(bytes, position) do
    <<value::little-unsigned-16>> = binary_part(bytes, position, 2)
    value
  end

  defp u32(bytes, position) do
    <<value::little-unsigned-32>> = binary_part(bytes, position, 4)
    value
  end

  defp replace(bytes, position, replacement) do
    tail = position + byte_size(replacement)

    binary_part(bytes, 0, position) <>
      replacement <> binary_part(bytes, tail, byte_size(bytes) - tail)
  end
end
