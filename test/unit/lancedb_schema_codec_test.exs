defmodule LanceDB.SchemaCodecTest do
  use ExUnit.Case, async: true
  use ExUnitProperties
  alias LanceDB.Backend.SchemaCodec
  alias LanceDB.{Field, Generators, Schema}

  property "recursive schema descriptions retain types, nullability and all metadata" do
    check all(schema <- Generators.schema()) do
      wire = %{
        fields: schema.fields |> SchemaCodec.encode_description() |> JSON.decode!(),
        metadata: schema.metadata
      }

      assert {:ok, ^schema} = SchemaCodec.decode(JSON.encode!(wire))
      assert Schema.new(schema.fields, schema.metadata) == schema
    end
  end

  property "duplicate aliases are rejected inside recursive structs" do
    check all(depth <- integer(0..4), type <- member_of([:int64, :string, :binary])) do
      duplicate = {:struct, [{:value, type}, {"value", type}]}

      nested =
        Enum.reduce(List.duplicate(:wrap, depth), duplicate, fn _, type -> {:list, type} end)

      assert_raise ArgumentError, ~r/duplicate/, fn -> Schema.new(root: nested) end
    end
  end

  test "Arrow JSON extensions are converted at every collection depth without dropping user metadata" do
    json = %Field{
      name: "data",
      type: :large_binary,
      metadata: %{"ARROW:extension:name" => "lance.json", "owner" => "user"}
    }

    logical = %{json | type: :json, metadata: %{"owner" => "user"}}

    fields = [
      json,
      %Field{name: "list", type: {:list, json}},
      %Field{name: "struct", type: {:struct, [json]}}
    ]

    assert SchemaCodec.logical_fields(fields) == [
             logical,
             %Field{name: "list", type: {:list, logical}},
             %Field{name: "struct", type: {:struct, [logical]}}
           ]
  end
end
