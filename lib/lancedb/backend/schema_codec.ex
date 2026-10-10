defmodule LanceDB.Backend.SchemaCodec do
  @moduledoc false
  alias LanceDB.{Field, Schema}
  @doc false
  def decode(json, representation \\ :logical) when is_binary(json) do
    case JSON.decode(json) do
      {:ok, wire} -> {:ok, from_wire(wire, representation)}
      error -> error
    end
  end

  defp from_wire(wire, representation) do
    module = if representation == :arrow, do: LanceDB.ArrowSchema, else: LanceDB.Schema

    struct!(module,
      fields: decode_fields(fetch(wire, :fields), representation),
      metadata: fetch(wire, :metadata) || %{}
    )
  end

  defp decode_fields(fields, representation) do
    fields = Enum.map(fields, &decode_field/1)
    if representation == :logical, do: logical_fields(fields), else: fields
  end

  def logical_fields(fields), do: Enum.map(fields, &logical_field/1)

  defp logical_field(%Field{type: type, metadata: metadata} = field) do
    extension = metadata["ARROW:extension:name"]

    if (extension == "lance.json" and type == :large_binary) or
         (extension == "arrow.json" and type in [:string, :large_string]) do
      %{field | type: :json, metadata: Map.delete(metadata, "ARROW:extension:name")}
    else
      %{field | type: logical_type(type)}
    end
  end

  defp logical_type({kind, %Field{} = item}) when kind in [:list, :large_list, :map],
    do: {kind, logical_field(item)}

  defp logical_type({:struct, fields}), do: {:struct, logical_fields(fields)}
  defp logical_type(type), do: type

  defp decode_field(wire) do
    %Field{
      name: fetch(wire, :name),
      type: decode_type(wire),
      nullable: fetch(wire, :nullable),
      metadata: fetch(wire, :metadata) || %{}
    }
  end

  @primitives ~w(string large_string binary large_binary blob json date32 date64 int8 int16 int32 int64 uint8 uint16 uint32 uint64 float16 float32 float64 boolean)a
  @units ~w(second millisecond microsecond nanosecond)a
  defp decode_type(wire), do: decode_type(fetch(wire, :type), wire)

  defp decode_type(name, wire) when name in ["list", "large_list", "map"],
    do: {collection_kind(name), decode_field(fetch(wire, :item))}

  defp decode_type(name, wire) when name in ["decimal128", "decimal256"],
    do:
      {if(name == "decimal128", do: :decimal128, else: :decimal256), fetch(wire, :precision),
       fetch(wire, :scale)}

  defp decode_type(name, wire) when name in ["time32", "time64", "duration"],
    do: {temporal_kind(name), Enum.find(@units, &(to_string(&1) == fetch(wire, :unit)))}

  defp decode_type("fixed_size_binary", wire), do: {:fixed_size_binary, fetch(wire, :width)}

  defp decode_type("dictionary", wire),
    do: {:dictionary, decode_type(fetch(wire, :key)), decode_type(fetch(wire, :value))}

  defp decode_type("struct", wire), do: {:struct, Enum.map(fetch(wire, :fields), &decode_field/1)}
  defp decode_type("vector", wire), do: decode_vector(wire)

  defp decode_type("timestamp", wire),
    do:
      {:timestamp, Enum.find(@units, &(to_string(&1) == fetch(wire, :unit))),
       fetch(wire, :timezone)}

  defp decode_type("unsupported", wire), do: {:unsupported, fetch(wire, :arrow_type)}

  defp decode_type(name, _wire),
    do:
      Enum.find(@primitives, &(to_string(&1) == name)) ||
        raise(ArgumentError, "unknown wire type #{inspect(name)}")

  defp collection_kind("list"), do: :list
  defp collection_kind("large_list"), do: :large_list
  defp collection_kind("map"), do: :map
  defp temporal_kind("time32"), do: :time32
  defp temporal_kind("time64"), do: :time64
  defp temporal_kind("duration"), do: :duration

  defp decode_vector(wire) do
    dimension = fetch(wire, :dimension)

    case fetch(wire, :item) do
      nil ->
        {:vector, dimension}

      item ->
        case decode_field(item) do
          %Field{name: "item", type: :float32, nullable: true, metadata: metadata}
          when map_size(metadata) == 0 ->
            {:vector, dimension}

          %Field{name: "item", type: type, nullable: true, metadata: metadata}
          when map_size(metadata) == 0 ->
            {:vector, dimension, type}

          field ->
            {:vector, dimension, field}
        end
    end
  end

  defp fetch(map, key), do: Map.get(map, key, Map.get(map, to_string(key)))

  def encode!(nil), do: nil
  def encode!(%Schema{} = schema), do: encode!(schema.fields)

  def encode!(fields),
    do: fields |> Schema.new() |> Map.fetch!(:fields) |> Enum.map(&field/1) |> JSON.encode!()

  def encode_description(fields), do: fields |> Enum.map(&field/1) |> JSON.encode!()

  def type_definition!(type), do: type |> Schema.normalize_type!() |> type()

  defp field(%Field{} = field),
    do:
      Map.merge(type(field.type), %{
        name: field.name,
        nullable: field.nullable,
        metadata: field.metadata
      })

  defp type({:vector, dimension}), do: %{type: "vector", dimension: dimension}

  defp type({:vector, dimension, %Field{} = item}),
    do: %{type: "vector", dimension: dimension, item: field(item)}

  defp type({:vector, dimension, element}),
    do: type({:vector, dimension, %Field{name: "item", type: element, nullable: true}})

  defp type({:timestamp, unit, timezone}),
    do: %{type: "timestamp", unit: to_string(unit), timezone: timezone}

  defp type({kind, item}) when kind in [:list, :large_list, :map],
    do: %{type: to_string(kind), item: field(item)}

  defp type({kind, precision, scale}) when kind in [:decimal128, :decimal256],
    do: %{type: to_string(kind), precision: precision, scale: scale}

  defp type({kind, unit}) when kind in [:time32, :time64, :duration],
    do: %{type: to_string(kind), unit: to_string(unit)}

  defp type({:fixed_size_binary, width}), do: %{type: "fixed_size_binary", width: width}

  defp type({:dictionary, key, value}),
    do: %{type: "dictionary", key: type(key), value: type(value)}

  defp type({:struct, fields}), do: %{type: "struct", fields: Enum.map(fields, &field/1)}
  defp type({:unsupported, description}), do: %{type: "unsupported", arrow_type: description}
  defp type(primitive), do: %{type: Atom.to_string(primitive)}
end
