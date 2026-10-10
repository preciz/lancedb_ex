defmodule LanceDB.Generators do
  @moduledoc false
  import StreamData
  import ExUnitProperties, only: [gen: 2]

  def text do
    one_of([
      string(:utf8, max_length: 24),
      member_of(["", "'", "O'Brien", "\\", "\n\r\t", <<0>>, "雪 🦀", "'; DROP TABLE docs; --"])
    ])
  end

  def int64 do
    one_of([
      integer(-10_000..10_000),
      member_of([-9_223_372_036_854_775_808, 9_223_372_036_854_775_807, 9_007_199_254_740_993])
    ])
  end

  # Select finite keys by construction instead of rejection sampling near the
  # domain's capacity, which can raise TooManyDuplicatesError for valid seeds.
  def unique_subset(values, min_length, max_length) do
    gen all(count <- integer(min_length..max_length), shuffled <- shuffle(values)) do
      Enum.take(shuffled, count)
    end
  end

  # Bounded recursion keeps examples small and lets StreamData shrink the shape.
  def json_value(depth \\ 2)

  def json_value(0),
    do: one_of([constant(nil), boolean(), int64(), text(), float(min: -100, max: 100)])

  def json_value(depth) do
    child = json_value(depth - 1)
    one_of([json_value(0), list_of(child, max_length: 4), map_of(text(), child, max_length: 4)])
  end

  def metadata, do: map_of(map(text(), &("key" <> &1)), text(), max_length: 3)

  def schema do
    gen all(
          type <- logical_type(2),
          nullable <- boolean(),
          field_metadata <- metadata(),
          schema_metadata <- metadata()
        ) do
      LanceDB.Schema.new(
        [%{name: "value", type: type, nullable: nullable, metadata: field_metadata}],
        schema_metadata
      )
    end
  end

  defp logical_type(0) do
    one_of([
      member_of([:string, :binary, :boolean, :int8, :int64, :uint64, :float32, :float64, :json]),
      map(integer(1..8), &{:vector, &1}),
      map(integer(1..8), &{:fixed_size_binary, &1}),
      member_of([
        {:timestamp, :nanosecond, "UTC"},
        {:timestamp, :second, nil},
        {:duration, :microsecond}
      ]),
      gen all(precision <- integer(1..38), scale <- integer(-5..precision)) do
        {:decimal128, precision, scale}
      end,
      constant({:dictionary, :int8, :string})
    ])
  end

  defp logical_type(depth) do
    child = logical_type(depth - 1)

    one_of([
      logical_type(0),
      gen all(
            kind <- member_of([:list, :large_list]),
            type <- child,
            nullable <- boolean(),
            metadata <- metadata()
          ) do
        {kind, %LanceDB.Field{name: "item", type: type, nullable: nullable, metadata: metadata}}
      end,
      gen all(left <- child, right <- child, metadata <- metadata()) do
        {:struct, [%{name: :left, type: left, metadata: metadata}, {:right, right}]}
      end,
      map(child, &{:map, :string, &1})
    ])
  end

  # The oracle is constructed with the input, independently of either row codec.
  def typed_value(depth \\ 2)

  def typed_value(0) do
    one_of([
      map(text(), &{:string, &1, &1}),
      map(binary(max_length: 32), &{:binary, &1, &1}),
      map(int64(), &{:int64, &1, &1}),
      map(integer(0..18_446_744_073_709_551_615), &{:uint64, &1, &1}),
      map(boolean(), &{:boolean, &1, &1})
    ])
  end

  def typed_value(depth) do
    child = typed_value(depth - 1)

    one_of([
      typed_value(0),
      gen all({type, input, expected} <- child, count <- integer(0..4), nulls? <- boolean()) do
        inputs = List.duplicate(input, count)
        outputs = List.duplicate(expected, count)
        inputs = if nulls?, do: [nil | inputs], else: inputs
        outputs = if nulls?, do: [nil | outputs], else: outputs
        {{:list, type}, inputs, outputs}
      end,
      gen all(
            {left_type, left, expected_left} <- child,
            {right_type, right, expected_right} <- child
          ) do
        {{:struct, [left: left_type, right: right_type]}, %{left: left, right: right},
         %{"left" => expected_left, "right" => expected_right}}
      end
    ])
  end
end
