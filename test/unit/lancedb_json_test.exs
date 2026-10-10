defmodule LanceDB.JSONTest do
  use ExUnit.Case, async: true
  use ExUnitProperties
  alias LanceDB.Generators, as: G
  alias LanceDB.JSON, as: J

  property "recursive JSON normalization agrees with standard JSON and is idempotent" do
    check all(value <- G.json_value()) do
      expected = value |> JSON.encode!() |> JSON.decode!()
      assert %J{value: ^expected} = J.new(value)
      assert J.new(expected) == J.new(value)
      assert value |> J.encode!() |> JSON.decode!() == expected
    end
  end

  property "invalid leaves are rejected at every nesting level" do
    invalid = member_of([<<255>>, %URI{}, {:tuple, 1}, 18_446_744_073_709_551_616])

    check all(value <- invalid, depth <- integer(0..4)) do
      nested =
        Enum.reduce(List.duplicate(:wrap, depth), value, fn _, child -> %{valid: [child]} end)

      assert_raise ArgumentError, fn -> J.new(nested) end
      assert_raise ArgumentError, fn -> J.encode!(nested) end
    end
  end

  property "atom and string keys normalize recursively and duplicate aliases are rejected" do
    check all(left <- G.json_value(), right <- G.json_value()) do
      expected = %{"name" => [%{"雪" => left}, right]}
      assert %J{value: ^expected} = J.new(%{name: [%{雪: left}, right]})

      for duplicate <- [%{"name" => right, name: left}, %{"雪" => right, 雪: left}] do
        assert_raise ArgumentError, ~r/unique/, fn -> J.new(%{outer: [duplicate]}) end
      end
    end
  end
end
