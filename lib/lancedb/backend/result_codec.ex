defmodule LanceDB.Backend.ResultCodec do
  @moduledoc false
  def decode({:ok, json}, module, cardinality) do
    value = JSON.decode!(json)

    {:ok,
     if(cardinality == :many, do: Enum.map(value, &value(&1, module)), else: value(value, module))}
  end

  def decode({:error, _} = error, _module, _cardinality), do: error

  defp value(wire, module) do
    fields = module.__struct__() |> Map.from_struct() |> Map.keys()
    struct!(module, Map.new(fields, &{&1, Map.fetch!(wire, to_string(&1))}))
  end
end
