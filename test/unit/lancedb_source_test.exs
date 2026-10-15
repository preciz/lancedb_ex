defmodule LanceDB.SourceTest do
  use ExUnit.Case, async: true
  use ExUnitProperties
  alias LanceDB.{Error, Source}
  alias LanceDB.Ingestion.Source, as: Producer

  property "row sources stay lazy, run in their caller and preserve every row across chunks" do
    check all(ids <- list_of(integer(), max_length: 30), size <- integer(1..10)) do
      owner = self()
      tag = make_ref()

      input =
        Stream.map(ids, fn id ->
          send(owner, {tag, self(), id})
          %{id: id}
        end)

      source = Source.rows(input, batch_size: size)
      chunks = Source.chunks(source)
      refute_received {^tag, _, _}
      actual = Enum.to_list(chunks)
      assert List.flatten(actual) == Enum.map(ids, &%{id: &1})
      assert Enum.all?(actual, &(length(&1) in 1..size))
      for id <- ids, do: assert_received({^tag, ^owner, ^id})
      refute_received {^tag, _, _}
    end
  end

  property "halting a producer closes it once without demanding extra input" do
    check all(count <- integer(1..30), take <- integer(1..count)) do
      tag = make_ref()
      owner = self()

      source =
        Stream.resource(
          fn -> 0 end,
          fn index ->
            send(owner, {tag, :demand, index})
            {[index], index + 1}
          end,
          fn _ -> send(owner, {tag, :closed}) end
        )

      refute_received {^tag, _, _}

      assert {values, nil} =
               Producer.reduce_while(source, [], fn {:ok, value}, values ->
                 values = [value | values]
                 if value == take - 1, do: {:halt, values}, else: {:cont, values}
               end)

      assert Enum.reverse(values) == Enum.to_list(0..(take - 1))
      for index <- 0..(take - 1), do: assert_received({^tag, :demand, ^index})
      refute_received {^tag, :demand, _}
      assert_received {^tag, :closed}
      refute_received {^tag, :closed}
    end
  end

  property "producer failures preserve earlier values and stop consumption" do
    check all(
            prefix <- list_of(integer(), max_length: 8),
            kind <- member_of([:raise, :throw, :exit])
          ) do
      tag = make_ref()
      owner = self()

      source =
        Stream.map(prefix ++ [:fail, :unreachable], fn
          :fail -> fail(kind)
          :unreachable -> send(owner, {tag, :unexpected})
          value -> value
        end)

      assert {result, nil} =
               Producer.reduce_while(source, [], fn item, items -> {:cont, [item | items]} end)

      result = Enum.reverse(result)
      assert Enum.take(result, length(prefix)) == Enum.map(prefix, &{:ok, &1})

      assert [{:error, %Error{kind: :source, details: %{phase: :input}}}] =
               Enum.drop(result, length(prefix))

      refute_received {^tag, :unexpected}
    end
  end

  property "cleanup failures remain separate from the consumer result" do
    check all(kind <- member_of([:raise, :throw, :exit])) do
      tag = make_ref()
      owner = self()

      source =
        Stream.resource(
          fn -> 0 end,
          fn index -> {[index], index + 1} end,
          fn _ ->
            send(owner, {tag, :closed})
            fail(kind)
          end
        )

      assert {:primary_failure, %Error{kind: :source, details: %{phase: :cleanup}}} =
               Producer.reduce_while(source, nil, fn _, _ -> {:halt, :primary_failure} end)

      assert_received {^tag, :closed}
      refute_received {^tag, :closed}
    end
  end

  test "consumer exceptions close the producer and keep their original exception" do
    tag = make_ref()
    owner = self()

    source =
      Stream.resource(
        fn -> nil end,
        fn state -> {[1], state} end,
        fn _ ->
          send(owner, {tag, :closed})
          raise "cleanup failed"
        end
      )

    assert_raise ArgumentError, "consumer failed", fn ->
      Producer.reduce_while(source, nil, fn _, _ -> raise ArgumentError, "consumer failed" end)
    end

    assert_received {^tag, :closed}
    refute_received {^tag, :closed}
  end

  test "structured database failures keep their kind and details during source consumption" do
    for kind <- [:busy, :storage, :invalid_data] do
      error = %Error{
        kind: kind,
        operation: :stream_next_batch,
        message: "snapshot read failed",
        details: %{phase: :execution, context: "original"}
      }

      source = Stream.map([:fail], fn _ -> raise error end)

      assert {[{:error, ^error}], nil} =
               Producer.reduce_while(source, [], fn item, items -> {:cont, [item | items]} end)
    end
  end

  test "structured cleanup failures stay separate and retain their original details" do
    for kind <- [:busy, :storage, :invalid_data] do
      error = %Error{
        kind: kind,
        operation: :stream_close,
        message: "cleanup failed",
        details: %{phase: :execution, context: "original"}
      }

      source =
        Stream.resource(
          fn -> nil end,
          fn state -> {[1], state} end,
          fn _ -> raise error end
        )

      assert {:primary_failure, ^error} =
               Producer.reduce_while(source, nil, fn _, _ -> {:halt, :primary_failure} end)
    end
  end

  defp fail(:raise), do: raise("producer failed")
  defp fail(:throw), do: throw(:producer_failed)
  defp fail(:exit), do: exit(:producer_failed)
end
