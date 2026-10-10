defmodule LanceDB.NativeSafetyTest do
  use LanceDB.DataCase, async: true
  use ExUnitProperties
  alias LanceDB.{Batch, Connection, Error, History, Query, Schema, Table}

  property "deep malformed lists and maps are rejected without committing rows" do
    check all(
            depth <- integer(65..512),
            shape <- member_of([:list, :map, :mixed]),
            max_runs: 15
          ) do
      with_database(fn conn ->
        schema = Schema.new(id: :int64)
        assert {:ok, table} = Connection.create_empty_table(conn, "rows", schema)
        assert {:ok, before} = History.version(table)
        value = nested(depth, shape)

        for result <- [Batch.from_rows([%{id: value}], schema), Table.add(table, [%{id: value}])] do
          assert {:error, %Error{kind: :invalid_data, message: message}} = result
          assert message =~ "maximum depth of 64"
        end

        assert {:ok, ^before} = History.version(table)
        assert {:ok, []} = Query.query(table)
      end)
    end
  end

  test "ten thousand nested containers return errors in an isolated BEAM", %{path: path} do
    code = """
    {:ok, conn} = LanceDB.connect(#{inspect(path)})
    schema = LanceDB.Schema.new(id: :int64)
    {:ok, table} = LanceDB.Connection.create_empty_table(conn, "rows", schema)
    for shape <- [:list, :map] do
      value = Enum.reduce(1..10_000, 0, fn _, value ->
        if shape == :list, do: [value], else: %{"child" => value}
      end)
      for result <- [LanceDB.Batch.from_rows([%{id: value}], schema),
                     LanceDB.Table.add(table, [%{id: value}])] do
        case result do
          {:error, %LanceDB.Error{kind: :invalid_data, message: message}} ->
            true = String.contains?(message, "maximum depth of 64")
          unexpected -> raise inspect(unexpected)
        end
      end
    end
    {:ok, 0} = LanceDB.Table.count_rows(table)
    IO.puts("deep inputs rejected; VM survived")
    """

    {output, status} =
      System.cmd(
        System.find_executable("elixir"),
        ["--erl", "+S 2:2", "-pa", Path.join(Mix.Project.build_path(), "lib/*/ebin"), "-e", code],
        stderr_to_stdout: true,
        env: [{"ERL_CRASH_DUMP", Path.join(path, "erl_crash.dump")}]
      )

    assert status == 0, output
    assert output =~ "deep inputs rejected; VM survived"
  end

  defp nested(depth, shape) do
    Enum.reduce(1..depth, 0, fn index, value ->
      if shape == :list or (shape == :mixed and rem(index, 2) == 0),
        do: [value],
        else: %{"child" => value}
    end)
  end
end
