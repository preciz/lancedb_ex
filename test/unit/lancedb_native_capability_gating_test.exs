defmodule LanceDB.NativeCapabilityGatingTest do
  use ExUnit.Case, async: true

  test "matching native builds enable preparation checks in both build modes" do
    for source_build <- [nil, "true"] do
      {output, status} = helper(:available, source_build)
      assert status == 0, output
      assert output =~ "preparation enabled"
      assert output =~ "optimization enabled"
    end
  end

  test "missing implementations skip old artifacts but fail source verification" do
    {output, status} = helper(:missing, nil)
    assert status == 0, output
    assert output =~ "preparation skipped"

    {output, status} = helper(:missing, "true")
    assert status != 0
    assert output =~ "Source build is missing FTS preparation"
  end

  test "unexpected preparation failures are never classified as missing support" do
    {output, status} = helper(:broken, nil)
    assert status != 0
    assert output =~ "Unexpected preparation failure"
  end

  test "missing optimization skips only its checks and fails source verification" do
    {output, status} = helper(:available, nil, :missing)
    assert status == 0, output
    assert output =~ "preparation enabled"
    assert output =~ "optimization skipped"
    {output, status} = helper(:available, "true", :missing)
    assert status != 0
    assert output =~ "Source build is missing standard optimization"
  end

  test "unexpected optimization errors fail verification" do
    {output, status} = helper(:available, nil, :broken)
    assert status != 0
    assert output =~ "Unexpected optimization failure"
  end

  defp helper(support, source_build, optimization \\ :available) do
    code = """
    Code.compiler_options(ignore_module_conflict: true)
    defmodule LanceDB.FTS do
      def prepare("") do
        case #{inspect(support)} do
          :available -> {:ok, :prepared}
          :missing -> :erlang.nif_error(:nif_not_loaded)
          :broken -> raise "Unexpected preparation failure"
        end
      end
    end
    defmodule LanceDB.Native do
      def optimize(nil, _reference) do
        case #{inspect(optimization)} do
          :available -> :erlang.error(:badarg)
          :missing -> :erlang.nif_error(:nif_not_loaded)
          :broken -> raise "Unexpected optimization failure"
        end
      end
    end
    Code.require_file("test/test_helper.exs")
    excluded = ExUnit.configuration()[:exclude]
    if :fts_preparation in excluded do
      IO.puts("preparation skipped")
    else
      IO.puts("preparation enabled")
    end
    if :standard_optimization in excluded do
      IO.puts("optimization skipped")
    else
      IO.puts("optimization enabled")
    end
    """

    System.cmd(
      System.find_executable("elixir"),
      ["--erl", "+S 2:2", "-pa", Path.join(Mix.Project.build_path(), "lib/*/ebin"), "-e", code],
      env: [{"LANCEDB_BUILD", source_build}],
      stderr_to_stdout: true
    )
  end
end
