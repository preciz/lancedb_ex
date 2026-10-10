defmodule LanceDB.SchedulerTest do
  use LanceDB.DataCase, async: true

  @tag timeout: 30_000
  test "held storage cannot occupy the only dirty I/O scheduler", %{path: path} do
    {output, status} =
      System.cmd(
        System.find_executable("elixir"),
        [
          "--erl",
          "+S 2:2 +SDcpu 2:2 +SDio 1",
          "-pa",
          Path.join(Mix.Project.build_path(), "lib/*/ebin"),
          "test/fixtures/cursor_schedulers.ex",
          path
        ],
        stderr_to_stdout: true
      )

    assert status == 0, output
    assert output =~ "scheduler remains available; timeout delivered; storage read completed"
  end
end
