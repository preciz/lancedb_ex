defmodule LanceDB.SchedulerTest do
  use LanceDB.DataCase, async: true

  @tag timeout: 30_000
  test "held storage cannot occupy the only dirty I/O scheduler", %{path: path} do
    {output, status} = run_fixture("cursor_schedulers.ex", path)
    assert status == 0, output
    assert output =~ "scheduler remains available; timeout delivered; storage read completed"
  end

  @tag timeout: 30_000
  test "job waiters cannot delay status, deadlines, or cancellation", %{path: path} do
    {output, status} = run_fixture("job_schedulers.ex", path)
    assert status == 0, output
    assert output =~ "job waiters yield schedulers; deadlines and cancellation remain available"
  end

  defp run_fixture(fixture, path) do
    System.cmd(
      System.find_executable("elixir"),
      [
        "--erl",
        "+S 2:2 +SDcpu 2:2 +SDio 1",
        "-pa",
        Path.join(Mix.Project.build_path(), "lib/*/ebin"),
        Path.join("test/fixtures", fixture),
        path
      ],
      stderr_to_stdout: true
    )
  end
end
