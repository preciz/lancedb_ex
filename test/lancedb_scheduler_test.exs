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

  @tag timeout: 45_000
  test "held history and branch storage leaves dirty CPU work available", %{path: path} do
    {output, status} = run_fixture("metadata_schedulers.ex", path, 1, 2)
    assert status == 0, output
    assert output =~ "history and branch storage leaves dirty CPU work available"
  end

  defp run_fixture(fixture, path, cpu_schedulers \\ 2, io_schedulers \\ 1) do
    System.cmd(
      System.find_executable("elixir"),
      [
        "--erl",
        "+S 2:2 +SDcpu #{cpu_schedulers}:#{cpu_schedulers} +SDio #{io_schedulers}",
        "-pa",
        Path.join(Mix.Project.build_path(), "lib/*/ebin"),
        Path.join("test/fixtures", fixture),
        path
      ],
      stderr_to_stdout: true
    )
  end
end
