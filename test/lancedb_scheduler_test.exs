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
  test "connect and open yield schedulers and release admission on cancellation", %{path: path} do
    {output, status} = run_fixture("open_schedulers.ex", path, 1, 1)
    assert status == 0, output

    assert output =~
             "connect and open yield schedulers; deadlines and caller death release admission"
  end

  @tag timeout: 45_000
  test "writer finish and abort yield schedulers until SDK settlement", %{path: path} do
    {output, status} = run_fixture("writer_schedulers.ex", path, 1, 1)
    assert status == 0, output
    assert output =~ "writer finish and abort yield schedulers and await SDK settlement"
  end

  @tag timeout: 45_000
  test "storage requests yield both scheduler pools and preserve admission lifetimes", %{
    path: path
  } do
    {output, status} = run_fixture("metadata_schedulers.ex", path, 1, 1)
    assert status == 0, output

    assert output =~
             "held storage yields both dirty scheduler pools; read cancellation and write settlement hold"
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
