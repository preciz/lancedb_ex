defmodule LanceDB.NativePackagingTest do
  use LanceDB.DataCase, async: true

  @version "0.0.0+aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
  @archive "liblancedb_nif-v#{@version}-nif-2.15-x86_64-unknown-linux-gnu.so.tar.gz"

  test "packaging preserves existing archives and checksum sidecars", %{path: root} do
    File.mkdir_p!(root)
    library = Path.join(root, "library")
    archive = Path.join(root, @archive)
    File.write!(library, "original library")
    {_, 0} = package(library, root)
    original = File.read!(archive)
    checksum = File.read!(archive <> ".sha256")

    File.write!(library, "replacement library")
    {output, status} = package(library, root)
    assert status != 0
    assert output =~ "Refusing to replace native build artifact"
    assert File.read!(archive) == original
    assert File.read!(archive <> ".sha256") == checksum

    File.rm!(archive)
    {output, status} = package(library, root)
    assert status != 0
    assert output =~ "Refusing to replace native build artifact"
    refute File.exists?(archive)
    assert File.read!(archive <> ".sha256") == checksum
  end

  test "concurrent packagers publish one complete archive with its matching checksum", %{
    path: root
  } do
    File.mkdir_p!(root)

    tasks =
      for content <- ["first library", "second library"] do
        library = Path.join(root, content)
        File.write!(library, content)
        Task.async(fn -> package(library, root) end)
      end

    results = Enum.map(tasks, &Task.await(&1, 5_000))
    assert Enum.count(results, fn {_output, status} -> status == 0 end) == 1
    archive = Path.join(root, @archive)

    assert {:ok, [{_name, contents}]} =
             :erl_tar.extract(String.to_charlist(archive), [:memory, :compressed])

    assert contents in ["first library", "second library"]
    hash = :crypto.hash(:sha256, File.read!(archive)) |> Base.encode16(case: :lower)
    assert File.read!(archive <> ".sha256") == "#{hash}  #{@archive}\n"
    assert Path.wildcard(Path.join(root, "*.tmp")) == []
  end

  defp package(library, output) do
    System.cmd(
      System.find_executable("elixir"),
      [
        "--erl",
        "+S 2:2",
        "scripts/package_nif.exs",
        library,
        "x86_64-unknown-linux-gnu",
        @version,
        output
      ],
      stderr_to_stdout: true
    )
  end
end
