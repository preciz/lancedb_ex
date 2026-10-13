defmodule LanceDB.NativePackagingTest do
  use LanceDB.DataCase, async: true

  @version "0.0.0+aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
  @archive "liblancedb_nif-v#{@version}-nif-2.15-x86_64-unknown-linux-gnu.so.tar.gz"

  test "all platform archives contain the BEAM library name and join the checksum manifest", %{
    path: root
  } do
    File.mkdir_p!(root)

    targets =
      ~w(x86_64-unknown-linux-gnu aarch64-unknown-linux-gnu aarch64-apple-darwin x86_64-apple-darwin)

    for target <- targets do
      extension = if String.ends_with?(target, "darwin"), do: ".dylib", else: ".so"
      library = Path.join(root, "#{target}#{extension}")
      File.write!(library, target)
      assert {_, 0} = package(library, root, target)
      name = "liblancedb_nif-v#{@version}-nif-2.15-#{target}.so"

      assert {:ok, [{member, ^target}]} =
               :erl_tar.extract(String.to_charlist(Path.join(root, name <> ".tar.gz")), [
                 :memory,
                 :compressed
               ])

      assert List.to_string(member) == name
    end

    manifest = Path.join(root, "checksums.exs")
    assert {_, 0} = elixir_script("scripts/checksums.exs", [root, manifest])
    {checksums, _} = Code.eval_file(manifest)
    assert map_size(checksums) == length(targets)

    for {archive, checksum} <- checksums do
      expected = :crypto.hash(:sha256, File.read!(Path.join(root, archive)))
      assert checksum == "sha256:" <> Base.encode16(expected, case: :lower)
    end
  end

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

  defp package(library, output, target \\ "x86_64-unknown-linux-gnu"),
    do: elixir_script("scripts/package_nif.exs", [library, target, @version, output])

  defp elixir_script(script, args) do
    System.cmd(
      System.find_executable("elixir"),
      ["--erl", "+S 2:2", script | args],
      stderr_to_stdout: true
    )
  end
end
