defmodule LanceDB.NativeFreshnessTest do
  use LanceDB.DataCase, async: true

  @script Path.expand("../../scripts/check_native_build.exs", __DIR__)
  @inputs ~w(native/lancedb_nif/src/lib.rs native/lancedb_nif/Cargo.toml Cargo.toml Cargo.lock rust-toolchain.toml .github/workflows/precompile.yml)

  setup %{path: root} do
    File.mkdir_p!(root)
    git!(root, ["init", "--quiet"])

    for path <- @inputs do
      destination = Path.join(root, path)
      File.mkdir_p!(Path.dirname(destination))
      File.write!(destination, "original input\n")
    end

    commit!(root)
    revision = revision(root)
    select_build!(root, revision)
    commit!(root)
    %{root: root}
  end

  test "matching native inputs pass across Elixir and documentation commits", %{root: root} do
    assert {output, 0} = check(root)
    assert output =~ "Committed native inputs match selected build"

    for path <- ~w(lib/lancedb/connection.ex docs/native-builds.md) do
      destination = Path.join(root, path)
      File.mkdir_p!(Path.dirname(destination))
      File.write!(destination, "Elixir or documentation change\n")
    end

    commit!(root)
    assert {_, 0} = check(root)
  end

  for path <- @inputs do
    test "committed changes to #{path} fail until a matching build is selected", %{root: root} do
      path = unquote(path)
      File.write!(Path.join(root, path), "changed native input\n")
      commit!(root)

      assert {output, 1} = check(root)
      assert output =~ "is stale"
      assert output =~ path
      assert output =~ "checksum manifest together"

      select_build!(root, revision(root))
      commit!(root)
      assert {_, 0} = check(root)
    end
  end

  test "added and deleted native files require a new build", %{root: root} do
    File.write!(Path.join(root, "native/lancedb_nif/src/new.rs"), "new input\n")
    File.rm!(Path.join(root, "native/lancedb_nif/src/lib.rs"))
    commit!(root)

    assert {output, 1} = check(root)
    assert output =~ "native/lancedb_nif/src/new.rs"
    assert output =~ "native/lancedb_nif/src/lib.rs"
  end

  test "invalid revisions fail before Git comparison", %{root: root} do
    for revision <- [nil, "main", String.duplicate("g", 40)] do
      select_build!(root, revision)
      assert {output, 1} = check(root)
      assert output =~ "must select a full immutable source revision"
    end
  end

  test "unavailable revisions fail instead of skipping the comparison", %{root: root} do
    select_build!(root, String.duplicate("a", 40))
    assert {output, 1} = check(root)
    assert output =~ "is unavailable"
    assert output =~ "fetch full Git history"
  end

  test "a shallow checkout needs the selected historical commit", %{root: root} do
    shallow = Path.join(root, "shallow")
    git!(root, ["clone", "--quiet", "--depth", "1", "file://#{root}", shallow])
    assert {output, 1} = check(shallow)
    assert output =~ "fetch full Git history"
    git!(shallow, ["fetch", "--quiet", "--unshallow"])
    assert {_, 0} = check(shallow)
  end

  defp select_build!(root, revision),
    do: File.write!(Path.join(root, "native-build.exs"), inspect(%{source_revision: revision}))

  defp commit!(root) do
    git!(root, ["add", "."])
    git!(root, ["commit", "--quiet", "-m", "Fixture change"])
  end

  defp revision(root), do: git!(root, ["rev-parse", "HEAD"]) |> String.trim()

  defp git!(root, args) do
    {output, status} =
      System.cmd(
        "git",
        [
          "-c",
          "user.name=Native build fixture",
          "-c",
          "user.email=fixture@example.invalid",
          "-c",
          "commit.gpgsign=false",
          "-c",
          "core.hooksPath=/dev/null" | args
        ],
        cd: root,
        stderr_to_stdout: true
      )

    assert status == 0, output
    output
  end

  defp check(root) do
    System.cmd(System.find_executable("elixir"), ["--erl", "+S 2:2", @script],
      cd: root,
      stderr_to_stdout: true
    )
  end
end
