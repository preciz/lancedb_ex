defmodule LanceDB.NativeSourceBuildTest do
  use LanceDB.DataCase, async: true
  @moduletag :source_build

  setup %{path: root} do
    File.mkdir_p!(Path.join(root, "lib/lancedb"))
    File.mkdir_p!(Path.join(root, "native/lancedb_nif/src"))

    File.write!(Path.join(root, "mix.exs"), """
    defmodule Fixture.MixProject do
      use Mix.Project
      def project, do: [app: :lancedb_ex, version: "0.0.0", elixir: "~> 1.20"]
      def application, do: [extra_applications: [:logger]]
    end
    """)

    File.write!(Path.join(root, "Cargo.toml"), """
    [workspace]
    resolver = "2"
    members = ["native/lancedb_nif"]
    """)

    File.write!(Path.join(root, "native/lancedb_nif/Cargo.toml"), """
    [package]
    name = "lancedb_nif"
    version = "0.0.0"
    edition = "2021"
    [lib]
    crate-type = ["cdylib"]
    """)

    File.write!(Path.join(root, "native/lancedb_nif/src/lib.rs"), """
    #[no_mangle]
    pub extern "C" fn fixture_value() -> i32 { 42 }
    """)

    File.write!(Path.join(root, "native-build.exs"), "%{id: \"0.0.0\", default_url: \"\"}\n")

    paths =
      for dependency <- ~w(rustler rustler_precompiled jason),
          do: Path.expand(Path.join([Mix.Project.build_path(), "lib", dependency, "ebin"]))

    # Exercise the package's actual guard and resource declarations with a tiny
    # crate. lib: false compiles Rust without trying to load this fixture as a NIF.
    native = File.read!("lib/lancedb/native.ex")

    native =
      String.replace(native, "crate: \"lancedb_nif\",", "crate: \"lancedb_nif\", lib: false,")

    native = "Code.prepend_paths(#{inspect(paths)})\n" <> native
    File.write!(Path.join(root, "lib/lancedb/native.ex"), native)

    assert {_, 0} = System.cmd("cargo", ["generate-lockfile", "--offline"], cd: root)
    %{root: root}
  end

  test "lockfile and workspace manifest edits trigger incremental native compilation", %{
    root: root
  } do
    assert {output, 0} = compile(root)
    assert output =~ "Compiling crate lancedb_nif"
    assert {output, 0} = compile(root)
    refute output =~ "Compiling crate lancedb_nif"

    for file <- ["Cargo.lock", "Cargo.toml"] do
      # Mix compares filesystem timestamps when checking external resources.
      Process.sleep(1_100)
      File.write!(Path.join(root, file), "\n# incremental input change\n", [:append])
      assert {output, 0} = compile(root)
      assert output =~ "Compiling crate lancedb_nif"
    end
  end

  test "a stale lockfile fails before Rustler can rewrite it", %{root: root} do
    lock = File.read!(Path.join(root, "Cargo.lock"))
    File.mkdir_p!(Path.join(root, "helper/src"))
    File.write!(Path.join(root, "helper/src/lib.rs"), "pub fn value() -> i32 { 1 }\n")

    File.write!(Path.join(root, "helper/Cargo.toml"), """
    [package]
    name = "helper"
    version = "0.0.0"
    edition = "2021"
    """)

    File.write!(
      Path.join(root, "native/lancedb_nif/Cargo.toml"),
      """
      [dependencies]
      helper = { path = "../../helper" }
      """,
      [:append]
    )

    {output, status} = compile(root)
    assert status != 0
    assert output =~ "maintainer build requires a current committed Cargo.lock"
    refute output =~ "Compiling crate lancedb_nif"
    assert File.read!(Path.join(root, "Cargo.lock")) == lock
  end

  defp compile(root) do
    System.cmd(System.find_executable("mix"), ["compile", "--warnings-as-errors"],
      cd: root,
      env: [{"LANCEDB_BUILD", "true"}, {"CARGO_NET_OFFLINE", "true"}, {"ERL_FLAGS", "+S 2:2"}],
      stderr_to_stdout: true
    )
  end
end
