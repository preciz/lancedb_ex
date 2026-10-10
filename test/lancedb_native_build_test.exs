defmodule LanceDB.NativeBuildTest do
  use ExUnit.Case, async: true

  setup do
    cache = Path.join(System.tmp_dir!(), "lancedb-nif-test-#{System.unique_integer([:positive])}")
    File.mkdir_p!(cache)
    on_exit(fn -> File.rm_rf!(cache) end)
    %{cache: cache}
  end

  test "empty artifact configuration fails without building Rust", %{cache: cache} do
    for build <- [nil, "false"] do
      {output, status} =
        compile_native(cache, [{"LANCEDB_BUILD", build}, {"LANCEDB_TEST_EMPTY_URL", "true"}])

      assert status != 0
      assert output =~ "LanceDB requires a precompiled NIF artifact URL"
      refute output =~ "Compiling crate"
    end
  end

  test "unsupported target fails without building Rust", %{cache: cache} do
    {output, status} = compile_native(cache, [{"TARGET_ARCH", "riscv64gc"}])
    assert status != 0
    assert output =~ "precompiled NIF is not available for this target"
    refute output =~ "Compiling crate"
  end

  test "unavailable download fails without building Rust", %{cache: cache} do
    {output, status} = compile_native(cache, [{"LANCEDB_NIF_BASE_URL", "http://127.0.0.1:1/"}])
    assert status != 0
    assert output =~ "Error while downloading precompiled NIF"
    refute output =~ "Compiling crate"
  end

  test "invalid cached checksum fails without building Rust", %{cache: cache} do
    {build, _} = Code.eval_file("native-build.exs")

    config =
      RustlerPrecompiled.Config.new(
        module: LanceDB.Native,
        otp_app: :lancedb_ex,
        crate: "lancedb_nif",
        base_url: build.default_url,
        version: build.id,
        force_build: false,
        targets:
          ~w(x86_64-unknown-linux-gnu aarch64-unknown-linux-gnu aarch64-apple-darwin x86_64-apple-darwin),
        nif_versions: ["2.15"]
      )

    {:ok, metadata} = RustlerPrecompiled.build_metadata(config)
    File.write!(Path.join(cache, metadata.file_name), "untrusted archive")
    {output, status} = compile_native(cache, [])
    assert status != 0
    assert output =~ "checksum of files does not match"
    refute output =~ "Compiling crate"
  end

  test "global force build still requires LanceDB maintainer opt-in", %{cache: cache} do
    {output, status} = compile_native(cache, [{"RUSTLER_PRECOMPILED_FORCE_BUILD_ALL", "true"}])
    assert status != 0
    assert output =~ "LANCEDB_BUILD=true"
    refute output =~ "Compiling crate"
  end

  test "native revision contributes to archive and cache identity" do
    configs =
      for revision <- [String.duplicate("a", 40), String.duplicate("b", 40)] do
        RustlerPrecompiled.Config.new(
          module: LanceDB.Native,
          otp_app: :lancedb_ex,
          crate: "lancedb_nif",
          base_url: "https://example.com/native-#{revision}",
          version: "0.0.0+#{revision}",
          force_build: false,
          targets:
            ~w(x86_64-unknown-linux-gnu aarch64-unknown-linux-gnu aarch64-apple-darwin x86_64-apple-darwin),
          nif_versions: ["2.15"]
        )
      end

    refute Enum.any?(configs, & &1.force_build?)
    [{:ok, first}, {:ok, second}] = Enum.map(configs, &RustlerPrecompiled.build_metadata/1)
    assert first.file_name != second.file_name
    assert first.cached_tar_gz != second.cached_tar_gz
    assert first.lib_name != second.lib_name
  end

  test "invalid, development and mismatched build IDs fail before invoking Rust", %{cache: cache} do
    {build, _} = Code.eval_file("native-build.exs")
    fixture = Path.join(cache, "package")
    File.mkdir_p!(Path.join(fixture, "lib/lancedb"))
    native = Path.join(fixture, "lib/lancedb/native.ex")
    File.cp!("lib/lancedb/native.ex", native)

    for id <- [build.source_revision, "0.1.0-dev", "0.0.0+" <> String.duplicate("a", 40)] do
      File.write!(Path.join(fixture, "native-build.exs"), inspect(%{build | id: id}))
      {output, status} = compile_native(cache, [], native)
      assert status != 0
      assert output =~ "native build ID must be 0.0.0+<full-source-revision>"
      refute output =~ "Compiling crate"
      refute output =~ "calling `cargo"
    end
  end

  defp compile_native(cache, overrides, native \\ "lib/lancedb/native.ex") do
    code = """
    Mix.start()
    Code.require_file("mix.exs")
    if System.get_env("LANCEDB_TEST_EMPTY_URL") == "true", do: Application.put_env(:lancedb_ex, :nif_base_url, "")
    Mix.Project.build_structure()
    Code.compile_file(#{inspect(native)})
    """

    env = [
      {"LANCEDB_BUILD", nil},
      {"LANCEDB_NIF_BASE_URL", nil},
      {"RUSTLER_PRECOMPILED_FORCE_BUILD_ALL", nil},
      {"RUSTLER_PRECOMPILED_GLOBAL_CACHE_PATH", cache},
      {"TARGET_ARCH", nil},
      {"TARGET_OS", nil},
      {"TARGET_ABI", nil},
      {"TARGET_VENDOR", nil}
    ]

    System.cmd(
      System.find_executable("elixir"),
      ["--erl", "+S 2:2", "-pa", Path.join(Mix.Project.build_path(), "lib/*/ebin"), "-e", code],
      env: Enum.into(overrides, Map.new(env)) |> Map.to_list(),
      stderr_to_stdout: true
    )
  end
end
