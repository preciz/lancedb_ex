defmodule LanceDB.NativeExtractionTest do
  use LanceDB.DataCase, async: true

  test "cached and downloaded archives preserve an already open native library", %{path: root} do
    File.mkdir_p!(root)

    File.write!(Path.join(root, "mix.exs"), """
    defmodule NativeExtraction.MixProject do
      use Mix.Project
      def project, do: [app: :native_extraction, version: "0.0.0"]
    end
    """)

    # A child VM isolates Mix's project, code paths, HTTP settings and cache.
    # An open descriptor observes the same inode that a loaded NIF would map.
    code = """
    Mix.start()
    Code.require_file("mix.exs")
    Mix.Project.build_structure()
    Code.prepend_path(Mix.Project.compile_path())
    config = RustlerPrecompiled.Config.new(
      module: NativeExtraction.NIF, otp_app: :native_extraction,
      version: "0.0.0", force_build: false, base_url: "http://127.0.0.1/",
      targets: ~w(x86_64-unknown-linux-gnu aarch64-unknown-linux-gnu aarch64-apple-darwin),
      nif_versions: ["2.15"], max_retries: 0
    )
    {:ok, metadata} = RustlerPrecompiled.build_metadata(config)
    name = String.replace_suffix(metadata.file_name, ".tar.gz", "")
    native = Application.app_dir(:native_extraction, "priv/native")
    File.mkdir_p!(native)
    File.mkdir_p!("archives")
    archive = Path.join("archives", metadata.file_name)
    :ok = :erl_tar.create(String.to_charlist(archive), [{String.to_charlist(name), "replacement library"}], [:compressed])
    hash = :crypto.hash(:sha256, File.read!(archive)) |> Base.encode16(case: :lower)
    File.write!("checksum-Elixir.NativeExtraction.NIF.exs", inspect(%{metadata.file_name => "sha256:" <> hash}))
    {:ok, server} = LanceDB.ObjectStoreFixture.start_link("archives")
    config = %{config | base_url: {"http://127.0.0.1:" <> to_string(LanceDB.ObjectStoreFixture.port(server)) <> "/probe/", []}}
    for source <- [:download, :cache] do
      library = Path.join(native, name)
      File.write!(library, "original library")
      {:ok, existing} = File.open(library, [:read, :binary])
      try do
        {:ok, _} = RustlerPrecompiled.download_or_reuse_nif_file(config, metadata)
        "replacement library" = File.read!(library)
        "original library" = IO.binread(existing, :eof)
        IO.puts("preserved loaded library during " <> to_string(source))
      after
        File.close(existing)
      end
    end
    """

    {output, status} =
      System.cmd(
        System.find_executable("elixir"),
        [
          "--erl",
          "+S 2:2",
          "-pa",
          Path.expand(Path.join(Mix.Project.build_path(), "lib/*/ebin")),
          "-e",
          code
        ],
        cd: root,
        env: [
          {"RUSTLER_PRECOMPILED_GLOBAL_CACHE_PATH", Path.join(root, "cache")},
          {"TARGET_ARCH", nil},
          {"TARGET_OS", nil},
          {"TARGET_ABI", nil},
          {"TARGET_VENDOR", nil},
          {"HTTP_PROXY", nil},
          {"http_proxy", nil},
          {"HTTPS_PROXY", nil},
          {"https_proxy", nil}
        ],
        stderr_to_stdout: true
      )

    assert status == 0, output
    assert output =~ "preserved loaded library during download"
    assert output =~ "preserved loaded library during cache"
  end
end
