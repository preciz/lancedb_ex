defmodule LanceDB.MixProject do
  use Mix.Project

  @force_build? System.get_env("LANCEDB_BUILD") in ["1", "true"]
  @version "0.1.0"
  @source_url "https://github.com/preciz/lancedb_ex"

  def project do
    [
      app: :lancedb,
      version: @version,
      elixir: "~> 1.20",
      description: "Embedded LanceDB vector search for Elixir, powered by Rustler.",
      name: "LanceDB",
      source_url: @source_url,
      homepage_url: @source_url,
      docs: [
        main: "readme",
        extras:
          ["README.md"] ++
            Enum.map(Path.wildcard("docs/*.md"), fn
              "docs/search.md" -> {"docs/search.md", filename: "search-guide"}
              path -> path
            end),
        source_ref: "v#{@version}"
      ],
      elixirc_paths: elixirc_paths(Mix.env()),
      start_permanent: Mix.env() == :prod,
      test_ignore_filters: [~r{test/fixtures/}],
      test_coverage: [
        summary: [threshold: 90],
        # Rust replaces these NIF fallback stubs; BEAM coverage cannot measure it.
        ignore_modules: [
          LanceDB.Native,
          LanceDB.TestSupport,
          LanceDB.DataCase,
          LanceDB.Generators,
          LanceDB.ObjectStoreFixture
        ]
      ],
      deps: [
        {:telemetry, "~> 1.4"},
        {:rustler_precompiled, "~> 0.10.0", runtime: false},
        {:rustler, "~> 0.38.0", optional: not @force_build?, runtime: false},
        {:stream_data, "~> 1.4", only: :test},
        {:credo, "~> 1.7.19", only: [:dev, :test], runtime: false},
        {:ex_doc, "~> 0.40.4", only: :dev, runtime: false}
      ],
      package: [
        name: "lancedb_ex",
        licenses: ["MIT"],
        links: %{"GitHub" => @source_url},
        files:
          ~w(lib docs native Cargo.toml Cargo.lock mix.exs README.md LICENSE .formatter.exs native-build.exs checksum-*.exs)
      ]
    ]
  end

  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_), do: ["lib"]

  def application, do: [extra_applications: [:logger]]
end
