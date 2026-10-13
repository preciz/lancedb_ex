# Maintainer/CI tool; never executed by consumer installation.
case System.argv() do
  [revision, repository, output] ->
    if !Regex.match?(~r/\A[0-9a-f]{40}\z/, revision),
      do: raise(ArgumentError, "source revision must be a full Git commit SHA")

    build = %{
      id: "0.0.0+" <> revision,
      source_revision: revision,
      sdk_version: "0.40.0",
      repository: repository,
      tag: "native-" <> revision,
      default_url: "https://github.com/#{repository}/releases/download/native-#{revision}"
    }

    File.write!(output, inspect(build, pretty: true, limit: :infinity) <> "\n")

  _ ->
    raise ArgumentError,
          "usage: elixir scripts/native_build.exs SOURCE_REVISION REPOSITORY OUTPUT"
end
