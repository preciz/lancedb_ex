# Run with plain Elixir so packaging does not compile or load the NIF.
# The library must be built for the supplied target using NIF ABI 2.15.
case System.argv() do
  [library, target, version, output] ->
    Version.parse!(version)

    if target not in ~w(x86_64-unknown-linux-gnu aarch64-unknown-linux-gnu aarch64-apple-darwin x86_64-apple-darwin) do
      raise ArgumentError, "unsupported precompiled target: #{target}"
    end

    name = "liblancedb_nif-v#{version}-nif-2.15-#{target}.so"
    archive = Path.join(output, name <> ".tar.gz")
    File.mkdir_p!(output)

    :ok =
      :erl_tar.create(
        String.to_charlist(archive),
        [{String.to_charlist(name), File.read!(library)}],
        [:compressed]
      )

    hash = :crypto.hash(:sha256, File.read!(archive)) |> Base.encode16(case: :lower)
    File.write!(archive <> ".sha256", "#{hash}  #{Path.basename(archive)}\n")
    IO.puts(archive)

  _ ->
    raise ArgumentError,
          "usage: elixir scripts/package_nif.exs LIBRARY TARGET VERSION OUTPUT_DIR"
end
