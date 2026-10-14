# Run with plain Elixir so packaging does not compile or load the NIF.
# The library must be built for the supplied target using NIF ABI 2.15.
case System.argv() do
  [library, target, version, output] ->
    Version.parse!(version)

    if target not in ~w(x86_64-unknown-linux-gnu aarch64-unknown-linux-gnu aarch64-apple-darwin) do
      raise ArgumentError, "unsupported precompiled target: #{target}"
    end

    # RustlerPrecompiled and BEAM use .so for NIFs on macOS as well as Linux.
    # Cargo's macOS input library is a .dylib; the archive member is renamed.
    name = "liblancedb_nif-v#{version}-nif-2.15-#{target}.so"
    archive = Path.join(output, name <> ".tar.gz")
    File.mkdir_p!(output)

    if File.exists?(archive) or File.exists?(archive <> ".sha256"),
      do: raise(ArgumentError, "Refusing to replace native build artifact: #{archive}")

    temporary = archive <> ".#{System.pid()}.#{System.unique_integer([:positive])}.tmp"

    try do
      :ok =
        :erl_tar.create(
          String.to_charlist(temporary),
          [{String.to_charlist(name), File.read!(library)}],
          [:compressed]
        )

      contents = File.read!(temporary)
      # Exclusive creation also protects against two packagers racing for an ID.
      File.write!(archive, contents, [:exclusive])
      hash = :crypto.hash(:sha256, contents) |> Base.encode16(case: :lower)
      File.write!(archive <> ".sha256", "#{hash}  #{Path.basename(archive)}\n", [:exclusive])
    after
      File.rm(temporary)
    end

    IO.puts(archive)

  _ ->
    raise ArgumentError,
          "usage: elixir scripts/package_nif.exs LIBRARY TARGET VERSION OUTPUT_DIR"
end
