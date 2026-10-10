# Generate a RustlerPrecompiled checksum manifest from verified build outputs.
# Do not run this against untrusted downloaded archives.
case System.argv() do
  [directory, output] ->
    archives = directory |> Path.join("*.so.tar.gz") |> Path.wildcard() |> Enum.sort()
    if archives == [], do: raise(ArgumentError, "no NIF archives found in #{directory}")

    entries =
      Enum.map(archives, fn archive ->
        hash = :crypto.hash(:sha256, File.read!(archive)) |> Base.encode16(case: :lower)
        "  #{inspect(Path.basename(archive))} => #{inspect("sha256:" <> hash)},\n"
      end)

    File.write!(output, ["%{\n", entries, "}\n"])
    IO.puts(output)

  _ ->
    raise ArgumentError, "usage: elixir scripts/checksums.exs ARCHIVE_DIR OUTPUT_FILE"
end
