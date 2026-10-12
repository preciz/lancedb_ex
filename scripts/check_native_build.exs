# Run with plain Elixir before dependency resolution or native compilation.
# Compare committed inputs: local, uncommitted changes are not release inputs.
{build, _binding} = Code.eval_file("native-build.exs")
revision = Map.get(build, :source_revision)

if !(is_binary(revision) and Regex.match?(~r/\A[0-9a-f]{40}\z/, revision)) do
  IO.puts(:stderr, "native-build.exs must select a full immutable source revision")
  System.halt(1)
end

case System.cmd("git", ["cat-file", "-e", "#{revision}^{commit}"], stderr_to_stdout: true) do
  {_, 0} ->
    :ok

  {output, _} ->
    IO.puts(:stderr, "Selected native source #{revision} is unavailable; fetch full Git history.")
    IO.puts(:stderr, output)
    System.halt(1)
end

inputs = ~w(native Cargo.toml Cargo.lock rust-toolchain.toml .github/workflows/precompile.yml)

case System.cmd("git", ["diff", "--exit-code", "--name-only", revision, "HEAD", "--" | inputs],
       stderr_to_stdout: true
     ) do
  {_, 0} ->
    IO.puts("Committed native inputs match selected build #{revision}.")

  {changed, 1} ->
    IO.puts(:stderr, "Selected precompiled build #{revision} is stale. Changed native inputs:")
    IO.puts(:stderr, changed)

    IO.puts(
      :stderr,
      "Publish and verify a new immutable native build, then update native-build.exs and its checksum manifest together. See docs/native-builds.md."
    )

    System.halt(1)

  {output, _} ->
    IO.puts(:stderr, "Could not compare the selected native build with HEAD:\n#{output}")
    System.halt(1)
end
