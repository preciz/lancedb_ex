# Run from the release checkout with plain Elixir; no library compilation is needed.
defmodule LanceDB.Release do
  @moduledoc false
  @repository "preciz/lancedb_ex"
  @usage "Usage: elixir scripts/release.exs [--verify-only | --help]"

  @spec main([String.t()]) :: :ok
  def main(["--help"]), do: IO.puts(@usage)
  def main([]), do: release!(false)
  def main(["--verify-only"]), do: release!(true)
  def main(_), do: raise(ArgumentError, @usage)

  defp release!(verify_only?) do
    # Hex must keep Rustler optional, and docs must use the selected public NIF.
    Enum.each(
      ~w(LANCEDB_BUILD LANCEDB_NIF_BASE_URL RUSTLER_PRECOMPILED_FORCE_BUILD_ALL),
      &System.delete_env/1
    )

    System.put_env("MIX_ENV", "dev")
    root = command!("git", ~w(rev-parse --show-toplevel)) |> String.trim()
    File.cd!(root)
    clean_tree!()
    Mix.start()
    Code.require_file("mix.exs")
    config = Mix.Project.config()
    version = Keyword.fetch!(config, :version)
    parsed = Version.parse!(version)

    ensure!(
      parsed.pre == [] and parsed.build == nil,
      "Only stable package versions are supported."
    )

    ensure!(config[:app] == :lancedb_ex, "Run this script in the lancedb_ex release checkout.")

    tag = "v#{version}"
    revision = command!("git", ~w(rev-parse HEAD)) |> String.trim()
    tagged = command!("git", ["rev-parse", "#{tag}^{commit}"]) |> String.trim()
    ensure!(tagged == revision, "#{tag} must identify HEAD; never move an existing release tag.")
    notes = release_notes!(version, tag)
    command!("elixir", ["scripts/check_native_build.exs"])
    command!("gh", ~w(auth status))
    remote = github!("commits/#{tag}")
    ensure!(remote["sha"] == revision, "The GitHub tag #{tag} does not identify HEAD.")
    ci!(tag, revision, verify_only?)

    temporary =
      Path.join(
        System.tmp_dir!(),
        "lancedb-release-#{Base.url_encode64(:crypto.strong_rand_bytes(12), padding: false)}"
      )

    File.mkdir_p!(temporary)

    try do
      finish!(version, tag, revision, notes, temporary, verify_only?)
    after
      File.rm_rf!(temporary)
    end
  end

  defp finish!(version, tag, revision, notes, temporary, verify_only?) do
    clean_tree!(revision)
    expected = Path.join(temporary, "expected")
    command!("mix", ["hex.build", "--unpack", "--output", expected])
    clean_tree!(revision)
    expected_files = package_files(expected)

    {:ok, archived} =
      :erl_tar.extract({:binary, command!("git", ["archive", revision])}, [:memory])

    tagged_files = Map.new(archived, fn {path, bytes} -> {List.to_string(path), bytes} end)

    ensure!(
      Map.take(tagged_files, Map.keys(expected_files)) == expected_files,
      "Built package contains files that differ from the tagged source, including uncommitted or ignored files."
    )

    release = hex_release(version)

    if is_nil(release) do
      ensure!(!verify_only?, "Hex #{version} is not published.")
      interactive!("mix", ~w(hex.publish --yes))
    end

    release = hex_release(version)
    ensure!(!is_nil(release), "Hex #{version} is not available yet; rerun this command.")
    ensure!(is_nil(release["retirement"]), "Hex #{version} is retired.")
    ensure!(release["version"] == version, "Hex returned a different package version.")

    ensure!(
      get_in(release, ["requirements", "rustler", "optional"]) == true,
      "Hex must keep Rustler optional."
    )

    published = Path.join(temporary, "published")

    command!("mix", [
      "hex.package",
      "fetch",
      "lancedb_ex",
      version,
      "--unpack",
      "--output",
      published
    ])

    ensure!(
      expected_files == package_files(published),
      "Published Hex contents differ from the tagged source. Use a new version; do not replace this package."
    )

    IO.puts("Published Hex contents match #{tag}.")

    docs!(version, tag, release, verify_only?)
    interactive!("bash", ["scripts/validate_consumer.sh", published])
    clean_tree!(revision)

    ensure!(
      github!("commits/#{tag}")["sha"] == revision,
      "The GitHub tag #{tag} changed during the release."
    )

    github_release!(version, tag, notes, temporary, verify_only?)

    IO.puts(
      "Release #{tag} complete: Hex, HexDocs, fresh consumer and production release, and GitHub Release verified."
    )
  end

  defp ci!(tag, revision, verify_only?) do
    runs =
      command!("gh", [
        "run",
        "list",
        "--repo",
        @repository,
        "--workflow",
        "ci.yml",
        "--branch",
        tag,
        "--commit",
        revision,
        "--event",
        "push",
        "--limit",
        "1",
        "--json",
        "databaseId,status,conclusion,headSha,url"
      ])
      |> JSON.decode!()

    ensure!(runs != [], "No CI run exists for #{tag} at HEAD; wait for tag CI and rerun.")
    [run] = runs

    if run["status"] != "completed" do
      ensure!(!verify_only?, "Tag CI is still running: #{run["url"]}")

      interactive!("gh", [
        "run",
        "watch",
        to_string(run["databaseId"]),
        "--repo",
        @repository,
        "--exit-status"
      ])
    end

    run =
      command!("gh", [
        "run",
        "view",
        to_string(run["databaseId"]),
        "--repo",
        @repository,
        "--json",
        "status,conclusion,headSha,url"
      ])
      |> JSON.decode!()

    ensure!(
      run["headSha"] == revision and run["status"] == "completed" and
        run["conclusion"] == "success",
      "Tag CI must pass before publication: #{run["url"]}"
    )
  end

  defp docs!(version, tag, release, verify_only?) do
    url = "https://hexdocs.pm/lancedb_ex/#{version}/"
    page = http!(url <> "readme.html")

    if release["has_docs"] != true or is_nil(page) do
      ensure!(!verify_only?, "HexDocs #{version} is missing.")
      interactive!("mix", ~w(hex.publish docs --yes))
    end

    release = hex_release(version)
    page = http!(url <> "readme.html")

    ensure!(
      !is_nil(release) and release["has_docs"] == true and is_binary(page),
      "HexDocs #{version} is not available yet; rerun this command."
    )

    ensure!(
      Regex.match?(Regex.compile!("<title>[^<]* v#{Regex.escape(version)}</title>"), page) and
        String.contains?(page, "https://github.com/#{@repository}/blob/#{tag}/"),
      "HexDocs must show #{version} and link to source at #{tag}."
    )

    IO.puts("Published documentation verified: #{url}")
  end

  defp github_release!(version, tag, notes, temporary, verify_only?) do
    release = github!("releases/tags/#{tag}", true)

    if is_nil(release) do
      ensure!(!verify_only?, "GitHub Release #{tag} is missing.")
      notes_file = Path.join(temporary, "release-notes.md")
      File.write!(notes_file, notes)
      package = http!("https://hex.pm/api/packages/lancedb_ex") |> JSON.decode!()
      latest? = package["latest_stable_version"] == version

      command!("gh", [
        "release",
        "create",
        tag,
        "--repo",
        @repository,
        "--verify-tag",
        "--title",
        tag,
        "--notes-file",
        notes_file,
        "--latest=#{latest?}"
      ])
    end

    release = github!("releases/tags/#{tag}")

    ensure!(
      release["tag_name"] == tag and release["draft"] == false and release["prerelease"] == false,
      "GitHub Release #{tag} must be public and stable."
    )

    ensure!(
      is_binary(release["body"]) and String.trim(release["body"]) != "",
      "GitHub Release #{tag} needs release notes."
    )

    IO.puts("GitHub Release verified: #{release["html_url"]}")
  end

  defp release_notes!(version, tag) do
    pattern = Regex.compile!("^## #{Regex.escape(version)}\\n\\n(.*?)(?=^## |\\z)", "ms")
    match = Regex.run(pattern, File.read!("CHANGELOG.md"))
    ensure!(!is_nil(match), "CHANGELOG.md needs a ## #{version} section.")
    [_, notes] = match
    ensure!(String.trim(notes) != "", "The #{version} changelog is empty.")

    notes =
      Regex.replace(~r/`t:([A-Za-z0-9_.]+)\.([a-z0-9_]+)\/(\d+)`/, notes, fn _,
                                                                             module,
                                                                             type,
                                                                             arity ->
        "[`#{module}.#{type}/#{arity}`](https://hexdocs.pm/lancedb_ex/#{version}/#{module}.html#t:#{type}/#{arity})"
      end)

    "[Hex package](https://hex.pm/packages/lancedb_ex/#{version}) · " <>
      "[Documentation](https://hexdocs.pm/lancedb_ex/#{version}/)\n\n" <>
      String.trim(notes) <> "\n" <> "\n[Source](https://github.com/#{@repository}/tree/#{tag})\n"
  end

  defp package_files(root) do
    Path.wildcard(Path.join(root, "**/*"), match_dot: true)
    |> Enum.filter(&File.regular?/1)
    |> Map.new(fn path -> {Path.relative_to(path, root), File.read!(path)} end)
    |> Map.delete("hex_metadata.config")
  end

  defp hex_release(version) do
    case http!("https://hex.pm/api/packages/lancedb_ex/releases/#{version}") do
      nil -> nil
      body -> JSON.decode!(body)
    end
  end

  defp http!(url) do
    destination =
      Path.join(
        System.tmp_dir!(),
        "lancedb-http-#{Base.url_encode64(:crypto.strong_rand_bytes(12), padding: false)}"
      )

    try do
      status =
        command!("curl", [
          "--disable",
          "--silent",
          "--show-error",
          "--location",
          "--connect-timeout",
          "10",
          "--max-time",
          "60",
          "--output",
          destination,
          "--write-out",
          "%{http_code}",
          url
        ])

      case status do
        "200" -> File.read!(destination)
        "404" -> nil
        _ -> raise "HTTP #{status} from #{url}; publication stopped."
      end
    after
      File.rm(destination)
    end
  end

  defp github!(path, missing? \\ false) do
    args = ["api", "repos/#{@repository}/#{path}"]

    case System.cmd("gh", args, stderr_to_stdout: true) do
      {output, 0} ->
        JSON.decode!(output)

      {output, _} ->
        if missing? and String.contains?(output, "(HTTP 404)"),
          do: nil,
          else: raise("GitHub request failed: #{output}")
    end
  end

  defp clean_tree!(revision \\ nil) do
    status = command!("git", ~w(status --porcelain --untracked-files=all))

    ensure!(
      status == "",
      "Commit all release changes first; the working tree must be clean.\n#{status}"
    )

    if revision do
      head = command!("git", ~w(rev-parse HEAD)) |> String.trim()

      ensure!(
        head == revision,
        "HEAD changed during the release; return to the tagged checkout and rerun."
      )
    end
  end

  defp command!(tool, args) do
    case System.cmd(tool, args, stderr_to_stdout: true) do
      {output, 0} -> output
      {output, status} -> raise "#{tool} #{Enum.join(args, " ")} exited #{status}:\n#{output}"
    end
  end

  defp interactive!(tool, args) do
    IO.puts("Running #{tool} #{Enum.join(args, " ")}")
    # Inherit the terminal so Hex can read passwords and OTPs itself.
    {_, status} = System.cmd(tool, args, use_stdio: false)
    ensure!(status == 0, "#{tool} exited #{status}; rerun the release command to resume.")
  end

  defp ensure!(condition, message) do
    if !condition, do: raise(message)
  end
end

try do
  LanceDB.Release.main(System.argv())
rescue
  error ->
    IO.puts(:stderr, "Release failed: #{Exception.message(error)}")
    System.halt(1)
end
