# External-service fixture for release-script tests; never contacts Hex or GitHub.
[tool | args] = System.argv()
root = System.fetch_env!("RELEASE_TEST_ROOT")
state_path = Path.join(root, "state.json")
state = JSON.decode!(File.read!(state_path))

File.write!(
  Path.join(root, "commands.jsonl"),
  JSON.encode!(%{
    "tool" => tool,
    "args" => args,
    "force_build" => System.get_env("LANCEDB_BUILD"),
    "mix_env" => System.get_env("MIX_ENV")
  }) <> "\n",
  [:append]
)

save = fn changes -> File.write!(state_path, JSON.encode!(Map.merge(state, changes))) end
json = fn data -> IO.write(JSON.encode!(data)) end

fail = fn message ->
  IO.puts(:stderr, message)
  System.halt(1)
end

ci = %{
  "databaseId" => 123,
  "headSha" => state["revision"],
  "status" => if(state["ci"] == "pending", do: "in_progress", else: "completed"),
  "conclusion" => state["ci"],
  "url" => "https://github.com/preciz/lancedb_ex/actions/runs/123"
}

case {tool, args} do
  {"gh", ["auth", "status"]} ->
    IO.puts("Authenticated fixture")

  {"gh", ["api", "repos/preciz/lancedb_ex/commits/v1.2.3"]} ->
    json.(%{"sha" => state["remote_revision"] || state["revision"]})

  {"gh", ["api", "repos/preciz/lancedb_ex/releases/tags/v1.2.3"]} ->
    if state["github_error"], do: fail.(state["github_error"])
    if !state["github"], do: fail.("gh: Not Found (HTTP 404)")

    json.(%{
      "tag_name" => "v1.2.3",
      "draft" => state["draft"] || false,
      "prerelease" => false,
      "body" => "Existing release notes",
      "html_url" => "https://github.com/preciz/lancedb_ex/releases/tag/v1.2.3"
    })

  {"gh", ["run", "list" | _]} ->
    json.(if(state["ci"] == "missing", do: [], else: [ci]))

  {"gh", ["run", "view" | _]} ->
    json.(ci)

  {"gh", ["run", "watch" | _]} ->
    save.(%{"ci" => "success"})

  {"gh", ["release", "create", "v1.2.3" | rest]} ->
    [notes_path | _] = rest |> Enum.drop_while(&(&1 != "--notes-file")) |> tl()
    File.cp!(notes_path, Path.join(root, "release-notes.md"))
    save.(%{"github" => true})

  {"curl", rest} ->
    [destination | _] = rest |> Enum.drop_while(&(&1 != "--output")) |> tl()
    url = List.last(rest)

    {status, body} =
      cond do
        state["http_error"] ->
          {state["http_error"], "Service error"}

        String.contains?(url, "/releases/1.2.3") and !state["hex"] ->
          {"404", "Not found"}

        String.contains?(url, "/releases/1.2.3") ->
          {"200",
           JSON.encode!(%{
             "version" => "1.2.3",
             "has_docs" => state["docs"],
             "retirement" => state["retirement"],
             "requirements" => %{"rustler" => %{"optional" => true}}
           })}

        String.contains?(url, "/api/packages/lancedb_ex") ->
          {"200", JSON.encode!(%{"latest_stable_version" => state["latest"] || "1.2.3"})}

        !state["docs"] ->
          {"404", "Not found"}

        true ->
          {"200",
           "<title>LanceDB v#{state["docs_version"] || "1.2.3"}</title><a href=\"https://github.com/preciz/lancedb_ex/blob/v1.2.3/README.md\">Source</a>"}
      end

    File.write!(destination, body)
    IO.write(status)

  {"mix", ["hex.build", "--unpack", "--output", destination]} ->
    File.cp_r!(Path.join(root, "expected"), destination)

  {"mix", ["hex.package", "fetch", "lancedb_ex", "1.2.3", "--unpack", "--output", destination]} ->
    File.cp_r!(Path.join(root, "published"), destination)

  {"mix", ["hex.publish", "--yes"]} ->
    if state["publish_failure"] do
      save.(%{"hex" => true, "docs" => false})
      fail.("Simulated failure after package publication")
    end

    save.(%{"hex" => true, "docs" => true})

  {"mix", ["hex.publish", "docs", "--yes"]} ->
    save.(%{"docs" => true})

  {"bash", ["scripts/validate_consumer.sh", package]} ->
    true = File.regular?(Path.join(package, "mix.exs"))
    if state["consumer_failure"], do: fail.("Consumer check failed")
    IO.puts("Fixture consumer and production release passed")

  _ ->
    fail.("Unexpected fixture command: #{inspect({tool, args})}")
end
