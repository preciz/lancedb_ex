defmodule LanceDB.ReleaseTest do
  use LanceDB.DataCase, async: true

  @script Path.expand("../../scripts/release.exs", __DIR__)
  @tool Path.expand("../fixtures/release_tool.exs", __DIR__)

  setup %{path: root} do
    checkout = Path.join(root, "checkout")
    File.mkdir_p!(Path.join(checkout, "scripts"))
    File.mkdir_p!(Path.join(root, "bin"))

    File.cp!(
      "scripts/check_native_build.exs",
      Path.join(checkout, "scripts/check_native_build.exs")
    )

    File.write!(Path.join(checkout, "mix.exs"), """
    defmodule ReleaseFixture.MixProject do
      use Mix.Project
      def project, do: [app: :lancedb_ex, version: "1.2.3"]
    end
    """)

    File.write!(Path.join(checkout, ".gitignore"), "ignored.secret\n")

    File.write!(Path.join(checkout, "CHANGELOG.md"), """
    # Changelog

    ## 1.2.3

    - Current release feature. Use `t:LanceDB.Table.t/0`.

    ## 1.2.2

    - Older release feature.
    """)

    git!(checkout, ~w(init --quiet))
    commit!(checkout)
    selected = git!(checkout, ~w(rev-parse HEAD)) |> String.trim()
    File.write!(Path.join(checkout, "native-build.exs"), inspect(%{source_revision: selected}))
    commit!(checkout)
    git!(checkout, ["tag", "v1.2.3"])
    revision = git!(checkout, ~w(rev-parse HEAD)) |> String.trim()

    for directory <- ~w(expected published) do
      File.mkdir_p!(Path.join(root, directory))
      File.cp!(Path.join(checkout, "mix.exs"), Path.join(root, "#{directory}/mix.exs"))
    end

    for tool <- ~w(gh curl mix bash) do
      path = Path.join(root, "bin/#{tool}")

      File.write!(path, """
      #!/bin/sh
      exec "$RELEASE_TEST_ELIXIR" --erl "+S 1:1" "$RELEASE_TEST_TOOL" "#{tool}" "$@"
      """)

      File.chmod!(path, 0o755)
    end

    File.write!(
      Path.join(root, "state.json"),
      JSON.encode!(%{
        "revision" => revision,
        "ci" => "success",
        "hex" => true,
        "docs" => true,
        "github" => true
      })
    )

    %{root: root, checkout: checkout}
  end

  test "complete releases can be verified and rerun without publishing again", context do
    for args <- [["--verify-only"], []] do
      assert {output, 0} = run(context, args)
      assert output =~ "Release v1.2.3 complete"
    end

    refute publishes?(commands(context))
    assert Enum.any?(commands(context), &(&1["tool"] == "bash"))
    assert Enum.all?(commands(context), &is_nil(&1["force_build"]))
    assert Enum.all?(commands(context), &(&1["mix_env"] == "dev"))
  end

  test "new releases publish Hex before validating consumers and creating GitHub notes",
       context do
    change_state(context, %{"hex" => false, "docs" => false, "github" => false})
    assert {output, 0} = run(context)
    assert output =~ "Release v1.2.3 complete"
    calls = commands(context)
    publish = Enum.find_index(calls, &(&1["args"] == ~w(hex.publish --yes)))
    consumer = Enum.find_index(calls, &(&1["tool"] == "bash"))
    create = Enum.find_index(calls, &(Enum.take(&1["args"], 2) == ~w(release create)))
    assert publish < consumer and consumer < create
    assert "--verify-tag" in Enum.at(calls, create)["args"]
    assert "--latest=true" in Enum.at(calls, create)["args"]
    notes = File.read!(Path.join(context.root, "release-notes.md"))
    assert notes =~ "Current release feature"
    assert notes =~ "LanceDB.Table.html#t:t/0"
    refute notes =~ "Older release feature"
    assert {_, 0} = run(context)
    assert Enum.count(commands(context), &(&1["args"] == ~w(hex.publish --yes))) == 1
  end

  test "a package-only partial release resumes docs and GitHub without replacing Hex", context do
    change_state(context, %{
      "hex" => false,
      "docs" => false,
      "github" => false,
      "publish_failure" => true
    })

    assert {output, 1} = run(context)
    assert output =~ "rerun the release command to resume"
    change_state(context, %{"publish_failure" => false})
    assert {_, 0} = run(context)
    calls = commands(context)
    assert Enum.count(calls, &(&1["args"] == ~w(hex.publish --yes))) == 1
    assert Enum.count(calls, &(&1["args"] == ~w(hex.publish docs --yes))) == 1
  end

  test "uncommitted changes and mismatched local tags fail before remote actions", context do
    File.write!(Path.join(context.checkout, "README.md"), "Changed release\n")
    assert {output, 1} = run(context)
    assert output =~ "working tree must be clean"
    assert commands(context) == []
    commit!(context.checkout)
    assert {output, 1} = run(context)
    assert output =~ "must identify HEAD"
    assert commands(context) == []
  end

  test "a different remote tag fails before publication", context do
    change_state(context, %{"remote_revision" => String.duplicate("a", 40)})
    assert {output, 1} = run(context)
    assert output =~ "GitHub tag v1.2.3 does not identify HEAD"
    refute publishes?(commands(context))
  end

  test "failed or missing CI prevents publication", context do
    for conclusion <- ~w(failure missing) do
      change_state(context, %{"ci" => conclusion, "hex" => false})
      assert {_, 1} = run(context)
      refute publishes?(commands(context))
    end
  end

  test "pending CI is watched and must pass before publication", context do
    change_state(context, %{"ci" => "pending"})
    assert {output, 1} = run(context, ["--verify-only"])
    assert output =~ "Tag CI is still running"
    refute publishes?(commands(context))
    assert {_, 0} = run(context)
    assert Enum.any?(commands(context), &(Enum.take(&1["args"], 2) == ~w(run watch)))
  end

  test "an existing package with different files cannot be released", context do
    change_state(context, %{"github" => false})
    File.write!(Path.join(context.root, "published/mix.exs"), "Different package\n")
    assert {output, 1} = run(context)
    assert output =~ "Published Hex contents differ"
    refute publishes?(commands(context))
  end

  test "ignored files selected by Hex are rejected before publication", context do
    change_state(context, %{"hex" => false, "docs" => false, "github" => false})

    for directory <- ~w(checkout expected published) do
      File.write!(Path.join(context.root, "#{directory}/ignored.secret"), "Not tagged\n")
    end

    assert {output, 1} = run(context)
    assert output =~ "Built package contains files that differ from the tagged source"
    refute publishes?(commands(context))
  end

  test "HTTP service errors fail instead of being interpreted as an unpublished package",
       context do
    change_state(context, %{"http_error" => "503"})
    assert {output, 1} = run(context)
    assert output =~ "HTTP 503"
    refute publishes?(commands(context))
  end

  test "read-only verification fails on incomplete releases without publishing", context do
    for state <- [%{"hex" => false}, %{"docs" => false}, %{"github" => false}] do
      change_state(context, Map.merge(%{"hex" => true, "docs" => true, "github" => true}, state))
      assert {_, 1} = run(context, ["--verify-only"])
    end

    refute publishes?(commands(context))
  end

  test "consumer failure prevents the GitHub announcement", context do
    change_state(context, %{"consumer_failure" => true, "github" => false})
    assert {output, 1} = run(context)
    assert output =~ "rerun the release command to resume"
    refute publishes?(commands(context))
  end

  test "older Hex versions do not replace the latest GitHub release", context do
    change_state(context, %{"latest" => "1.3.0", "github" => false})
    assert {_, 0} = run(context)
    create = Enum.find(commands(context), &(Enum.take(&1["args"], 2) == ~w(release create)))
    assert "--latest=false" in create["args"]
  end

  test "GitHub permission failures do not trigger release creation", context do
    change_state(context, %{"github_error" => "gh: Forbidden (HTTP 403)"})
    assert {output, 1} = run(context)
    assert output =~ "GitHub request failed"
    refute publishes?(commands(context))
  end

  test "retired packages, stale documentation and draft releases cannot report success",
       context do
    for {state, expected} <- [
          {%{"retirement" => %{"reason" => "invalid"}}, "is retired"},
          {%{"retirement" => nil, "docs_version" => "1.2.2"}, "HexDocs must show"},
          {%{"docs_version" => nil, "draft" => true}, "must be public and stable"}
        ] do
      change_state(context, state)
      assert {output, 1} = run(context)
      assert output =~ expected
    end
  end

  defp run(context, args \\ []) do
    System.cmd(System.find_executable("elixir"), ["--erl", "+S 2:2", @script | args],
      cd: context.checkout,
      env: [
        {"PATH", Path.join(context.root, "bin") <> ":" <> System.get_env("PATH")},
        {"RELEASE_TEST_ROOT", context.root},
        {"RELEASE_TEST_TOOL", @tool},
        {"RELEASE_TEST_ELIXIR", System.find_executable("elixir")},
        {"LANCEDB_BUILD", "true"},
        {"MIX_ENV", "test"}
      ],
      stderr_to_stdout: true
    )
  end

  defp commands(context) do
    path = Path.join(context.root, "commands.jsonl")
    if File.exists?(path), do: path |> File.stream!() |> Enum.map(&JSON.decode!/1), else: []
  end

  defp publishes?(calls) do
    Enum.any?(calls, fn call ->
      List.first(call["args"]) == "hex.publish" or
        Enum.take(call["args"], 2) == ~w(release create)
    end)
  end

  defp change_state(context, changes) do
    path = Path.join(context.root, "state.json")
    state = path |> File.read!() |> JSON.decode!()
    File.write!(path, JSON.encode!(Map.merge(state, changes)))
  end

  defp commit!(checkout) do
    git!(checkout, ~w(add .))
    git!(checkout, ["commit", "--quiet", "-m", "Release fixture"])
  end

  defp git!(checkout, args) do
    {output, status} =
      System.cmd(
        "git",
        [
          "-c",
          "user.name=Release fixture",
          "-c",
          "user.email=fixture@example.invalid",
          "-c",
          "commit.gpgsign=false",
          "-c",
          "core.hooksPath=/dev/null" | args
        ],
        cd: checkout,
        stderr_to_stdout: true
      )

    assert status == 0, output
    output
  end
end
