defmodule LanceDB.ConsumerRunnerTest do
  use LanceDB.DataCase, async: true

  test "consumer verification isolates inherited Mix project settings", %{path: root} do
    File.mkdir_p!(Path.join(root, "scripts"))
    File.mkdir_p!(Path.join(root, "bin"))
    File.cp!("scripts/validate_consumer.sh", Path.join(root, "scripts/validate_consumer.sh"))
    File.write!(Path.join(root, "mix.exs"), "# unpacked package fixture\n")

    File.write!(Path.join(root, "bin/mix"), """
    #!/usr/bin/env bash
    for setting in "${MIX_TARGET+MIX_TARGET}" "${MIX_BUILD_PATH+MIX_BUILD_PATH}" "${MIX_BUILD_ROOT+MIX_BUILD_ROOT}" "${MIX_DEPS_PATH+MIX_DEPS_PATH}" "${MIX_LOCKFILE+MIX_LOCKFILE}" "${MIX_EXS+MIX_EXS}"; do
      if [[ -n "$setting" ]]; then
        echo "Inherited $setting" >&2
        exit 2
      fi
    done
    if [[ "$1" == deps.get && -n "${MIX_ENV+MIX_ENV}" ]]; then
      echo 'Inherited MIX_ENV' >&2
      exit 2
    fi
    echo 'Consumer environment isolated'
    exit 23
    """)

    File.chmod!(Path.join(root, "bin/mix"), 0o755)

    # The runner retains the Elixir executable's directory in its restricted PATH.
    # Supply launchers in the fixture directory so its Mix stub stays accessible.
    for tool <- ~w(elixir erl) do
      File.ln_s!(System.find_executable(tool), Path.join(root, "bin/#{tool}"))
    end

    overrides =
      for key <-
            ~w(MIX_ENV MIX_TARGET MIX_BUILD_PATH MIX_BUILD_ROOT MIX_DEPS_PATH MIX_LOCKFILE MIX_EXS),
          do: {key, Path.join(root, "external")}

    assert {output, 23} =
             System.cmd("bash", ["scripts/validate_consumer.sh", root],
               cd: root,
               env: [
                 {"PATH", Path.join(root, "bin") <> ":" <> System.get_env("PATH")} | overrides
               ],
               stderr_to_stdout: true
             )

    assert output =~ "Consumer environment isolated"
    refute File.exists?(Path.join(root, "external"))
  end
end
