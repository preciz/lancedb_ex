defmodule LanceDB.MinioRunnerTest do
  use LanceDB.DataCase, async: true

  setup %{path: root} do
    File.mkdir_p!(Path.join(root, "scripts"))
    File.mkdir_p!(Path.join(root, "bin"))
    File.cp!("scripts/test_s3.sh", Path.join(root, "scripts/test_s3.sh"))

    File.write!(Path.join(root, "bin/uname"), """
    #!/usr/bin/env bash
    if [[ "$1" == '-s' ]]; then echo Linux; else echo x86_64; fi
    """)

    File.write!(Path.join(root, "bin/curl"), """
    #!/usr/bin/env bash
    while [[ "$1" != '--output' ]]; do shift; done
    printf 'invalid binary' > "$2"
    printf 'download\\n' >> "$MINIO_TEST_DOWNLOADS"
    """)

    for tool <- ~w(uname curl), do: File.chmod!(Path.join(root, "bin/#{tool}"), 0o755)
    %{root: root}
  end

  test "invalid downloads are never cached and the next run downloads again", %{root: root} do
    for _ <- 1..2 do
      assert {output, 1} = run(root)
      assert output =~ "checksum mismatch; download was not cached"
      assert Path.wildcard(Path.join(root, "_build/minio/*/minio.*")) == []
    end

    assert File.read!(Path.join(root, "downloads")) == "download\ndownload\n"
  end

  test "invalid existing cache entries are discarded before downloading", %{root: root} do
    assert {_, 1} = run(root)
    [cache] = Path.wildcard(Path.join(root, "_build/minio/*"))
    binary = Path.join(cache, "minio.linux-amd64")
    File.write!(binary, "corrupted cache entry")

    assert {output, 1} = run(root)
    assert output =~ "Discarding invalid cached MinIO binary"
    refute File.exists?(binary)
    assert File.read!(Path.join(root, "downloads")) == "download\ndownload\n"
  end

  defp run(root) do
    System.cmd("bash", ["scripts/test_s3.sh"],
      cd: root,
      env: [
        {"PATH", Path.join(root, "bin") <> ":" <> System.get_env("PATH")},
        {"MINIO_TEST_DOWNLOADS", Path.join(root, "downloads")}
      ],
      stderr_to_stdout: true
    )
  end
end
