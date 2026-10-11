# Run with MIX_ENV=test LANCEDB_BUILD=true scripts/test_s3.sh mix run
# benchmarks/connection_open.exs. --legacy-only also works before cache options
# were added to connect. Results are JSON; times are microseconds.
defmodule ConnectionOpenBenchmark do
  @moduledoc false
  alias LanceDB.{Connection, Index}

  @budgets [
    index_cache_size_bytes: 112 * 1024 * 1024,
    metadata_cache_size_bytes: 16 * 1024 * 1024
  ]
  @consistency [read_consistency_interval_ms: 0]

  def run do
    iterations = System.get_env("LANCEDB_BENCH_ITERATIONS", "100") |> String.to_integer()
    if iterations < 1, do: raise(ArgumentError, "iterations must be positive")

    local =
      Path.join(System.tmp_dir!(), "lancedb_open_bench_#{System.unique_integer([:positive])}")

    modes = if "--legacy-only" in System.argv(), do: [:legacy], else: [:legacy, :configured]
    stores = [{"local", local, %{}}] ++ s3_store()

    try do
      for {storage, uri, options} <- stores do
        {:ok, seed} = Connection.connect(uri, options)

        {:ok, table} =
          Connection.create_table(
            seed,
            "bench",
            for(id <- 1..100, do: %{id: id, text: "document #{id}"})
          )

        {:ok, :created} = Index.create(table, :text, type: :fts, name: "texts")

        try do
          for mode <- modes do
            # Prime module loading and SDK initialization outside the samples.
            for _ <- 1..5, do: cold(mode, uri, options)
            samples = for _ <- 1..iterations, do: cold(mode, uri, options)
            report(storage, mode, "cold", samples)

            {setup, conn, table} = open(mode, uri, options)
            {:ok, :prewarmed} = Index.prewarm(table, "texts")
            report(storage, mode, "warm_setup", [setup])
            for _ <- 1..5, do: warm(mode, conn)
            samples = for _ <- 1..iterations, do: warm(mode, conn)
            report(storage, mode, "warm", samples)
          end
        after
          {:ok, :dropped} = Connection.drop_table(seed, "bench")
        end
      end
    after
      File.rm_rf!(local)
    end
  end

  defp s3_store do
    case System.get_env("LANCEDB_TEST_S3_URI") do
      nil ->
        []

      uri ->
        options = %{
          aws_region: "us-east-1",
          aws_endpoint: System.fetch_env!("LANCEDB_TEST_S3_ENDPOINT"),
          aws_access_key_id: System.fetch_env!("LANCEDB_TEST_S3_ACCESS_KEY"),
          aws_secret_access_key: System.fetch_env!("LANCEDB_TEST_S3_SECRET_KEY"),
          allow_http: "true",
          aws_virtual_hosted_style_request: "false"
        }

        [{"s3", uri <> "/open_benchmark_#{System.unique_integer([:positive])}", options}]
    end
  end

  defp open(mode, uri, options) do
    opts = if mode == :configured, do: @consistency ++ @budgets, else: @consistency
    {connect_us, {:ok, conn}} = :timer.tc(fn -> Connection.connect(uri, options, opts) end)
    {sample, table} = measured_open(mode, conn)
    {Map.put(sample, :connect_us, connect_us), conn, table}
  end

  defp cold(mode, uri, options) do
    {sample, _conn, _table} = open(mode, uri, options)
    sample
  end

  defp measured_open(mode, conn) do
    opts = if mode == :legacy, do: @budgets, else: []
    {open_us, {:ok, table}} = :timer.tc(fn -> Connection.open_table(conn, "bench", opts) end)
    {%{connect_us: 0, open_us: open_us}, table}
  end

  defp warm(mode, conn) do
    {sample, _table} = measured_open(mode, conn)
    sample
  end

  defp report(storage, mode, state, samples) do
    stats =
      Map.new([:connect_us, :open_us, :total_us], fn field ->
        values =
          Enum.map(samples, fn sample ->
            if field == :total_us, do: sample.connect_us + sample.open_us, else: sample[field]
          end)
          |> Enum.sort()

        {field,
         %{
           median: Enum.at(values, div(length(values), 2)),
           p95: Enum.at(values, ceil(length(values) * 0.95) - 1)
         }}
      end)

    IO.puts(
      JSON.encode!(%{
        storage: storage,
        mode: mode,
        state: state,
        iterations: length(samples),
        timings: stats
      })
    )
  end
end

ConnectionOpenBenchmark.run()
