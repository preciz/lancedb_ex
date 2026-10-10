defmodule LanceDB.TestSupport do
  alias LanceDB.Query.Executor
  @moduledoc false
  def tmp_dir, do: :persistent_term.get({__MODULE__, :tmp_dir})

  def unique_path do
    Path.join(tmp_dir(), "case_#{System.unique_integer([:positive, :monotonic])}")
  end

  # Allocate inside check all: every generation and shrink attempt starts clean.
  # The callback may accept the connection alone, or the connection and its path.
  def with_database(fun) do
    path = unique_path()

    try do
      {:ok, conn} = LanceDB.connect(path)
      if is_function(fun, 2), do: fun.(conn, path), else: fun.(conn)
    after
      File.rm_rf!(path)
    end
  end

  def assert_eventually(fun, timeout_ms \\ 1000) do
    eventually(fun, System.monotonic_time(:millisecond) + timeout_ms)
  end

  defp eventually(fun, deadline) do
    if fun.() do
      :ok
    else
      if System.monotonic_time(:millisecond) < deadline do
        Process.sleep(5)
        eventually(fun, deadline)
      else
        ExUnit.Assertions.flunk("condition did not become true before the deadline")
      end
    end
  end

  # Parameterized integration matrices select a source before using the public builder.
  def query(table, opts \\ []) do
    table = if table == :unused, do: %LanceDB.Table{resource: make_ref()}, else: table

    keys =
      Enum.filter(
        [:row_ids, :row_offsets, :vectors, :multivector, :vector, :text],
        &Keyword.has_key?(opts, &1)
      )

    {source, controls} =
      case keys do
        [] ->
          {:scan, opts}

        [:vector, :text] ->
          {{:hybrid, opts[:vector], opts[:text]}, Keyword.drop(opts, keys)}

        [key] ->
          source =
            case key do
              :vectors -> {:batch, opts[key]}
              key when key in [:row_ids, :row_offsets] -> {:take, key, opts[key]}
              key -> {key, opts[key]}
            end

          {source, Keyword.delete(opts, key)}

        _ ->
          raise ArgumentError, "incompatible test query sources"
      end

    LanceDB.Query.new(table, source, controls)
  end

  def rows(table, opts \\ []) do
    {batch_size, opts} = Keyword.pop(opts, :batch_size, 1024)
    table |> query(opts) |> LanceDB.Query.stream(batch_size: batch_size)
  end

  def output_schema(table, opts \\ []), do: table |> query(opts) |> LanceDB.Query.schema()
  def analyze_plan(table, opts \\ []), do: table |> query(opts) |> LanceDB.Query.analyze()

  def explain_plan(table, opts \\ []) do
    {verbose, opts} = Keyword.pop(opts, :verbose, false)
    table |> query(opts) |> LanceDB.Query.explain(verbose: verbose)
  end

  def logical_schema({:ok, arrow}), do: LanceDB.ArrowSchema.to_schema(arrow)
  def logical_schema(error), do: error

  def next_rows(cursor) do
    case Executor.next_batch(cursor) do
      {:ok, nil} -> {:ok, nil}
      {:ok, batch} -> LanceDB.Batch.to_rows(batch)
      error -> error
    end
  end
end
