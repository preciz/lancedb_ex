# unique_integer resets between VMs. Give every run its own root, including
# interrupted runs and concurrent Mix invocations, before creating table paths.
run_id = :crypto.strong_rand_bytes(12) |> Base.url_encode64(padding: false)
run_tmp = Path.join(System.tmp_dir!(), "lancedb_test_#{run_id}")
File.mkdir_p!(run_tmp)
:persistent_term.put({LanceDB.TestSupport, :tmp_dir}, run_tmp)
System.at_exit(fn _ -> File.rm_rf!(run_tmp) end)

# Functional tests stay below the default 16-write admission budget.
# ContractTest independently verifies saturation with explicit one-operation limits.
source_build? = System.get_env("LANCEDB_BUILD") in ["1", "true"]

# Elixir exports NIF stubs even when an older artifact lacks their native
# implementations. Probe empty analysis instead of checking exported functions.
preparation_available? =
  try do
    {:ok, _} = LanceDB.FTS.prepare("")
    true
  catch
    :error, :nif_not_loaded -> false
  end

if source_build? and !preparation_available?,
  do: raise("Source build is missing FTS preparation; recompile the native library")

exclude = [:s3]
exclude = if source_build?, do: exclude, else: [:source_build | exclude]
exclude = if preparation_available?, do: exclude, else: [:fts_preparation | exclude]

ExUnit.start(exclude: exclude, max_cases: 8)
