defmodule LanceDB.WriteFailurePropertyTest do
  use ExUnit.Case, async: true
  use ExUnitProperties
  import LanceDB.TestSupport, only: [with_database: 1]
  alias LanceDB.{Connection, Error, History, Query, Source, Table}

  property "malformed rows at any chunk position cannot partially commit a single write" do
    check all(
            prefix_size <- integer(0..18),
            chunk_size <- integer(1..6),
            mode <- member_of([:append, :overwrite]),
            max_runs: 35
          ) do
      with_database(fn conn ->
        original = [%{"id" => -1}]
        assert {:ok, table} = Connection.create_table(conn, "rows", original)
        assert {:ok, before} = History.version(table)
        valid = Enum.map(indices(prefix_size), &%{id: &1})
        input = valid ++ [%{id: "invalid"}, %{id: 999}]

        assert {:error, %Error{kind: :invalid_data, operation: :add, details: %{inserted: 0}}} =
                 Table.add(table, Source.rows(input, batch_size: chunk_size),
                   commit: :once,
                   mode: mode
                 )

        assert {:ok, ^before} = History.version(table)
        assert {:ok, ^original} = Query.query(table)
      end)
    end
  end

  property "producer failures report exactly the committed prefix for each commit policy" do
    check all(
            prefix_size <- integer(0..18),
            chunk_size <- integer(1..6),
            failure <- member_of([:raise, :throw, :exit]),
            max_runs: 35
          ) do
      for {commit, mode} <- [once: :append, once: :overwrite, batch: :append] do
        with_database(fn conn ->
          assert {:ok, table} = Connection.create_table(conn, "rows", [%{id: -1}])
          assert {:ok, before} = History.version(table)
          tag = make_ref()
          owner = self()

          input =
            Stream.resource(
              fn -> 0 end,
              fn
                ^prefix_size -> fail(failure)
                index -> {[%{id: index}], index + 1}
              end,
              fn _ -> send(owner, {tag, :closed}) end
            )

          commits = if commit == :batch, do: div(prefix_size, chunk_size), else: 0
          inserted = commits * chunk_size
          version = if commits > 0, do: before + commits

          assert {:error, %Error{kind: :source, operation: :add, details: details}} =
                   Table.add(table, Source.rows(input, batch_size: chunk_size),
                     commit: commit,
                     mode: mode
                   )

          assert details.inserted == inserted
          assert details.commits == commits
          assert details.version == version
          assert_received {^tag, :closed}
          refute_received {^tag, :closed}
          current = before + commits
          assert {:ok, ^current} = History.version(table)
          expected = Enum.map([-1 | indices(inserted)], &%{"id" => &1})
          assert {:ok, ^expected} = Query.query(table, order_by: [asc: :id], limit: nil)
        end)
      end
    end
  end

  property "failing finalizers preserve validation failures and exactly the committed prefix" do
    check all(
            prefix_size <- integer(0..18),
            chunk_size <- integer(1..6),
            failure <- member_of([:raise, :throw, :exit]),
            max_runs: 25
          ) do
      for {commit, mode} <- [once: :append, once: :overwrite, batch: :append] do
        with_database(fn conn ->
          assert {:ok, table} = Connection.create_table(conn, "rows", [%{id: -1}])
          assert {:ok, before} = History.version(table)
          owner = self()
          tag = make_ref()

          input =
            Stream.resource(
              fn -> 0 end,
              fn index ->
                value = if index == prefix_size, do: "invalid", else: index
                {[%{id: value}], index + 1}
              end,
              fn _ ->
                send(owner, {tag, :closed})
                fail(failure)
              end
            )

          assert {:error, %Error{kind: :invalid_data, operation: :add, details: details}} =
                   Table.add(table, Source.rows(input, batch_size: chunk_size),
                     commit: commit,
                     mode: mode
                   )

          commits = if commit == :batch, do: div(prefix_size, chunk_size), else: 0
          inserted = commits * chunk_size
          assert details.inserted == inserted
          assert Map.get(details, :commits, 0) == commits
          assert Map.get(details, :version) == if(commits > 0, do: before + commits)
          assert %Error{kind: :source, details: %{phase: :cleanup}} = details.cleanup_error
          assert_received {^tag, :closed}
          refute_received {^tag, :closed}
          current = before + commits
          assert {:ok, ^current} = History.version(table)
          expected = Enum.map([-1 | indices(inserted)], &%{"id" => &1})
          assert {:ok, ^expected} = Query.query(table, order_by: [asc: :id], limit: nil)
        end)
      end
    end
  end

  property "finalizer failures at EOF abort pending writes and retain earlier batch commits" do
    check all(
            count <- integer(0..12),
            failure <- member_of([:raise, :throw, :exit]),
            max_runs: 20
          ) do
      for commit <- [:once, :batch] do
        with_database(fn conn ->
          assert {:ok, table} = Connection.create_table(conn, "rows", [%{id: -1}])
          assert {:ok, before} = History.version(table)
          owner = self()
          tag = make_ref()

          input =
            Stream.resource(
              fn -> indices(count) end,
              fn
                [] -> {:halt, []}
                [id | rest] -> {[%{id: id}], rest}
              end,
              fn _ ->
                send(owner, {tag, :closed})
                fail(failure)
              end
            )

          assert {:error, %Error{kind: :source, operation: :add, details: details}} =
                   Table.add(table, Source.rows(input, batch_size: 1), commit: commit)

          committed = if commit == :batch, do: count, else: 0
          assert details.inserted == committed
          assert details.commits == committed
          assert details.version == if(committed > 0, do: before + committed)
          assert_received {^tag, :closed}
          refute_received {^tag, :closed}
          current = before + committed
          assert {:ok, ^current} = History.version(table)
          expected = Enum.map([-1 | indices(committed)], &%{"id" => &1})
          assert {:ok, ^expected} = Query.query(table, order_by: [asc: :id], limit: nil)
        end)
      end
    end
  end

  defp indices(0), do: []
  defp indices(count), do: Enum.to_list(0..(count - 1))
  defp fail(:raise), do: raise("producer failed")
  defp fail(:throw), do: throw(:producer_failed)
  defp fail(:exit), do: exit(:producer_failed)
end
