defmodule LanceDB.MergeModelPropertyTest do
  use ExUnit.Case, async: true
  use ExUnitProperties
  import LanceDB.TestSupport, only: [with_database: 1]
  alias LanceDB.{Connection, Generators, History, Query, Source, Table}

  property "conditional composite-key merges agree with a model independent of source chunking" do
    keys = for tenant <- ["a", "b"], id <- 0..5, do: {id, tenant}

    rows =
      gen all(
            keys <- Generators.unique_subset(keys, 0, 10),
            values <-
              list_of(tuple({integer(-3..3), nullable(Generators.text())}), length: length(keys))
          ) do
        Enum.zip_with(keys, values, fn {id, tenant}, {revision, text} ->
          %{id: id, tenant: tenant, revision: revision, text: text}
        end)
      end

    check all(
            initial <- rows,
            incoming <- rows,
            delete? <- boolean(),
            size <- integer(1..5),
            max_runs: 35
          ) do
      source = Map.new(incoming, &{key(&1), &1})
      model = Map.new(initial, &{key(&1), &1})

      model =
        if delete?,
          do:
            Map.reject(model, fn {key, row} ->
              row.tenant == "a" and not Map.has_key?(source, key)
            end),
          else: model

      model =
        Enum.reduce(source, model, fn {key, row}, model ->
          case Map.fetch(model, key) do
            {:ok, old} when old.revision >= row.revision -> model
            _ -> Map.put(model, key, row)
          end
        end)

      expected =
        model
        |> Map.values()
        |> Enum.sort_by(&{&1.tenant, &1.id})
        |> Enum.map(&Map.new(&1, fn {key, value} -> {to_string(key), value} end))

      with_database(fn conn ->
        schema = [id: :int64, tenant: :string, revision: :int64, text: :string]
        assert {:ok, table} = Connection.create_table(conn, "rows", initial, schema: schema)
        assert {:ok, before} = History.version(table)
        assert {:ok, snapshot} = History.snapshot(table, before)

        assert {:ok, %{operation: :merge}} =
                 Table.merge(table, Source.rows(incoming, batch_size: size),
                   on: [:tenant, :id],
                   when_matched_update_all: "source.revision > target.revision",
                   when_not_matched_insert_all: true,
                   when_not_matched_by_source_delete: if(delete?, do: "tenant = 'a'", else: false)
                 )

        assert {:ok, ^expected} =
                 Query.query(table, order_by: [asc: :tenant, asc: :id], limit: nil)

        assert {:ok, reopened} = Connection.open_table(conn, "rows")

        assert {:ok, ^expected} =
                 Query.query(reopened, order_by: [asc: :tenant, asc: :id], limit: nil)

        original =
          Enum.map(initial, &Map.new(&1, fn {key, value} -> {to_string(key), value} end))
          |> Enum.sort_by(&{&1["tenant"], &1["id"]})

        assert {:ok, ^original} =
                 Query.query(snapshot, order_by: [asc: :tenant, asc: :id], limit: nil)
      end)
    end
  end

  defp key(row), do: {row.id, row.tenant}
end
