defmodule LanceDB.MetadataPropertyTest do
  use ExUnit.Case, async: true
  use ExUnitProperties
  import LanceDB.TestSupport, only: [with_database: 1]

  property "metadata operation sequences match map models across fields, reopen and history" do
    changes = metadata_map(nullable(text()))
    operation = tuple({member_of([:schema, :id, :nested]), boolean(), changes, boolean()})

    check all(
            initial <- metadata_map(text()),
            operations <- list_of(operation, min_length: 1, max_length: 10),
            max_runs: 40
          ) do
      with_database(fn conn, path ->
        schema = [
          %{name: :id, type: :int64, metadata: initial},
          {:data,
           {:struct, [%{name: :value, type: :string, metadata: initial}, {:sibling, :boolean}]}}
        ]

        {:ok, table} =
          LanceDB.Connection.create_table(
            conn,
            "docs",
            [%{id: 1, data: %{value: "unchanged", sibling: true}}],
            schema: schema,
            schema_metadata: initial
          )

        initial = string_keys(initial)
        model = %{schema: initial, id: initial, nested: initial}
        assert_metadata(table, model)
        {:ok, version} = LanceDB.History.version(table)

        {model, history} =
          Enum.reduce(operations, {model, %{version => model}}, fn operation, {model, history} ->
            next = apply_operation(table, operation, model)
            assert_metadata(table, next)
            {:ok, reopened} = LanceDB.Connection.open_table(conn, "docs")
            assert_metadata(reopened, next)
            {:ok, version} = LanceDB.History.version(table)
            {next, Map.put(history, version, next)}
          end)

        {:ok, fresh} = LanceDB.connect(path)
        {:ok, reopened} = LanceDB.Connection.open_table(fresh, "docs")
        assert_metadata(reopened, model)

        for {version, expected} <- history do
          {:ok, snapshot} = LanceDB.Connection.open_table(fresh, "docs", version: version)
          assert_metadata(snapshot, expected)
          assert {:error, _} = LanceDB.Metadata.update_schema(snapshot, %{owner: "blocked"})

          assert {:error, _} =
                   LanceDB.Metadata.update_field(snapshot, "data.value", %{note: nil})

          assert {:ok, ^version} = LanceDB.History.version(snapshot)
          assert_metadata(snapshot, expected)
        end

        assert_metadata(table, model)
      end)
    end
  end

  defp apply_operation(table, {target, replace, changes, atom_keys?}, model) do
    input = if atom_keys?, do: changes, else: string_keys(changes)
    {:ok, before} = LanceDB.History.version(table)

    assert {:ok, %LanceDB.WriteResult{version: version}} =
             update_metadata(table, target, input, replace)

    assert {:ok, ^version} = LanceDB.History.version(table)
    base = if replace, do: %{}, else: Map.fetch!(model, target)

    expected =
      Enum.reduce(changes, base, fn
        {key, nil}, acc -> Map.delete(acc, Atom.to_string(key))
        {key, value}, acc -> Map.put(acc, Atom.to_string(key), value)
      end)

    next = Map.put(model, target, expected)
    assert version >= before
    if next != model, do: assert(version > before)
    next
  end

  defp update_metadata(table, :schema, changes, replace),
    do: LanceDB.Metadata.update_schema(table, changes, replace: replace)

  defp update_metadata(table, :id, changes, replace),
    do: LanceDB.Metadata.update_field(table, :id, changes, replace: replace)

  defp update_metadata(table, :nested, changes, replace),
    do: LanceDB.Metadata.update_field(table, "data.value", changes, replace: replace)

  defp assert_metadata(table, model) do
    assert {:ok, metadata} = LanceDB.Metadata.schema(table)
    assert metadata == model.schema
    assert {:ok, %LanceDB.Schema{fields: [id, data]} = schema} = LanceDB.Table.schema(table)
    assert schema.metadata == model.schema
    assert id.metadata == model.id
    assert hd(elem(data.type, 1)).metadata == model.nested
    assert data.metadata == %{}
    assert List.last(elem(data.type, 1)).metadata == %{}

    assert {:ok, [%{"id" => 1, "data" => %{"value" => "unchanged", "sibling" => true}}]} =
             LanceDB.Query.query(table)
  end

  defp metadata_map(value) do
    map(list_of(tuple({member_of([:owner, :note, :雪]), value}), max_length: 5), &Map.new/1)
  end

  defp text do
    one_of([string(:utf8, max_length: 20), member_of(["", "雪 🦀", "'\\\n", <<0>>])])
  end

  defp string_keys(map), do: Map.new(map, fn {key, value} -> {Atom.to_string(key), value} end)
end
