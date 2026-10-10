defmodule LanceDB.PropertyTest do
  use ExUnit.Case, async: true
  use ExUnitProperties
  import LanceDB.TestSupport, only: [with_database: 1]

  import LanceDB.Generators, only: [text: 0, int64: 0]

  defp scalar do
    map(integer(-100_000..100_000), &(&1 / 37))
  end

  defp as_float32(nil), do: nil

  defp as_float32(value) do
    <<rounded::float-32>> = <<value::float-32>>
    rounded
  end

  defp string_keys(row), do: Map.new(row, fn {key, value} -> {to_string(key), value} end)

  # Arrow emits the shortest decimal that round-trips to float32, which need not
  # equal its exact float64 expansion after Elixir decodes the JSON number.
  defp canonical_float32_fields(row) do
    row
    |> Map.update!("weight", &as_float32/1)
    |> Map.update!("vector", fn
      nil -> nil
      vector -> Enum.map(vector, &as_float32/1)
    end)
  end

  property "typed records round trip through JSON and Arrow, including nulls and float32 rounding" do
    check all(
            dimension <- integer(1..5),
            values <-
              list_of(
                fixed_map(%{
                  text: nullable(text()),
                  number: nullable(int64()),
                  score: nullable(scalar()),
                  weight: nullable(scalar()),
                  active: nullable(boolean()),
                  vector: nullable(list_of(scalar(), length: dimension))
                }),
                min_length: 1,
                max_length: 8
              ),
            atom_keys? <- boolean(),
            max_runs: 50
          ) do
      rows = Enum.with_index(values, fn row, id -> Map.put(row, :id, id) end)

      expected =
        Enum.map(rows, fn row ->
          row
          |> Map.update!(:weight, &as_float32/1)
          |> Map.update!(:vector, fn
            nil -> nil
            vector -> Enum.map(vector, &as_float32/1)
          end)
          |> string_keys()
        end)

      with_database(fn conn ->
        schema = [
          id: :int64,
          text: :string,
          number: :int64,
          score: :float64,
          weight: :float32,
          active: :boolean,
          vector: {:vector, dimension}
        ]

        input = if atom_keys?, do: rows, else: Enum.map(rows, &string_keys/1)
        assert {:ok, table} = LanceDB.Connection.create_table(conn, "docs", input, schema: schema)
        assert {:ok, actual} = LanceDB.Query.query(table, order_by: [asc: :id])
        assert Enum.map(actual, &canonical_float32_fields/1) == expected
        assert {:ok, reopened} = LanceDB.Connection.open_table(conn, "docs")
        assert {:ok, persisted} = LanceDB.Query.query(reopened, order_by: [asc: :id])
        assert Enum.map(persisted, &canonical_float32_fields/1) == expected
      end)
    end
  end

  property "inferred and explicit creation agree on supported records" do
    check all(
            dimension <- integer(1..5),
            values <-
              list_of(
                fixed_map(%{
                  text: nullable(text()),
                  score: nullable(scalar()),
                  vector: list_of(scalar(), length: dimension)
                }),
                max_length: 8
              ),
            max_runs: 40
          ) do
      # The first row determines types; later scalar nulls remain valid.
      first = %{text: "first", score: 0.5, vector: List.duplicate(0.0, dimension)}
      rows = Enum.with_index([first | values], fn row, id -> Map.put(row, :id, id) end)

      with_database(fn conn ->
        schema = [id: :int64, text: :string, score: :float64, vector: {:vector, dimension}]
        assert {:ok, inferred} = LanceDB.Connection.create_table(conn, "inferred", rows)

        assert {:ok, explicit} =
                 LanceDB.Connection.create_table(conn, "explicit", rows, schema: schema)

        assert {:ok, inferred_rows} = LanceDB.Query.query(inferred, order_by: [asc: :id])
        assert {:ok, explicit_rows} = LanceDB.Query.query(explicit, order_by: [asc: :id])
        assert inferred_rows == explicit_rows
        assert {:ok, _} = LanceDB.Table.schema(inferred)
      end)
    end
  end

  property "literal string updates preserve the exact value and unrelated rows" do
    check all(value <- text(), target <- integer(1..3), max_runs: 50) do
      rows = for id <- 1..3, do: %{id: id, text: "original"}

      expected =
        Enum.map(rows, fn row ->
          if row.id == target, do: string_keys(%{row | text: value}), else: string_keys(row)
        end)

      with_database(fn conn ->
        {:ok, table} = LanceDB.Connection.create_table(conn, "docs", rows)

        assert {:ok, %{updated: 1, version: version}} =
                 LanceDB.Table.update(table, %{text: value}, filter: "id = #{target}")

        assert {:ok, ^version} = LanceDB.History.version(table)
        assert {:ok, ^expected} = LanceDB.Query.query(table, order_by: [asc: :id])
      end)
    end
  end

  property "upsert sequences agree with a map model and repeated upserts preserve contents" do
    batch =
      gen all(
            ids <- LanceDB.Generators.unique_subset(Enum.to_list(0..9), 1, 6),
            texts <- list_of(text(), length: length(ids))
          ) do
        Enum.zip_with(ids, texts, &%{id: &1, text: &2})
      end

    check all(batches <- list_of(batch, min_length: 1, max_length: 4), max_runs: 40) do
      with_database(fn conn ->
        {:ok, table} =
          LanceDB.Connection.create_empty_table(conn, "docs", id: :int64, text: :string)

        Enum.reduce(batches, %{}, fn rows, model ->
          inserted = Enum.count(rows, &(not Map.has_key?(model, &1.id)))
          updated = length(rows) - inserted

          assert {:ok, %{inserted: ^inserted, updated: ^updated}} =
                   LanceDB.Table.upsert(table, rows)

          model = Enum.reduce(rows, model, &Map.put(&2, &1.id, &1))
          expected = model |> Map.values() |> Enum.sort_by(& &1.id) |> Enum.map(&string_keys/1)
          assert {:ok, ^expected} = LanceDB.Query.query(table, order_by: [asc: :id])
          count = length(rows)
          assert {:ok, %{inserted: 0, updated: ^count}} = LanceDB.Table.upsert(table, rows)
          assert {:ok, ^expected} = LanceDB.Query.query(table, order_by: [asc: :id])
          model
        end)
      end)
    end
  end

  property "ordered projected pages and filtered counts agree with an Elixir model" do
    check all(
            scores <- list_of(nullable(integer(-5..5)), min_length: 1, max_length: 20),
            page_size <- integer(1..7),
            cutoff <- integer(-5..5),
            filtered? <- boolean(),
            direction <-
              member_of([:asc_nulls_first, :asc_nulls_last, :desc_nulls_first, :desc_nulls_last]),
            max_runs: 40
          ) do
      rows = Enum.with_index(scores, fn score, id -> %{id: id, score: score} end)

      expected =
        rows
        |> Enum.filter(&(not filtered? or (is_integer(&1.score) and &1.score >= cutoff)))
        |> Enum.sort_by(fn row ->
          nulls_first? = direction in [:asc_nulls_first, :desc_nulls_first]
          descending? = direction in [:desc_nulls_first, :desc_nulls_last]
          null_rank = if is_nil(row.score) == nulls_first?, do: 0, else: 1
          score = if descending?, do: -(row.score || 0), else: row.score || 0
          {null_rank, score, row.id}
        end)
        |> Enum.map(&%{"id" => &1.id})

      with_database(fn conn ->
        {:ok, table} =
          LanceDB.Connection.create_table(conn, "docs", rows, schema: [id: :int64, score: :int64])

        filter = if filtered?, do: "score >= #{cutoff}"
        count = length(expected)
        assert {:ok, ^count} = LanceDB.Table.count_rows(table, filter)

        # Include a final empty page; score is intentionally omitted from projection.
        actual =
          Enum.flat_map(0..div(length(rows), page_size), fn page ->
            assert {:ok, result} =
                     LanceDB.Query.query(table,
                       filter: filter,
                       columns: [:id],
                       order_by: [{direction, :score}, {:asc, :id}],
                       limit: page_size,
                       offset: page * page_size
                     )

            result
          end)

        assert actual == expected
      end)
    end
  end

  property "invalid vector lengths and nulls in required fields leave data and version unchanged" do
    check all(
            dimension <- integer(1..5),
            bad_length <- member_of([0, dimension + 1]),
            max_runs: 30
          ) do
      with_database(fn conn ->
        schema = [%{name: :id, type: :int64, nullable: false}, {:vector, {:vector, dimension}}]
        original = %{id: 1, vector: List.duplicate(1.0, dimension)}
        {:ok, table} = LanceDB.Connection.create_table(conn, "docs", [original], schema: schema)
        {:ok, version} = LanceDB.History.version(table)

        for bad <- [
              %{id: 2, vector: List.duplicate(1.0, bad_length)},
              %{id: nil, vector: List.duplicate(1.0, dimension)}
            ] do
          assert {:error, %LanceDB.Error{kind: :invalid_data, operation: :add}} =
                   LanceDB.Table.add(table, [bad])

          assert {:ok, ^version} = LanceDB.History.version(table)
          assert {:ok, [row]} = LanceDB.Query.query(table)
          assert row == string_keys(original)
        end
      end)
    end
  end

  property "atom and string aliases cannot silently overwrite fields" do
    check all(name <- member_of([:id, :text, :vector, :雪]), left <- text(), right <- text()) do
      assert {:error, _} =
               LanceDB.Records.validate([%{name => left, Atom.to_string(name) => right}])

      assert_raise ArgumentError, fn ->
        LanceDB.Schema.new([{name, :string}, {Atom.to_string(name), :int64}])
      end
    end
  end
end
