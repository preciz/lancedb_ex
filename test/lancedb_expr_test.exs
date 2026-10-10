defmodule LanceDB.ExprTest do
  use LanceDB.DataCase, async: true
  use ExUnitProperties
  alias LanceDB.{Connection, Expr, Query, SchemaEvolution, Table}

  setup %{path: path} do
    {:ok, conn} = LanceDB.connect(path)
    %{conn: conn}
  end

  property "quoted names and string literals survive SQL execution" do
    check all(suffix <- string(:utf8, max_length: 30), max_runs: 30) do
      with_database(fn conn ->
        name = "Case Name " <> String.replace(suffix, [".", "\"", "`"], "_")
        value = "O'Reilly \\ -- \" 雪 " <> suffix
        table_name = "quoted_#{System.unique_integer([:positive])}"
        assert {:ok, table} = Connection.create_table(conn, table_name, [%{name => value}])

        assert {:ok, [%{"result" => ^value}]} =
                 Query.query(table,
                   filter: Expr.eq(Expr.col(name), value),
                   select: [result: Expr.col(name)]
                 )
      end)
    end
  end

  test "one expression model serves queries and every SQL write boundary", %{conn: conn} do
    assert {:ok, table} =
             Connection.create_table(conn, "expressions", [
               %{id: 1, value: 10, text: "BOOK"},
               %{id: 2, value: 20, text: "other"}
             ])

    id = Expr.col(:id)
    assert {:ok, 1} = Table.count_rows(table, Expr.eq(id, 1))

    predicate =
      Expr.all([
        Expr.gte(Expr.col(:value), 10),
        Expr.in_list(id, [1, 3]),
        Expr.eq(Expr.call(:lower, [Expr.col(:text)]), "book")
      ])

    assert {:ok, [%{"twice" => 20}]} =
             Query.query(table, filter: predicate, select: [twice: Expr.mul(Expr.col(:value), 2)])

    assert {:ok, %{updated: 1}} =
             Table.update(table, %{value: Expr.add(Expr.col(:value), 1)}, filter: Expr.eq(id, 1))

    assert {:ok, %{updated: 1}} =
             Table.merge(table, [%{id: 1, value: 30, text: "new"}],
               on: :id,
               when_matched_update_all:
                 Expr.gt(Expr.col(:value, relation: :source), Expr.col(:value, relation: :target))
             )

    assert {:ok, _} = SchemaEvolution.add_columns(table, doubled: Expr.mul(Expr.col(:value), 2))

    assert {:ok, [%{"doubled" => 60}]} =
             Query.query(table, filter: Expr.eq(id, 1), columns: [:doubled])

    assert {:ok, %{deleted: 1}} = Table.delete(table, Expr.neq(id, 1))
  end

  test "operators, nulls, function calls, dates and bytes", %{conn: conn} do
    assert {:ok, table} =
             Connection.create_table(
               conn,
               "values",
               [
                 %{
                   id: 1,
                   text: "Hello",
                   bytes: <<0, 255>>,
                   day: ~D[2024-02-29],
                   object: %{name: "child"}
                 }
               ],
               schema: [
                 id: :int64,
                 text: :string,
                 bytes: :binary,
                 day: :date32,
                 object: {:struct, [name: :string]}
               ]
             )

    id = Expr.col(:id)

    predicate =
      Expr.all([
        Expr.any([false, Expr.lt(id, 2)]),
        Expr.lte(id, 1),
        Expr.gt(id, 0),
        Expr.is_not_null(id),
        Expr.negate(Expr.is_null(id)),
        Expr.like(Expr.col(:text), "H%"),
        Expr.ilike(Expr.col(:text), "h%"),
        Expr.eq(Expr.col(:bytes), Expr.binary(<<0, 255>>)),
        Expr.eq(Expr.col(:day), ~D[2024-02-29])
      ])

    assert {:ok, [%{"child" => "child", "number" => 1}]} =
             Query.query(table,
               filter: predicate,
               select: [
                 child: Expr.field(Expr.col(:object), :name),
                 number: Expr.div(Expr.sub(3, id), 2)
               ]
             )

    assert {:ok, []} = Query.query(table, filter: Expr.in_list(id, []))
    assert Expr.to_sql(Expr.all([])) == "TRUE"
    assert Expr.to_sql(Expr.any([])) == "FALSE"
    assert Expr.to_sql(Expr.lit(nil)) == "NULL"
    assert {:ok, [%{"value" => 1.5}]} = Query.query(table, select: [value: Expr.lit(1.5)])
  end

  test "builders reject malformed input" do
    for fun <- [
          fn -> Expr.col("") end,
          fn -> Expr.lit(%{}) end,
          fn -> Expr.lit(<<255>>) end,
          fn -> Expr.binary(1) end,
          fn -> Expr.in_list(1, :bad) end,
          fn -> Expr.all(:bad) end,
          fn -> Expr.call("x); DROP TABLE t; --", []) end,
          fn -> Expr.call(:lower, :bad) end,
          fn -> Expr.to_sql(%Expr{node: :bad}) end
        ] do
      assert_raise ArgumentError, fun
    end
  end

  test "integer literals retain boundary precision and builders work in streaming queries", %{
    conn: conn
  } do
    min = -9_223_372_036_854_775_808
    max = 18_446_744_073_709_551_615

    assert {:ok, table} =
             Connection.create_table(conn, "limits", [%{signed: min, unsigned: max}],
               schema: [signed: :int64, unsigned: :uint64]
             )

    query = table |> Query.new() |> Query.where(Expr.eq(Expr.col(:unsigned), max))
    assert [%{"signed" => ^min, "unsigned" => ^max}] = query |> Query.stream() |> Enum.to_list()

    assert {:ok, [%{"min" => ^min, "max" => ^max}]} =
             Query.query(table,
               filter: Expr.eq(Expr.col(:signed), min),
               select: [min: Expr.lit(min), max: Expr.lit(max)]
             )

    assert_raise ArgumentError, fn -> Expr.lit(max + 1) end
    assert_raise ArgumentError, fn -> Expr.lit(min - 1) end
    assert Expr.to_sql(Expr.col("a`b")) == "`a``b`"
    assert Expr.to_sql(Expr.col("a.b")) == "`a.b`"
  end
end
