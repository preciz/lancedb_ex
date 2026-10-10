defmodule LanceDB.Expr do
  @moduledoc """
  Composable SQL expressions for filters, projections and writes.

  Column names are quoted exactly (including case and dots); values are always
  literals. Use `col/2` for a merge relation and `field/2` for a nested field.
  Expressions follow SQL null semantics: use `is_null/1`, not `eq(column, nil)`.
  Existing APIs continue to accept SQL strings as well.

      alias LanceDB.Expr, as: E
      predicate = E.all([E.gte(E.col(:price), 10), E.eq(E.col(:category), "books")])
      LanceDB.Query.query(table, filter: predicate, select: [cost: E.mul(E.col(:price), 2)])
      LanceDB.Table.update(table, %{price: E.add(E.col(:price), 1)}, filter: predicate)

  `call/2` invokes a scalar function registered in the pinned SDK. Function
  existence, argument types and column resolution are checked when executed.
  Builders raise `ArgumentError` for invalid inputs.
  """
  @enforce_keys [:node]
  defstruct [:node]
  @opaque t :: %__MODULE__{node: term()}
  @type input :: t() | nil | boolean() | number() | String.t() | Date.t()

  @doc "References an exact column name, optionally qualified by a relation (such as source or target)."
  @spec col(atom() | String.t(), keyword()) :: t()
  def col(name, opts \\ []) do
    opts = Keyword.validate!(opts, [:relation])
    parts = if opts[:relation], do: [opts[:relation], name], else: [name]
    %__MODULE__{node: {:column, Enum.map(parts, &LanceDB.Options.column_name!/1)}}
  end

  @doc "Builds a literal; strings are UTF-8 text. Use binary/1 for arbitrary bytes."
  @spec lit(input()) :: t()
  def lit(%__MODULE__{} = expression), do: expression
  def lit(nil), do: %__MODULE__{node: {:literal, nil}}

  def lit(value)
      when is_integer(value) and
             (value < -9_223_372_036_854_775_808 or value > 18_446_744_073_709_551_615),
      do: raise(ArgumentError, "integer literals must fit int64 or uint64")

  def lit(value) when is_boolean(value) or is_number(value),
    do: %__MODULE__{node: {:literal, value}}

  def lit(%Date{calendar: Calendar.ISO} = date),
    do: %__MODULE__{node: {:date, Date.to_iso8601(date)}}

  def lit(value) when is_binary(value) do
    if !String.valid?(value),
      do: raise(ArgumentError, "text literals must be valid UTF-8; use Expr.binary/1 for bytes")

    %__MODULE__{node: {:literal, value}}
  end

  def lit(_), do: raise(ArgumentError, "unsupported expression literal")

  @doc "Builds an arbitrary binary literal."
  @spec binary(binary()) :: t()
  def binary(value) when is_binary(value), do: %__MODULE__{node: {:binary, Base.encode16(value)}}
  def binary(_), do: raise(ArgumentError, "binary literals require bytes")

  @doc "References a child of a struct expression."
  @spec field(input(), atom() | String.t()) :: t()
  def field(expression, name),
    do: call(:get_field, [expression, LanceDB.Options.column_name!(name)])

  for {name, operator} <- [
        eq: "=",
        neq: "<>",
        gt: ">",
        gte: ">=",
        lt: "<",
        lte: "<=",
        add: "+",
        sub: "-",
        mul: "*",
        div: "/",
        like: "LIKE",
        ilike: "ILIKE"
      ] do
    @doc "Combines two expressions using SQL `#{operator}`. Plain values become literals."
    @spec unquote(name)(input(), input()) :: t()
    def unquote(name)(left, right),
      do: %__MODULE__{node: {:operator, unquote(operator), lit(left), lit(right)}}
  end

  @doc "Combines predicates with AND. An empty list is true."
  @spec all([input()]) :: t()
  def all(values), do: combine(values, "AND", true)

  @doc "Combines predicates with OR. An empty list is false."
  @spec any([input()]) :: t()
  def any(values), do: combine(values, "OR", false)

  defp combine(values, op, empty) when is_list(values),
    do:
      Enum.reduce(values, nil, fn value, acc ->
        if acc, do: %__MODULE__{node: {:operator, op, acc, lit(value)}}, else: lit(value)
      end) || lit(empty)

  defp combine(_, _, _), do: raise(ArgumentError, "predicates must be a list")

  @doc "Negates a predicate using SQL NOT."
  @spec negate(input()) :: t()
  def negate(value), do: %__MODULE__{node: {:not, lit(value)}}

  @doc "Tests whether an expression is SQL NULL."
  @spec is_null(input()) :: t()
  # This builds an expression, not an Elixir boolean predicate.
  # credo:disable-for-next-line Credo.Check.Readability.PredicateFunctionNames
  def is_null(value), do: %__MODULE__{node: {:is_null, lit(value)}}

  @doc "Tests whether an expression is non-null."
  @spec is_not_null(input()) :: t()
  # This builds an expression, not an Elixir boolean predicate.
  # credo:disable-for-next-line Credo.Check.Readability.PredicateFunctionNames
  def is_not_null(value), do: negate(is_null(value))

  @doc "Tests membership in a list of expressions or literals. An empty list is false."
  @spec in_list(input(), [input()]) :: t()
  def in_list(value, values) when is_list(values),
    do: %__MODULE__{node: {:in, lit(value), Enum.map(values, &lit/1)}}

  def in_list(_, _), do: raise(ArgumentError, "membership values must be a list")

  @doc "Invokes a registered scalar function with expression or literal arguments."
  @spec call(atom() | String.t(), [input()]) :: t()
  def call(name, args) when is_list(args) do
    name = LanceDB.Options.column_name!(name)

    if !Regex.match?(~r/\A[a-zA-Z_][a-zA-Z_0-9]*\z/, name),
      do: raise(ArgumentError, "function names must be SQL identifiers")

    %__MODULE__{node: {:call, name, Enum.map(args, &lit/1)}}
  end

  def call(_, _), do: raise(ArgumentError, "function arguments must be a list")

  @doc "Serializes an expression to SQL, quoting identifiers and literals."
  @spec to_sql(t()) :: String.t()
  def to_sql(%__MODULE__{node: node}), do: render(node)

  defp render({:column, parts}), do: Enum.map_join(parts, ".", &quote_identifier/1)
  defp render({:literal, nil}), do: "NULL"
  defp render({:literal, true}), do: "TRUE"
  defp render({:literal, false}), do: "FALSE"

  defp render({:literal, value}) when is_integer(value) do
    type = if value > 9_223_372_036_854_775_807, do: "UInt64", else: "Int64"
    "arrow_cast('#{value}', '#{type}')"
  end

  defp render({:literal, value}) when is_float(value), do: "arrow_cast('#{value}', 'Float64')"
  defp render({:literal, value}) when is_binary(value), do: quote_string(value)
  defp render({:date, value}), do: "DATE " <> quote_string(value)
  defp render({:binary, hex}), do: "decode(#{quote_string(hex)}, 'hex')"

  defp render({:operator, op, left, right})
       when op in [
              "=",
              "<>",
              ">",
              ">=",
              "<",
              "<=",
              "+",
              "-",
              "*",
              "/",
              "LIKE",
              "ILIKE",
              "AND",
              "OR"
            ],
       do: "(#{to_sql(left)} #{op} #{to_sql(right)})"

  defp render({:not, value}), do: "(NOT #{to_sql(value)})"
  defp render({:is_null, value}), do: "(#{to_sql(value)} IS NULL)"
  defp render({:in, _, []}), do: "FALSE"

  defp render({:in, value, values}),
    do: "(#{to_sql(value)} IN (#{Enum.map_join(values, ", ", &to_sql/1)}))"

  defp render({:call, name, args}),
    do: "#{quote_identifier(name)}(#{Enum.map_join(args, ", ", &to_sql/1)})"

  defp render(_), do: raise(ArgumentError, "invalid expression")

  defp quote_identifier(value), do: "`" <> String.replace(value, "`", "``") <> "`"
  defp quote_string(value), do: "'" <> String.replace(value, "'", "''") <> "'"
end
