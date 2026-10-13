defmodule LanceDB.SchemaEvolution do
  @moduledoc "SQL and Expr transformations, column sources and snapshot backfills."
  alias LanceDB.Operations.SchemaEvolution
  @type table :: LanceDB.Table.t()
  @type result(value) :: LanceDB.result(value)
  @doc """
  Adds columns from SQL strings or `LanceDB.Expr` values for existing rows.

  Supply a map or list of `{name, expression}` pairs, for example:
  `[doubled: "score * 2", status: "'new'", note: "CAST(NULL AS STRING)"]`.
  String expressions are SQL, including quoted string literals. Result types and
  field properties must meet the package's storage requirements. The operation
  fills existing rows once. It does not set defaults for future rows or create
  generated columns. Each expression uses the schema before the change.
  The result is a WriteResult with the new version. The operation cannot
  overwrite existing columns.
  """
  @spec add_columns(table(), map() | list()) :: result(LanceDB.WriteResult.t())
  def add_columns(table, expressions) do
    LanceDB.Execution.write(
      :add_columns,
      fn -> SchemaEvolution.add_columns(table, expressions) end,
      &LanceDB.WriteResult.version/2
    )
  end

  @doc """
  Renames columns, changes nullability, or converts top-level column types.

  Supply a map or list of `{name, options}` pairs, such as:
  `[text: [rename: :title], score: [nullable: false]]`.
  Each entry must specify `:rename`, `:nullable`, or `:type`.
  The `:type` option accepts a `LanceDB.Schema` type. A change to non-nullable
  fails if null values exist. Rename and nullability changes accept nested paths,
  such as `"data.count"`. Use Lance backtick syntax to quote literal dots and
  backticks. Rename targets must be leaf names without dots.
  The operation rejects duplicate paths, overlapping paths, and duplicate names
  under the same parent before it changes the schema.

  Type conversions use the SDK's compatible type families. These include integer
  to integer, float to float, compatible date and time types, and compatible
  complete lists.
  Drop indexes on converted columns before the conversion. Create the indexes
  again after the conversion. Integer overflow returns an error without a commit.
  Float and time conversions can lose precision. Nested struct fields can be
  converted without changing their siblings. Casts must also keep the
  field's extension markers compatible with its storage type.
  The result is a WriteResult with the new version.
  """
  @spec alter_columns(table(), map() | list()) :: result(LanceDB.WriteResult.t())
  def alter_columns(table, changes) do
    LanceDB.Execution.write(
      :alter_columns,
      fn -> SchemaEvolution.alter_columns(table, changes) end,
      &LanceDB.WriteResult.version/2
    )
  end

  @doc """
  Drops columns or nested fields and returns a WriteResult with the committed version.

  Indexes on dropped columns are removed by the SDK. Current rows retain their
  remaining fields. Old versions may retain the dropped data; compaction and
  version cleanup are separate operations. At least one column must remain.
  Paths use the same syntax as `alter_columns/2`; overlapping paths are rejected.
  """
  @spec drop_columns(table(), list()) :: result(LanceDB.WriteResult.t())
  def drop_columns(table, columns) do
    LanceDB.Execution.write(
      :drop_columns,
      fn -> SchemaEvolution.drop_columns(table, columns) end,
      &LanceDB.WriteResult.version/2
    )
  end

  @doc """
  Adds columns from a Source with an explicit output schema in physical row order.
  Supply one output row per visible row, excluding deleted rows. The optional
  positive `:expected_version` guards against changes to the target version.
  Row counts and successful source EOF are checked before committing.
  """
  @spec add_from_source(table(), LanceDB.Source.t(), keyword()) ::
          result(LanceDB.WriteResult.t())
  def add_from_source(table, source, opts \\ []) do
    LanceDB.Execution.write(
      :add_columns,
      fn -> SchemaEvolution.columns(table, source, opts) end,
      &LanceDB.WriteResult.version/2
    )
  end

  @doc """
  Maps snapshot batches to new columns, preserving row count and order.

  The output schema accepts a `LanceDB.Schema` or a list of field definitions.
  The mapper runs in the caller and returns a native Batch or row list for the
  new columns. `:read_columns` selects input fields; `:batch_size` defaults to
  1024. The operation guards the snapshot version and checks successful source
  EOF before committing. Returns a `LanceDB.WriteResult`.
  """
  @spec backfill_columns(
          table(),
          LanceDB.Schema.t() | list(),
          (LanceDB.Batch.t() -> LanceDB.Batch.t() | [map()]),
          keyword()
        ) :: result(LanceDB.WriteResult.t())
  def backfill_columns(table, schema, mapper, opts \\ []) do
    LanceDB.Execution.write(
      :backfill_columns,
      fn -> SchemaEvolution.backfill_columns(table, schema, mapper, opts) end,
      &LanceDB.WriteResult.version/2
    )
  end
end
