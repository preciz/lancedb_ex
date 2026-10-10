defmodule LanceDB.Schema do
  alias LanceDB.Options

  @moduledoc """
  Reusable schemas contain Field structs, schema metadata, and tagged logical types.
  Build one with `Schema.new/2`; `LanceDB.Table.schema/1` returns the same representation.
  Non-empty lists of field definitions are accepted as creation shorthand.

      [id: :int64, text: :string, vector: {:vector, 3}]

  For nullability controls, use maps (which may be mixed with tuples):

      [%{name: :id, type: :int64, nullable: false}, {:text, :string}]

  Field maps also accept `:metadata`, a map with string or atom keys and UTF-8
  string values. This works at every struct depth. Table-level metadata is supplied
  through the `:schema_metadata` creation option. `LanceDB.Table.schema/1` includes
  schema and field metadata, including empty maps.

  Supported primitive types: `:string`, `:binary`, `:boolean`, `:float16`, `:float32`, `:float64`,
  `:int8`, `:int16`, `:int32`, `:int64`, `:uint8`, `:uint16`, `:uint32`, `:uint64`,
  `:date32`, and `:date64`. Other types are
  `{:vector, positive_dimension}` (fixed-size float32 lists),
  `{:vector, positive_dimension, element_type}` with `:float16`, `:float32`,
  `:float64`, or `:uint8`,
  `{:timestamp, unit}`, `{:timestamp, unit, timezone}`, `{:list, type}`,
  `{:list, type, item_nullable: false}`, and `{:struct, fields}`.
  Lists and structs can nest; struct fields use the same definitions as table fields.
  List items are nullable by default, independently of the list field's nullability.
  Lists may be empty. Struct values are maps and must supply every child field,
  including explicit nil values. Atom/string aliases must be unique at every level.
  Lists and structs require explicit schemas; numeric-list inference remains vectors.

  Lance 10 cannot store a null struct with required children, including through
  nested struct fields. Supply a complete struct or make its child fields nullable.
  Lance 10 also cannot cast struct update literals to required child fields;
  use upsert or merge_insert with complete records for those columns. This also
  applies to field names that cannot round trip through Arrow's SQL type parser,
  such as double-quoted names; literal updates reject them before execution.
  Literal updates replace whole top-level collections; nested-path updates are
  not exposed. Schema evolution supports nested rename/drop/nullability changes.

  Integer fields accept integers within their signed or unsigned storage range,
  without rounding or coercion. Binary fields accept ordinary Elixir binaries,
  including empty binaries and arbitrary non-UTF-8 bytes; results return binaries.
  String fields require valid UTF-8. These types also work inside lists and structs.

  Blob fields use `:blob` and accept binaries, nil, or `LanceDB.Blob.reference/1`.
  They also nest inside lists and structs. Byte fetching supports top-level and
  struct-nested blobs; Lance 10 cannot address individual list elements by row ID.
  Table queries return small descriptors;
  retrieve bytes with `LanceDB.Blob.fetch/3`, `LanceDB.Blob.fetch_ranges/3`, or
  `LanceDB.Blob.open/3`. A descriptor cannot be written back as a blob value.
  The Arrow extension marker is managed internally and omitted from logical blob
  field metadata. Blob types require explicit schemas and cannot be introduced
  by casting another field's type. See `LanceDB.Blob` for streaming and cursor reads.

  Date fields accept `Calendar.ISO` Date structs, `YYYY-MM-DD` strings, or epoch
  integers: days for `:date32`, midnight milliseconds for `:date64`. Years must be
  0001–9999; invalid dates and fractional days are rejected. Both return ISO date
  strings. Dates, binary fields and additional integer widths require explicit
  schemas; inference continues to produce strings and signed int64 integers.

  Timestamp units are `:second`, `:millisecond`, `:microsecond`, and `:nanosecond`.
  A timezone is nil (wall-clock time), `"UTC"`, an IANA name such as
  `"Europe/Vienna"`, or a fixed offset such as `"+02:00"`.

  Timestamp values accept signed epoch integers in the field's unit, ISO 8601
  strings, and `Calendar.ISO` Elixir date-time structs. Timezone-free fields use
  `NaiveDateTime` or strings without offsets; fields with a timezone use
  `DateTime` or strings with `Z`/an explicit offset. Subsecond precision must fit
  the unit without rounding. Years are limited to 0001–9999 and the signed
  64-bit storage range (about 1677–2262 for nanoseconds). Leap seconds are rejected.
  Query results contain ISO strings: UTC with `Z` for timezone-bearing fields,
  no offset for timezone-free fields. Nanoseconds remain exact in string results.
  Timestamp types are never inferred from strings or date-time structs.

  Additional explicit types:

    * `{:decimal128, precision, scale}` and `{:decimal256, precision, scale}` use
      exact plain decimal strings. Precision is 1–38 or 1–76; scale is -128 through
      precision. Floats, overflow and lossy fractions are rejected. Reads return
      strings. Negative-scale decimal literal updates require upsert or merge.
    * `{:time32, :second | :millisecond}` and `{:time64, :microsecond | :nanosecond}`
      accept ISO time strings, ISO `Time` structs, or integer ticks since midnight.
      Values must fit one day exactly. Reads return ISO strings.
    * `{:duration, unit}` accepts and returns signed int64 ticks.
    * `{:fixed_size_binary, width}` requires exactly that many bytes.
    * `:large_string`, `:large_binary`, and `{:large_list, item}` use 64-bit offsets.
      Large list items accept the same nullability and Field forms as ordinary lists.
    * `{:dictionary, integer_key_type, scalar_value_type}` accepts and returns
      decoded string, binary or numeric values, retaining dictionary encoding in
      Arrow. Key types are signed/unsigned integer widths.
    * `{:map, key_type, value_type}` uses key/value pairs, returning pairs even for
      string keys. String/atom-keyed maps are also accepted as input. Keys are
      required and unique; values are nullable by default. Custom key/value Fields
      or `{:map, entries_field}` preserve nested properties. Maps require storage
      format 2.2 or newer, and sorted maps are unsupported. Use upsert or merge for
      whole-map literal updates. Collection types may nest.
    * `:json` stores native Lance JSONB and accepts JSON values. SQL NULL is nil;
      `LanceDB.JSON.null/0` preserves a top-level JSON null. Reads return decoded
      values. See `LanceDB.JSON` for the value contract. Arrow extension metadata
      is preserved in ArrowSchema and managed by logical JSON fields.

  Binary vectors use uint8 elements: lists of integers from 0 through 255, with
  dimensions measured in bytes. Their search/index distance must be `:hamming`.
  Floating vector writes and queries reject values outside the element type's
  finite range; float16/float32 storage rounds to that type's precision. Float64
  queries preserve their precision. Batch search supports all four element types;
  document multivector search remains float32 with the pinned upstream SDK.

  Arrow batches may describe custom vector item fields using
  `{:vector, dimension, %LanceDB.Field{type: :float32}}`. Table creation/evolution
  reject nonstandard vector item names, nullability or metadata because Lance 10
  cannot persist those properties. Unsupported external Arrow types are described
  explicitly as `{:unsupported, description}` and cannot be silently recreated.

  Fields are nullable by default. Every write must supply every field,
  including explicit `nil` values for nulls. Vector elements cannot be null.
  """

  alias LanceDB.Field
  defstruct fields: [], metadata: %{}

  @type primitive ::
          :string
          | :large_string
          | :large_binary
          | :json
          | :binary
          | :blob
          | :boolean
          | :float16
          | :float32
          | :float64
          | :int8
          | :int16
          | :int32
          | :int64
          | :uint8
          | :uint16
          | :uint32
          | :uint64
          | :date32
          | :date64
  @type unit :: :second | :millisecond | :microsecond | :nanosecond
  @type logical_type ::
          primitive()
          | {:vector, pos_integer()}
          | {:vector, pos_integer(), primitive() | Field.t()}
          | {:timestamp, unit(), String.t() | nil}
          | {:list, Field.t()}
          | {:large_list, Field.t()}
          | {:fixed_size_binary, pos_integer()}
          | {:decimal128 | :decimal256, pos_integer(), integer()}
          | {:time32 | :time64 | :duration, unit()}
          | {:dictionary, primitive(), primitive()}
          | {:map, Field.t()}
          | {:struct, [Field.t()]}
          | {:unsupported, String.t()}
  @type t :: %__MODULE__{fields: [Field.t()], metadata: map()}

  @doc "Builds a schema directly from logical field definitions. Invalid definitions raise ArgumentError."
  @spec new(list(), map()) :: t()
  def new(fields, metadata \\ %{}),
    do: %__MODULE__{
      fields: fields!(fields),
      metadata: Options.metadata!(metadata)
    }

  defp fields!([_ | _] = fields) do
    fields = Enum.map(fields, &field!/1)
    names = Enum.map(fields, & &1.name)

    if length(names) != length(Enum.uniq(names)),
      do: raise(ArgumentError, "duplicate schema fields")

    fields
  end

  defp fields!(_), do: raise(ArgumentError, "schema must be a non-empty list of fields")

  defp field!(%Field{} = field), do: field!(Map.from_struct(field))
  defp field!({name, type}), do: field!(%{name: name, type: type})

  defp field!(%{name: name, type: type} = field) when map_size(field) in [2, 3, 4] do
    if !Enum.all?(Map.keys(field), &(&1 in [:name, :type, :nullable, :metadata])),
      do: raise(ArgumentError, "unknown schema field option")

    name = LanceDB.Options.column_name!(name)
    nullable = Map.get(field, :nullable, true)
    if not is_boolean(nullable), do: raise(ArgumentError, "nullable must be a boolean")

    %Field{
      name: name,
      type: normalize_type!(type),
      nullable: nullable,
      metadata: Options.metadata!(Map.get(field, :metadata, %{}))
    }
  end

  defp field!(_), do: raise(ArgumentError, "invalid schema field definition")

  defp map_field!(%Field{} = field, _name, _nullable), do: field!(field)

  defp map_field!(type, name, nullable),
    do: field!(%Field{name: name, type: type, nullable: nullable})

  @primitives ~w(string large_string binary large_binary blob json date32 date64 int8 int16 int32 int64 uint8 uint16 uint32 uint64 float16 float32 float64 boolean)a
  @units ~w(second millisecond microsecond nanosecond)a

  @doc false
  def normalize_type!(type) when type in @primitives, do: type

  def normalize_type!({kind, precision, scale})
      when kind in [:decimal128, :decimal256] and is_integer(precision) and
             is_integer(scale) do
    maximum = if kind == :decimal128, do: 38, else: 76

    if !(precision in 1..maximum and scale >= -128 and scale <= precision),
      do: raise(ArgumentError, "invalid decimal precision or scale")

    {kind, precision, scale}
  end

  def normalize_type!({:fixed_size_binary, width})
      when is_integer(width) and width > 0 and width <= 2_147_483_647,
      do: {:fixed_size_binary, width}

  def normalize_type!({:time32, unit}) when unit in [:second, :millisecond], do: {:time32, unit}

  def normalize_type!({:time64, unit}) when unit in [:microsecond, :nanosecond],
    do: {:time64, unit}

  def normalize_type!({:duration, unit}) when unit in @units, do: {:duration, unit}

  def normalize_type!({:large_list, type}) do
    {:list, item} = normalize_type!({:list, type})
    {:large_list, item}
  end

  def normalize_type!({:large_list, type, opts}) do
    {:list, item} = normalize_type!({:list, type, opts})
    {:large_list, item}
  end

  @dictionary_keys ~w(int8 int16 int32 int64 uint8 uint16 uint32 uint64)a
  @dictionary_values @dictionary_keys ++
                       ~w(string large_string binary large_binary float16 float32 float64)a
  def normalize_type!({:dictionary, key, value})
      when key in @dictionary_keys and value in @dictionary_values,
      do: {:dictionary, key, value}

  def normalize_type!({:map, key, value}) do
    key = map_field!(key, "key", false)
    value = map_field!(value, "value", true)

    normalize_type!(
      {:map, %Field{name: "entries", type: {:struct, [key, value]}, nullable: false}}
    )
  end

  def normalize_type!({:map, %Field{} = entries}) do
    entries = field!(entries)

    case entries do
      %Field{nullable: false, type: {:struct, [%Field{nullable: false}, %Field{}]}} ->
        {:map, entries}

      _ ->
        raise ArgumentError,
              "map entries must be a non-nullable struct with a required key and a value"
    end
  end

  def normalize_type!({:vector, dimension})
      when is_integer(dimension) and dimension > 0 and dimension <= 2_147_483_647,
      do: {:vector, dimension}

  def normalize_type!({:vector, dimension, element})
      when element in [:float16, :float32, :float64, :uint8],
      do:
        normalize_type!({:vector, dimension, %Field{name: "item", type: element, nullable: true}})

  def normalize_type!({:vector, dimension, %Field{type: element} = item})
      when element in [:float16, :float32, :float64, :uint8] do
    {:vector, dimension} = normalize_type!({:vector, dimension})

    case field!(item) do
      %Field{name: "item", type: :float32, nullable: true, metadata: metadata}
      when map_size(metadata) == 0 ->
        {:vector, dimension}

      %Field{name: "item", type: type, nullable: true, metadata: metadata}
      when map_size(metadata) == 0 ->
        {:vector, dimension, type}

      field ->
        {:vector, dimension, field}
    end
  end

  def normalize_type!({:list, %Field{} = item}), do: {:list, field!(item)}
  def normalize_type!({:list, type}), do: normalize_type!({:list, type, []})

  def normalize_type!({:list, type, opts}) do
    opts = Keyword.validate!(opts, [:item_nullable])

    {:list,
     field!(%{name: "item", type: type, nullable: Keyword.get(opts, :item_nullable, true)})}
  end

  def normalize_type!({:struct, fields}), do: {:struct, fields!(fields)}
  def normalize_type!({:timestamp, unit}), do: normalize_type!({:timestamp, unit, nil})

  def normalize_type!({:timestamp, unit, timezone}) when unit in @units do
    if !(is_nil(timezone) or
           (is_binary(timezone) and String.valid?(timezone) and String.trim(timezone) != "")),
       do: raise(ArgumentError, "timestamp timezone must be nil or a non-empty string")

    {:timestamp, unit, timezone}
  end

  def normalize_type!(_),
    do: raise(ArgumentError, "unsupported schema type, timestamp unit, or vector dimension")
end
