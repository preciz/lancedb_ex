defmodule LanceDB.BlobTest do
  use LanceDB.DataCase, async: true
  use ExUnitProperties
  alias LanceDB.{Batch, Blob, Connection, Error, Query, Schema, Source, Table}

  setup %{path: path} do
    {:ok, conn} = Connection.connect(path)
    %{conn: conn, path: path, schema: Schema.new(id: :int64, payload: :blob)}
  end

  test("blob rows and Arrow sources preserve nulls, empty bytes and schema", %{
    conn: conn,
    schema: schema
  }) do
    rows = [%{id: 1, payload: <<0, 255, 128, 1>>}, %{id: 2, payload: nil}, %{id: 3, payload: ""}]
    {:ok, batch} = Batch.from_rows(rows, schema)
    assert {:ok, ^schema} = LanceDB.TestSupport.logical_schema(Batch.schema(batch))
    assert {:ok, ipc} = Batch.to_ipc(batch)
    assert {:ok, [imported]} = Batch.from_ipc(ipc)
    assert {:ok, ^schema} = LanceDB.TestSupport.logical_schema(Batch.schema(imported))

    for {input, n} <-
          Enum.with_index([
            Source.rows(rows, schema: schema, batch_size: 1),
            Source.batches([imported], schema: schema)
          ]) do
      assert {:ok, table} = Connection.create_table(conn, "blobs_#{n}", input)
      assert {:ok, ^schema} = Table.schema(table)
      assert {:ok, ["payload"]} = LanceDB.Blob.columns(table)
      assert {:ok, found} = Query.query(table, with_row_id: true, order_by: [asc: :id])
      [first, second, third] = Enum.map(found, & &1["_rowid"])
      assert is_map(hd(found)["payload"])

      assert {:ok, ["", <<0, 255, 128, 1>>, nil, <<0, 255, 128, 1>>]} =
               LanceDB.Blob.fetch(table, :payload, [third, first, second, first])

      assert {:ok, [<<255, 128>>, nil, ""]} =
               LanceDB.Blob.fetch_ranges(table, :payload, [
                 {first, 1, 2},
                 {second, 0, 0},
                 {third, 0, 0}
               ])

      assert {:ok, []} = LanceDB.Blob.fetch(table, :payload, [])
      assert {:ok, []} = LanceDB.Blob.fetch_ranges(table, :payload, [])
      assert {:ok, []} = LanceDB.Blob.open(table, :payload, [])
      assert {:ok, _} = Table.add(table, [%{id: 4, payload: "later"}])
      assert {:ok, _} = Table.add(table, Source.batches([batch]))
      assert {:ok, 7} = Table.count_rows(table)
      assert {:ok, reopened} = Connection.open_table(conn, "blobs_#{n}")
      assert {:ok, ^schema} = Table.schema(reopened)
      assert {:ok, [<<0, 255, 128, 1>>]} = LanceDB.Blob.fetch(reopened, :payload, [first])

      assert {:error, %Error{operation: :fetch_blob_ranges}} =
               LanceDB.Blob.fetch_ranges(table, :payload, [{first, 3, 2}])

      assert {:error, %Error{}} = LanceDB.Blob.fetch(table, :id, [first])
      assert {:error, %Error{}} = LanceDB.Blob.fetch(table, :missing, [first])
      assert {:error, %Error{}} = LanceDB.Blob.fetch(table, :payload, [9999])
    end
  end

  test("file handles have synchronized cursors, independent ranges and explicit close", %{
    conn: conn,
    schema: schema
  }) do
    {:ok, table} =
      Connection.create_table(
        conn,
        "handles",
        [%{id: 1, payload: "abcdef"}, %{id: 2, payload: nil}],
        schema: schema
      )

    {:ok, rows} = Query.query(table, with_row_id: true, order_by: [asc: :id])
    [id, null_id] = Enum.map(rows, & &1["_rowid"])
    assert {:ok, [blob, nil]} = LanceDB.Blob.open(table, :payload, [id, null_id])
    assert %Blob{} = blob
    refute Blob.closed?(blob)
    assert Blob.size(blob) == 6
    assert {:ok, %{"size" => 6, "kind" => "inline", "uri" => nil}} = Blob.info(blob)
    assert {:ok, 0} = Blob.tell(blob)
    assert {:ok, "ab"} = Blob.read(blob, 2)
    assert {:ok, "de"} = Blob.read_range(blob, 3, 2)
    assert {:ok, ["ef", "ab", ""]} = Blob.read_ranges(blob, [{4, 2}, {0, 2}, {6, 0}])
    assert {:ok, []} = Blob.read_ranges(blob, [])
    assert {:ok, 2} = Blob.tell(blob)
    assert {:ok, "c"} = Task.async(fn -> Blob.read(blob, 1) end) |> Task.await()
    assert {:ok, 3} = Blob.tell(blob)
    assert {:ok, [other]} = LanceDB.Blob.open(table, :payload, [id])
    assert {:ok, 0} = Blob.tell(other)
    assert {:ok, :ok} = Blob.seek(blob, 4)
    assert {:ok, "ef"} = Blob.read(blob)
    assert {:ok, ""} = Blob.read(blob, 3)
    assert {:ok, :ok} = Blob.seek(blob, 100)
    assert {:ok, ""} = Blob.read(blob)
    assert {:ok, 6} = Blob.tell(blob)
    assert {:error, %Error{kind: :invalid_data}} = Blob.read_range(blob, 5, 2)
    assert {:ok, :ok} = Blob.seek(blob, 1)
    stream = Blob.stream(blob, chunk_size: 2)
    assert Enum.take(stream, 1) == ["ab"]
    refute Blob.closed?(blob)
    assert Enum.to_list(stream) == ["ab", "cd", "ef"]
    assert Enum.to_list(Blob.stream(blob)) == ["abcdef"]
    assert {:ok, 1} = Blob.tell(blob)
    assert {:ok, :ok} = Blob.close(blob)
    assert {:ok, :ok} = Blob.close(blob)
    assert Blob.closed?(blob)
    assert Blob.size(blob) == 6

    for result <- [
          Blob.read(blob),
          Blob.read(blob, 1),
          Blob.read_range(blob, 0, 1),
          Blob.tell(blob),
          Blob.seek(blob, 0)
        ] do
      assert {:error, %Error{kind: :invalid_data}} = result
    end

    assert_raise Error, fn -> Enum.to_list(stream) end
    assert {:ok, "abcdef"} = Blob.read(other)
    assert {:ok, :ok} = Blob.close(other)
  end

  test("external references and nested blob fields", %{conn: conn, path: path}) do
    file = Path.join(path, "external.bin")
    File.write!(file, <<0, 255, 1, 2>>)
    uri = "file://" <> file
    schema = Schema.new(id: :int64, info: {:struct, image: :blob})

    assert {:ok, table} =
             Connection.create_table(
               conn,
               "external",
               [%{id: 1, info: %{image: Blob.reference(uri)}}],
               schema: schema,
               allow_external_blob_outside_bases: true
             )

    assert {:ok, ^schema} = Table.schema(table)
    assert {:ok, ["info.image"]} = LanceDB.Blob.columns(table)
    {:ok, [%{"_rowid" => id}]} = Query.query(table, with_row_id: true)
    assert {:ok, [<<0, 255, 1, 2>>]} = LanceDB.Blob.fetch(table, "info.image", [id])
    assert {:ok, [blob]} = LanceDB.Blob.open(table, "info.image", [id])
    assert {:ok, %{"kind" => "external", "uri" => ^uri}} = Blob.info(blob)
    assert {:ok, <<255, 1>>} = Blob.read_range(blob, 1, 2)
    assert {:ok, :ok} = Blob.close(blob)
  end

  test("external ingest copies bytes and sidecar layouts support ranges", %{
    conn: conn,
    path: path
  }) do
    file = Path.join(path, "ingest.bin")
    bytes = :binary.copy(<<0, 255, 1>>, 100)
    File.write!(file, bytes)

    schema =
      Schema.new([
        {:id, :int64},
        %{
          name: :payload,
          type: :blob,
          metadata: %{
            "lance-encoding:blob-inline-size-threshold" => "8",
            "lance-encoding:blob-dedicated-size-threshold" => "32"
          }
        }
      ])

    input = [%{id: 1, payload: Blob.reference("file://" <> file)}]

    assert {:ok, table} =
             Connection.create_table(conn, "ingested", input,
               schema: schema,
               external_blob_mode: :ingest,
               blob_pack_file_size_threshold: 128
             )

    assert {:ok, _} =
             Table.add(table, [%{id: 2, payload: Blob.reference("file://" <> file)}],
               external_blob_mode: :ingest
             )

    assert {:ok, _} =
             Table.add(table, [%{id: 3, payload: :binary.copy(<<255>>, 16)}],
               blob_pack_file_size_threshold: 128
             )

    File.rm!(file)
    {:ok, rows} = Query.query(table, with_row_id: true, order_by: [asc: :id])
    ids = Enum.map(rows, & &1["_rowid"])
    assert {:ok, ^schema} = Table.schema(table)
    assert {:ok, [^bytes, ^bytes, packed]} = LanceDB.Blob.fetch(table, :payload, ids)
    assert packed == :binary.copy(<<255>>, 16)
    assert {:ok, [dedicated, _, packed_handle]} = LanceDB.Blob.open(table, :payload, ids)
    assert {:ok, %{"kind" => "dedicated"}} = Blob.info(dedicated)
    assert {:ok, %{"kind" => "packed"}} = Blob.info(packed_handle)
    assert {:ok, <<255, 1, 0, 255>>} = Blob.read_range(dedicated, 1, 4)
    assert {:ok, <<255, 255>>} = Blob.read_range(packed_handle, 2, 2)
    assert {:ok, :ok} = Blob.close(dedicated)
    assert {:ok, :ok} = Blob.close(packed_handle)
  end

  test("blob upserts retain historical bytes and column renames preserve reads", %{
    conn: conn,
    schema: schema
  }) do
    {:ok, table} =
      Connection.create_table(conn, "changes", [%{id: 1, payload: "before"}], schema: schema)

    {:ok, version} = LanceDB.History.version(table)
    {:ok, snapshot} = LanceDB.History.snapshot(table, version)
    {:ok, [%{"_rowid" => old_id}]} = Query.query(snapshot, with_row_id: true)

    assert {:ok, %{updated: 1, inserted: 1}} =
             Table.upsert(table, [%{id: 1, payload: "after"}, %{id: 2, payload: <<255>>}])

    {:ok, rows} = Query.query(table, with_row_id: true, order_by: [asc: :id])
    ids = Enum.map(rows, & &1["_rowid"])
    assert {:ok, ["after", <<255>>]} = LanceDB.Blob.fetch(table, :payload, ids)
    assert {:ok, ["before"]} = LanceDB.Blob.fetch(snapshot, :payload, [old_id])
    assert {:ok, _} = LanceDB.SchemaEvolution.alter_columns(table, payload: [rename: :renamed])
    assert {:ok, ["renamed"]} = LanceDB.Blob.columns(table)
    assert {:ok, ["after", <<255>>]} = LanceDB.Blob.fetch(table, :renamed, ids)

    assert {:error, %Error{kind: :invalid_data}} =
             LanceDB.SchemaEvolution.alter_columns(table, id: [type: :blob])
  end

  test("list blob schemas retain their logical type and discoverable paths", %{conn: conn}) do
    schema = Schema.new(id: :int64, images: {:list, :blob})

    assert {:ok, table} =
             Connection.create_table(conn, "list", [%{id: 1, images: ["one", "two", nil]}],
               schema: schema
             )

    assert {:ok, ^schema} = Table.schema(table)
    assert {:ok, ["images.item"]} = LanceDB.Blob.columns(table)
    assert {:ok, [%{"images" => [first, second, nil]}]} = Query.query(table)
    assert is_map(first)
    assert is_map(second)
    assert {:error, %Error{}} = LanceDB.Blob.fetch(table, "images.item", [0])
  end

  test("query descriptor batches cannot be mistaken for writable blob payloads", %{
    conn: conn,
    schema: schema
  }) do
    {:ok, table} =
      Connection.create_table(conn, "descriptor_source", [%{id: 1, payload: "bytes"}],
        schema: schema
      )

    [batch] = table |> Query.new() |> Query.stream_batches() |> Enum.to_list()
    {:ok, descriptor_schema} = Batch.schema(batch)

    assert {:error, %Error{kind: :invalid_data}} =
             LanceDB.ArrowSchema.to_schema(descriptor_schema)

    assert_raise ArgumentError, fn ->
      LanceDB.Source.batches([batch], schema: descriptor_schema)
    end

    assert {:error, %Error{kind: :invalid_data}} = Table.add(table, Source.batches([batch]))
    assert {:ok, 1} = Table.count_rows(table)
    assert {:ok, names} = Connection.table_names(conn)
    refute "bad_copy" in names
  end

  test "blob write options validate consistently on creation and append" do
    for opts <- [
          [external_blob_mode: :invalid],
          [allow_external_blob_outside_bases: nil],
          [external_blob_mode: :ingest, allow_external_blob_outside_bases: true],
          [blob_pack_file_size_threshold: 0],
          [blob_pack_file_size_threshold: -1]
        ] do
      assert_raise ArgumentError, fn -> Connection.create_table(:unused, "invalid", [], opts) end
      assert_raise ArgumentError, fn -> Table.add(:unused, [], opts) end
    end
  end

  test("invalid blob writes fail without committing", %{conn: conn, schema: schema}) do
    {:ok, table} = Connection.create_empty_table(conn, "invalid", schema)

    for value <- [
          123,
          %{},
          %{data: nil, uri: nil},
          %{data: "bytes", uri: "file:///x"},
          %{data: nil, uri: ""},
          %{data: nil, uri: <<255>>},
          %{data: 123, uri: nil}
        ] do
      assert {:error, %Error{kind: :invalid_data}} = Table.add(table, [%{id: 1, payload: value}])
      assert {:ok, 0} = Table.count_rows(table)
    end

    required = Schema.new([%{name: :payload, type: :blob, nullable: false}])
    assert {:error, %Error{kind: :invalid_data}} = Batch.from_rows([%{payload: nil}], required)
    assert_raise ArgumentError, fn -> Blob.reference("") end
  end

  test "invalid read requests fail before native calls" do
    blob = %Blob{resource: make_ref()}

    for value <- [-1, 1.5, nil, 18_446_744_073_709_551_616] do
      assert_raise ArgumentError, fn -> Blob.read(blob, value) end
      assert_raise ArgumentError, fn -> Blob.seek(blob, value) end
      assert_raise ArgumentError, fn -> LanceDB.Blob.fetch(:unused, :payload, [value]) end
    end

    for ranges <- [:bad, [1], [{-1, 2}], [{18_446_744_073_709_551_615, 1}]] do
      assert_raise ArgumentError, fn -> Blob.read_ranges(blob, ranges) end
    end

    for requests <- [:bad, [1], [{0, 0, -1}], [{-1, 0, 1}]] do
      assert_raise ArgumentError, fn -> LanceDB.Blob.fetch_ranges(:unused, :payload, requests) end
    end

    assert_raise ArgumentError, fn -> LanceDB.Blob.open(:unused, :payload, :bad) end
    assert_raise ArgumentError, fn -> Blob.stream(blob, chunk_size: 0) end
  end

  property("binary payloads survive ingestion, compaction and arbitrary range chunking", %{
    schema: schema
  }) do
    check(all(bytes <- binary(max_length: 1024), chunk_size <- integer(1..100), max_runs: 12)) do
      with_database(fn conn ->
        {:ok, table} =
          Connection.create_table(conn, "property", [%{id: 1, payload: bytes}],
            schema: schema,
            mode: :overwrite
          )

        {:ok, _} = Table.add(table, [%{id: 2, payload: nil}])
        {:ok, [%{"_rowid" => id}]} = Query.query(table, filter: "id = 1", with_row_id: true)
        assert {:ok, _} = LanceDB.Maintenance.compact_files(table, target_rows_per_fragment: 100)
        assert {:ok, [^bytes]} = LanceDB.Blob.fetch(table, :payload, [id])
        assert {:ok, [blob]} = LanceDB.Blob.open(table, :payload, [id])
        chunks = Enum.to_list(Blob.stream(blob, chunk_size: chunk_size))
        assert IO.iodata_to_binary(chunks) == bytes
        assert Enum.all?(chunks, &(byte_size(&1) > 0 and byte_size(&1) <= chunk_size))
        assert {:ok, 0} = Blob.tell(blob)
        assert {:ok, :ok} = Blob.close(blob)
      end)
    end
  end
end
