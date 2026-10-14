defmodule LanceDB.S3Test do
  use ExUnit.Case, async: true
  @moduletag :s3
  @moduletag timeout: 120_000
  setup do
    base_uri = System.fetch_env!("LANCEDB_TEST_S3_URI")
    suffix = "#{System.system_time(:nanosecond)}_#{System.unique_integer([:positive])}"
    uri = String.trim_trailing(base_uri, "/") <> "/case_#{suffix}"

    options = %{
      "aws_region" => "us-east-1",
      "aws_endpoint" => System.fetch_env!("LANCEDB_TEST_S3_ENDPOINT"),
      "aws_access_key_id" => System.fetch_env!("LANCEDB_TEST_S3_ACCESS_KEY"),
      "aws_secret_access_key" => System.fetch_env!("LANCEDB_TEST_S3_SECRET_KEY"),
      "allow_http" => "true",
      "aws_virtual_hosted_style_request" => "false"
    }

    assert {:ok, conn} = LanceDB.connect(uri, options)

    on_exit(fn ->
      assert {:ok, names} = LanceDB.Connection.table_names(conn)

      for name <- names do
        assert {:ok, :dropped} = LanceDB.Connection.drop_table(conn, name)
      end
    end)

    %{conn: conn, uri: uri, options: options}
  end

  test "local connections inherit storage configuration for remote creation", %{
    conn: remote,
    uri: uri,
    options: options
  } do
    alias LanceDB.{Batch, Connection, Source, Table}

    path =
      Path.join(LanceDB.TestSupport.tmp_dir(), "s3_local_#{System.unique_integer([:positive])}")

    on_exit(fn -> File.rm_rf!(path) end)
    {:ok, local} = Connection.connect(path, options)
    schema = LanceDB.Schema.new(id: :int64)
    {:ok, batch} = Batch.from_rows([%{id: 1}], schema)

    for {input, name} <- [
          {[%{id: 1}], "inferred"},
          {Source.rows([%{id: 1}], schema: schema), "rows"},
          {Source.batches([batch], schema: schema), "batches"}
        ] do
      location = uri <> "/#{name}.lance"
      assert {:ok, created} = Connection.create_table(local, name, input, location: location)
      assert {:ok, 1} = Table.count_rows(created)
      assert {:ok, reopened} = Connection.open_table(local, name, location: location)
      assert {:ok, 1} = Table.count_rows(reopened)
      assert {:ok, opened_remote} = Connection.open_table(remote, name)
      assert {:ok, 1} = Table.count_rows(opened_remote)
    end

    assert {:ok, empty} =
             Connection.create_empty_table(local, "empty", schema,
               location: uri <> "/empty.lance",
               storage_options: %{}
             )

    assert {:ok, 0} = Table.count_rows(empty)

    bad_options = Map.put(options, "aws_endpoint", "http://127.0.0.1:1")
    {:ok, bad_local} = Connection.connect(path, bad_options)

    assert {:ok, override} =
             Connection.create_empty_table(bad_local, "override", schema,
               location: uri <> "/override.lance",
               storage_options: %{"aws_endpoint" => options["aws_endpoint"]}
             )

    assert {:ok, 0} = Table.count_rows(override)
  end

  test "supervised cache retains shared FTS warming on object storage", %{
    conn: conn,
    uri: uri,
    options: options
  } do
    alias LanceDB.{Connection, Index, Query, Table, TableCache}
    alias LanceDB.TableCache.Warming

    rows = for id <- 1..100, do: %{id: id, text: "document #{id}", title: "title #{id}"}
    {:ok, source} = Connection.create_table(conn, "documents", rows)
    {:ok, :created} = Index.create(source, :text, type: :fts, name: "words")
    {:ok, :created} = Index.create(source, :title, type: :fts, name: "titles")

    cache =
      start_supervised!(
        {TableCache,
         uri: uri,
         storage_options: options,
         index_cache_size_bytes: 16 * 1024 * 1024,
         read_consistency_interval_ms: 0}
      )

    {:ok, :scheduled} = TableCache.prewarm(cache, "documents", indexes: [:words, :titles])

    LanceDB.TestSupport.assert_eventually(
      fn ->
        match?(
          {:ok,
           %{
             "words" => %Warming{status: :completed},
             "titles" => %Warming{status: :completed}
           }},
          TableCache.status(cache, "documents")
        )
      end,
      10_000
    )

    {:ok, table} = TableCache.fetch(cache, "documents")
    {:ok, before_query} = Table.cache_stats(table)
    assert before_query["index"]["num_entries"] > 0

    assert {:ok, [%{"id" => 42}]} =
             Query.full_text_search(table, "document",
               text_column: :text,
               filter: "id = 42",
               columns: [:id]
             )

    {:ok, after_query} = Table.cache_stats(table)
    assert after_query["index"]["hits"] > before_query["index"]["hits"]
    {:ok, _} = Table.add(source, [%{id: 101, text: "document 101", title: "title 101"}])
    assert {:ok, 101} = Table.count_rows(table)
    :ok = TableCache.invalidate(cache, "documents")
    assert {:ok, fresh} = TableCache.fetch(cache, "documents")
    assert fresh != table
    assert {:ok, 101} = Table.count_rows(fresh)
  end

  test("blob ranges, creation overrides and advanced FTS persist on object storage", %{
    conn: conn,
    uri: uri,
    options: options
  }) do
    alias LanceDB.{Blob, Connection, FTS, Index, Query, Schema, Table}
    schema = Schema.new(id: :int64, payload: :blob, text: :string)
    bytes = :binary.copy(<<0, 255, 128>>, 50_000)

    rows =
      for id <- 1..3 do
        %{id: id, payload: bytes, text: JSON.encode!(%{title: "elixir"})}
      end

    assert {:ok, table} =
             LanceDB.Connection.create_table(
               conn,
               "blobs",
               LanceDB.Source.rows(rows, schema: schema, batch_size: 1),
               max_rows_per_file: 1,
               enable_stable_row_ids: true,
               enable_v2_manifest_paths: false,
               storage_options: options
             )

    assert {:ok, false} = LanceDB.Maintenance.uses_v2_manifest_paths(table)

    assert {:ok, :created} =
             Index.create(table, :text,
               type: :fts,
               analyzer: [document_tokenizer: :json],
               block_size: 256,
               format_version: :v3,
               num_workers: 1
             )

    assert {:ok, [_]} =
             Query.full_text_search(table, FTS.json_match("title", "elixir"),
               limit: 1,
               wand_factor: 1.5
             )

    {:ok, [%{"_rowid" => id}]} = Query.query(table, with_row_id: true, filter: "id = 1")

    assert {:ok, [<<255, 128, 0, 255>>]} =
             LanceDB.Blob.fetch_ranges(table, :payload, [{id, 1, 4}])

    assert {:ok, _} =
             LanceDB.Maintenance.compact_files(table,
               target_rows_per_fragment: 100,
               num_threads: 1
             )

    assert {:ok, :migrated} = LanceDB.Maintenance.migrate_manifest_paths_v2(table)
    assert {:ok, true} = LanceDB.Maintenance.uses_v2_manifest_paths(table)
    {:ok, other} = Connection.connect(uri, options)
    {:ok, reopened} = Connection.open_table(other, "blobs")
    assert {:ok, [blob]} = LanceDB.Blob.open(reopened, :payload, [id])
    assert Blob.size(blob) == byte_size(bytes)
    assert IO.iodata_to_binary(Enum.to_list(Blob.stream(blob, chunk_size: 16_384))) == bytes
    assert {:ok, :ok} = Blob.close(blob)
  end

  test("streamed creation, native jobs, merges and backfills work over S3", %{conn: conn}) do
    alias LanceDB.{Batch, Connection, Index, Job, Query, Schema, Table}
    schema = Schema.new(id: :int64, vector: {:vector, 2, :uint8})
    rows = Enum.map(1..8, &%{id: &1, vector: [&1, 0]})

    assert {:ok, table} =
             LanceDB.Connection.create_table(
               conn,
               "bulk",
               LanceDB.Source.rows(rows, schema: schema, batch_size: 2),
               []
             )

    assert {:ok, job} =
             Index.create_async(table, :vector,
               type: :ivf_flat,
               distance: :hamming,
               num_partitions: 1
             )

    assert {:ok, :created} = Job.wait(job, timeout_ms: 30_000)

    assert {:ok, [%{"id" => 4}]} =
             LanceDB.Query.search(table, [4, 0], distance: :hamming, limit: 1)

    assert {:ok, %{inserted: 1, updated: 1}} =
             LanceDB.Table.upsert(
               table,
               LanceDB.Source.rows([%{id: 4, vector: [40, 0]}, %{id: 9, vector: [9, 0]}],
                 batch_size: 1
               ),
               []
             )

    assert {:ok, _} =
             LanceDB.SchemaEvolution.backfill_columns(
               table,
               [doubled: :int64],
               fn batch ->
                 {:ok, rows} = Batch.to_rows(batch)
                 Enum.map(rows, &%{doubled: &1["id"] * 2})
               end,
               read_columns: [:id],
               batch_size: 2
             )

    {:ok, schema} = Table.schema(table)
    batches = table |> Query.new(limit: nil) |> Query.stream_batches(batch_size: 2)

    assert {:ok, copied} =
             LanceDB.Connection.create_table(
               conn,
               "bulk_copy",
               LanceDB.Source.batches(batches, schema: schema),
               []
             )

    assert {:ok, rows} = LanceDB.Query.query(copied)
    assert length(rows) == 9
    assert Enum.all?(rows, &(&1["doubled"] == &1["id"] * 2))
  end

  test("single-commit ingestion, rich results and native metrics work over S3", %{
    uri: uri,
    options: options
  }) do
    assert :ok = LanceDB.Telemetry.enable_native_metrics()
    assert {:ok, conn} = LanceDB.connect(uri, options)
    assert {:ok, table} = LanceDB.Connection.create_empty_table(conn, "ingestion", id: :int64)
    {:ok, before} = LanceDB.History.version(table)
    records = Stream.map(1..13, &%{id: &1})

    assert {:ok, %{inserted: 13, version: version}} =
             LanceDB.Table.add(
               table,
               LanceDB.Source.rows(records, batch_size: 2),
               commit: :once,
               write_parallelism: 2,
               max_rows_per_file: 4,
               progress: {self(), :s3_import}
             )

    assert version == before + 1
    assert_receive {:lancedb_write_progress, :s3_import, %{done: true, output_rows: 13}}, 1000
    assert {:ok, 13} = LanceDB.Table.count_rows(table)

    assert {:error, %LanceDB.Error{details: %{inserted: 0}}} =
             LanceDB.Table.add(
               table,
               LanceDB.Source.rows([%{id: 14}, %{id: "invalid"}], batch_size: 1),
               commit: :once
             )

    assert {:ok, ^version} = LanceDB.History.version(table)
    assert {:ok, %{deleted: 3, version: deleted_version}} = LanceDB.Table.delete(table, "id <= 3")
    assert deleted_version == version + 1
    assert {:ok, reopened} = LanceDB.Connection.open_table(conn, "ingestion")
    assert {:ok, 10} = LanceDB.Table.count_rows(reopened)

    assert Enum.any?(LanceDB.Telemetry.snapshot(), fn metric ->
             metric["kind"] == "counter" and metric["measurements"]["value"] > 0
           end)
  end

  test("custom hybrid ranking and null ordering work over S3", %{conn: conn}) do
    schema = [id: :int64, priority: :int32, vector: {:vector, 2}, text: :string]

    {:ok, table} =
      LanceDB.Connection.create_table(
        conn,
        "ranking",
        [
          %{id: 1, priority: nil, vector: [1, 0], text: "elixir"},
          %{id: 2, priority: 2, vector: [0, 1], text: "elixir code"}
        ],
        schema: schema
      )

    assert {:ok, :created} = LanceDB.Index.create(table, :text, type: :fts)

    callback = fn vector, _text ->
      Enum.map(vector, &Map.put(&1, "_relevance_score", &1["id"]))
    end

    opts = [
      reranker: callback,
      normalize: :rank,
      order_by: [desc_nulls_first: :priority],
      columns: [:id]
    ]

    assert {:ok, rows} = LanceDB.Query.hybrid_search(table, [1, 0], "elixir", opts)
    assert Enum.map(rows, & &1["id"]) == [1, 2]

    assert rows ==
             LanceDB.TestSupport.rows(
               table,
               opts ++ [vector: [1, 0], text: "elixir", batch_size: 1]
             )
             |> Enum.to_list()

    assert {:ok, [%{"id" => 1}, %{"id" => 2}]} =
             LanceDB.Query.full_text_search(table, "elixir",
               order_by: [asc_nulls_first: :priority],
               columns: [:id]
             )
  end

  test("primitive schemas and nested evolution persist over S3", %{
    conn: conn,
    uri: uri,
    options: options
  }) do
    bytes = <<0, 255, 128>>

    schema = [
      id: :uint64,
      day: :date32,
      payload: :binary,
      data: {:struct, count: :int16, obsolete: :string}
    ]

    {:ok, table} =
      LanceDB.Connection.create_table(
        conn,
        "primitives",
        [
          %{
            id: 18_446_744_073_709_551_615,
            day: ~D[2024-02-29],
            payload: bytes,
            data: %{count: 12, obsolete: nil}
          }
        ],
        schema: schema
      )

    assert {:ok, %{version: _}} =
             LanceDB.SchemaEvolution.alter_columns(table, [
               {"day", [type: :date64]},
               {"data.count", [rename: :total]}
             ])

    assert {:ok, %{version: _}} = LanceDB.SchemaEvolution.drop_columns(table, ["data.obsolete"])
    assert {:ok, other} = LanceDB.connect(uri, options)
    assert {:ok, reopened} = LanceDB.Connection.open_table(other, "primitives")

    assert {:ok,
            [
              %{
                "id" => 18_446_744_073_709_551_615,
                "day" => "2024-02-29",
                "payload" => ^bytes,
                "data" => %{"total" => 12}
              }
            ]} = LanceDB.Query.query(reopened)

    assert {:ok, %{updated: 1}} =
             LanceDB.Table.update(reopened, %{payload: <<254>>, day: ~D[1969-12-31]})

    assert [%{"payload" => <<254>>, "day" => "1969-12-31"}] =
             LanceDB.TestSupport.rows(reopened, batch_size: 1) |> Enum.to_list()
  end

  test("vector index variants and training options persist over S3", %{
    conn: conn,
    uri: uri,
    options: options
  }) do
    vector = fn id ->
      for d <- 1..16 do
        :math.sin(id * d / 20)
      end
    end

    {:ok, table} =
      LanceDB.Connection.create_table(
        conn,
        "vector_training",
        for id <- 1..64 do
          %{id: id, vector: vector.(id)}
        end
      )

    for {type, native_type} <- [ivf_sq: "IVF_SQ", ivf_rq: "IVF_RQ", hnsw_flat: "IVF_HNSW_FLAT"] do
      training = [
        type: type,
        name: "trained",
        replace: true,
        target_partition_size: 64,
        sample_rate: 16,
        max_iterations: 2
      ]

      training =
        if type == :ivf_rq do
          training ++ [num_bits: 4]
        else
          training
        end

      assert {:ok, :created} = LanceDB.Index.create(table, :vector, training)
      {:ok, other} = LanceDB.connect(uri, options)
      {:ok, reopened} = LanceDB.Connection.open_table(other, "vector_training")

      assert {:ok, %{"index_type" => ^native_type, "num_indexed_rows" => 64}} =
               LanceDB.Index.stats(reopened, "trained")

      assert {:ok, [%{"id" => 64}]} =
               LanceDB.Query.search(reopened, vector.(64),
                 approx_mode: :accurate,
                 limit: 1,
                 refine_factor: 64,
                 ef: 64
               )
    end
  end

  test("LabelList/FM indexes and batch/multivector search persist on S3", %{
    conn: conn,
    uri: uri,
    options: options
  }) do
    {:ok, table} =
      LanceDB.Connection.create_table(
        conn,
        "search_modes",
        [
          %{
            id: 1,
            text: "Elixir",
            tags: ["elixir", "rust"],
            vector: [1, 0],
            embeddings: [[1, 0], [0, 1]]
          },
          %{id: 2, text: "Rust", tags: ["rust"], vector: [0, 1], embeddings: [[0, 1]]}
        ],
        schema: [
          id: :int64,
          text: :string,
          tags: {:list, :string},
          vector: {:vector, 2},
          embeddings: {:list, {:vector, 2}}
        ]
      )

    assert {:ok, :created} = LanceDB.Index.create(table, :tags, type: :label_list)
    assert {:ok, :created} = LanceDB.Index.create(table, :text, type: :fm)
    {:ok, other} = LanceDB.connect(uri, options)
    {:ok, table} = LanceDB.Connection.open_table(other, "search_modes")

    assert {:ok, [%{"id" => 1}]} =
             LanceDB.Query.query(table, filter: "array_has_all(tags, ['elixir', 'rust'])")

    assert {:ok, [%{"id" => 1}]} = LanceDB.Query.query(table, filter: "contains(text, 'ixir')")

    rows =
      Enum.to_list(
        LanceDB.TestSupport.rows(table,
          vectors: [[1, 0], [0, 1]],
          limit: 1,
          columns: [:id],
          batch_size: 1
        )
      )

    assert Enum.sort(Enum.map(rows, &{&1["query_index"], &1["id"]})) == [{0, 1}, {1, 2}]

    assert {:ok, [%{"id" => 1}]} =
             LanceDB.Query.multivector_search(table, [[1, 0], [0, 1]], limit: 1)
  end

  test("nested schemas and literal updates persist across S3 connections", %{
    conn: conn,
    uri: uri,
    options: options
  }) do
    fields = [
      id: :int64,
      data: {:struct, tags: {:list, :string}, at: {:timestamp, :microsecond, "Europe/Vienna"}}
    ]

    {:ok, table} = LanceDB.Connection.create_empty_table(conn, "nested", fields)

    assert {:ok, %{inserted: 1}} =
             LanceDB.Table.add(table, [%{id: 1, data: %{tags: [], at: ~U[2024-01-01 00:00:00Z]}}])

    assert {:ok, %{updated: 1}} =
             LanceDB.Table.update(table, %{data: %{tags: ["elixir", nil], at: -1}})

    {:ok, other} = LanceDB.connect(uri, options)
    {:ok, reopened} = LanceDB.Connection.open_table(other, "nested")

    assert [
             %{
               "id" => 1,
               "data" => %{"tags" => ["elixir", nil], "at" => "1969-12-31T23:59:59.999999Z"}
             }
           ] = Enum.to_list(LanceDB.TestSupport.rows(reopened, batch_size: 1))
  end

  test("query diagnostics and table statistics work over S3", %{conn: conn}) do
    {:ok, table} =
      LanceDB.Connection.create_empty_table(conn, "diagnostics",
        id: :int64,
        text: :string,
        vector: {:vector, 2}
      )

    {:ok, %{inserted: 2}} =
      LanceDB.Table.add(table, [
        %{id: 1, text: "elixir", vector: [1, 0]},
        %{id: 2, text: "rust", vector: [0, 1]}
      ])

    {:ok, :created} = LanceDB.Index.create(table, :text, type: :fts)
    opts = [vector: [1, 0], text: "elixir", select: [item: "id"], limit: 1]

    assert {:ok, %LanceDB.ArrowSchema{fields: fields}} =
             LanceDB.TestSupport.output_schema(table, opts)

    assert Enum.map(fields, & &1.name) == ["item", "_relevance_score"]
    assert {:ok, analysis} = LanceDB.TestSupport.analyze_plan(table, opts)
    assert analysis =~ "metrics=["
    assert String.ends_with?(analysis, "Output: rows=1\n")
    assert {:ok, stats} = LanceDB.Maintenance.stats(table)
    assert stats["num_rows"] == 2
    assert stats["num_indices"] == 1
    assert stats["total_bytes"] > 0
    assert stats["fragment_stats"]["num_fragments"] == 1

    assert {:ok, _} =
             LanceDB.TestSupport.output_schema(table, select: [bad: "CAST(text AS BIGINT)"])

    assert {:error, _} =
             LanceDB.TestSupport.analyze_plan(table, select: [bad: "CAST(text AS BIGINT)"])
  end

  test("computed projections and direct retrieval work over S3", %{conn: conn}) do
    {:ok, table} =
      LanceDB.Connection.create_empty_table(conn, "retrieval", id: :int64, price: :float64)

    {:ok, %{inserted: 3}} =
      LanceDB.Table.add(table, [%{id: 1, price: 1.5}, %{id: 2, price: 3.0}, %{id: 3, price: 4.5}])

    opts = [select: [item: "id", total: "price * 2"], with_row_id: true]
    assert {:ok, rows} = LanceDB.Query.query(table, [order_by: [asc: :id]] ++ opts)
    assert Enum.map(rows, & &1["total"]) == [3.0, 6.0, 9.0]
    ids = Enum.map(rows, & &1["_rowid"])
    assert {:ok, fetched} = LanceDB.Query.take_row_ids(table, ids, opts)
    assert Enum.sort_by(fetched, & &1["item"]) == rows
    assert {:ok, [%{"item" => 2, "total" => 6.0}]} = LanceDB.Query.take_offsets(table, [1], opts)

    assert Enum.to_list(LanceDB.TestSupport.rows(table, [row_ids: ids, batch_size: 1] ++ opts)) ==
             fetched
  end

  test("streamed ingestion and query batches work over S3", %{conn: conn}) do
    schema = [id: :int64, at: {:timestamp, :nanosecond, "UTC"}]
    {:ok, source} = LanceDB.Connection.create_empty_table(conn, "source", schema)
    {:ok, target} = LanceDB.Connection.create_empty_table(conn, "target", schema)
    input = Stream.map(1..37, &%{id: &1, at: &1})

    assert {:ok, %{inserted: 37}} =
             LanceDB.Table.add(
               source,
               LanceDB.Source.rows(input, batch_size: 8),
               []
             )

    rows =
      LanceDB.TestSupport.rows(source, batch_size: 3, filter: "id > 10", order_by: [asc: :id])

    assert [%{"id" => 11}] = Enum.take(rows, 1)

    assert {:ok, %{inserted: 27}} =
             LanceDB.Table.add(
               target,
               LanceDB.Source.rows(rows, batch_size: 10),
               []
             )

    assert Enum.map(
             LanceDB.TestSupport.rows(target, order_by: [asc: :id]),
             & &1["id"]
           ) == Enum.to_list(11..37)
  end

  test("timestamp schemas and precision persist across S3 connections", context) do
    %{conn: conn, uri: uri, options: options} = context
    schema = [id: :int64, at: {:timestamp, :nanosecond, "Europe/Vienna"}]
    assert {:ok, table} = LanceDB.Connection.create_empty_table(conn, "events", schema)

    assert {:ok, %{inserted: 1}} =
             LanceDB.Table.add(table, [%{id: 1, at: "2024-01-01T01:00:00.123456789+01:00"}])

    assert {:ok, reopened_conn} = LanceDB.connect(uri, options)
    assert {:ok, reopened} = LanceDB.Connection.open_table(reopened_conn, "events")
    assert {:ok, [%{"at" => "2024-01-01T00:00:00.123456789Z"}]} = LanceDB.Query.query(reopened)
    assert {:ok, %{updated: 1}} = LanceDB.Table.update(reopened, %{at: -1})
    assert {:ok, [%{"at" => "1969-12-31T23:59:59.999999999Z"}]} = LanceDB.Query.query(reopened)
    assert {:ok, %LanceDB.Schema{fields: fields}} = LanceDB.Table.schema(reopened)

    assert Enum.find(fields, &(&1.name == "at")).type ==
             {:timestamp, :nanosecond, "Europe/Vienna"}
  end

  test("writes and indexes persist across independent S3 connections", context) do
    %{conn: conn, uri: uri, options: options} = context
    schema = [id: :int64, text: :string, vector: {:vector, 2}]
    assert {:ok, table} = LanceDB.Connection.create_empty_table(conn, "docs", schema)
    assert {:ok, 0} = LanceDB.Table.count_rows(table)

    rows =
      for id <- 1..32 do
        %{id: id, text: "document", vector: [id / 32, 1.0]}
      end

    assert {:ok, %{inserted: 32}} = LanceDB.Table.add(table, rows)

    assert {:ok, %{inserted: 1, updated: 1}} =
             LanceDB.Table.upsert(table, [
               %{id: 1, text: "replaced", vector: [0.0, 1.0]},
               %{id: 33, text: "added", vector: [2.0, 1.0]}
             ])

    assert {:ok, %{updated: 1, version: version}} =
             LanceDB.Table.update(table, %{text: "needle"}, filter: "id = 32")

    assert {:ok, ^version} = LanceDB.History.version(table)

    assert {:ok, :created} =
             LanceDB.Index.create(table, :vector,
               type: :ivf_flat,
               num_partitions: 1,
               name: "vectors"
             )

    assert {:ok, :created} = LanceDB.Index.create(table, :text, type: :fts, name: "words")
    assert {:ok, reopened_conn} = LanceDB.connect(uri, options, read_consistency_interval_ms: 0)
    assert {:ok, ["docs"]} = LanceDB.Connection.table_names(reopened_conn)
    assert {:ok, reopened} = LanceDB.Connection.open_table(reopened_conn, "docs")
    assert {:ok, 33} = LanceDB.Table.count_rows(reopened)

    assert {:ok, [%{"id" => 1, "text" => "replaced"}]} =
             LanceDB.Query.query(reopened, filter: "id = 1", timeout_ms: 10_000)

    assert {:ok, :prewarmed} = LanceDB.Index.prewarm(reopened, "vectors")

    assert {:ok, [%{"id" => 32, "_rowid" => row_id}]} =
             LanceDB.Query.search(reopened, [1.0, 1.0],
               limit: 1,
               timeout_ms: 10_000,
               with_row_id: true,
               distance_range: {0, 0.01},
               minimum_nprobes: 1,
               maximum_nprobes: nil,
               fast_search: true
             )

    assert is_integer(row_id)

    assert {:ok, [%{"_rowid" => ^row_id}]} =
             LanceDB.Query.query(reopened,
               filter: "id = 32",
               columns: [:_rowid],
               with_row_id: true
             )

    assert {:ok, [%{"id" => 32}]} =
             LanceDB.Query.full_text_search(reopened, "needle", timeout_ms: 10_000)

    structured = LanceDB.FTS.boolean(must: ["needle"], must_not: ["document"])

    assert {:ok, [%{"id" => 32}]} =
             LanceDB.Query.full_text_search(reopened, structured, timeout_ms: 10_000)

    assert {:ok, tokens} = LanceDB.Tokenizer.tokenize("Needle")
    assert {:ok, ^tokens} = LanceDB.Tokenizer.tokenize(reopened, "Needle", index: "words")

    assert {:ok, [%{"id" => 32, "_relevance_score" => score}]} =
             LanceDB.Query.hybrid_search(
               reopened,
               [1.0, 1.0],
               LanceDB.FTS.match("nedl", fuzziness: 1),
               limit: 1,
               timeout_ms: 10_000
             )

    assert_in_delta score, 2 / 60, 1.0e-6
    assert {:ok, %{"num_indexed_rows" => 33}} = LanceDB.Index.stats(reopened, "vectors")

    assert {:ok, plan} =
             LanceDB.TestSupport.explain_plan(reopened,
               vector: [1.0, 1.0],
               limit: 1,
               timeout_ms: 10_000
             )

    assert plan =~ ~r/ANN|Ivf/i

    assert {:ok, %{updated: 1}} =
             LanceDB.Table.update(table, %{text: "refreshed"}, filter: "id = 33")

    assert {:ok, [%{"id" => 33, "text" => "refreshed"}]} =
             LanceDB.Query.query(reopened, filter: "id = 33", timeout_ms: 10_000)
  end

  test("S3 snapshots, tags, restore, deletion and cleanup", %{
    conn: conn,
    uri: uri,
    options: options
  }) do
    assert {:ok, table} =
             LanceDB.Connection.create_table(conn, "docs", [%{id: 1, text: "original"}])

    assert {:ok, original} = LanceDB.History.version(table)
    assert {:ok, :created} = LanceDB.History.create_tag(table, "saved", original)
    assert {:ok, %{inserted: 1}} = LanceDB.Table.add(table, [%{id: 2, text: "later"}])

    assert {:ok, %{updated: 1}} =
             LanceDB.Table.update(table, %{text: "changed"}, filter: "id = 1")

    assert {:ok, other_conn} = LanceDB.connect(uri, options)
    assert {:ok, other} = LanceDB.Connection.open_table(other_conn, "docs")
    assert {:ok, ^original} = LanceDB.History.tag_version(other, "saved")
    assert {:ok, other} = LanceDB.History.snapshot_tag(other, "saved")
    assert {:ok, [%{"text" => "original"}]} = LanceDB.Query.query(other)
    {:ok, other} = LanceDB.History.latest(other)
    assert {:ok, %LanceDB.WriteResult{}} = LanceDB.History.restore(other, original)
    assert {:ok, :deleted} = LanceDB.History.delete_tag(other, "saved")
    assert {:ok, _} = LanceDB.Maintenance.cleanup_old_versions(other, older_than_seconds: 0)
    assert {:ok, history} = LanceDB.History.list_versions(other)
    refute Enum.any?(history, &(&1.version == original))
    assert {:ok, %{operation: :delete}} = LanceDB.Table.delete(other, "id = 1")
    assert {:ok, 0} = LanceDB.Table.count_rows(other)
    assert {:ok, :dropped} = LanceDB.Connection.drop_table(other_conn, "docs")
    assert {:ok, []} = LanceDB.Connection.table_names(other_conn)
    assert {:error, _} = LanceDB.Connection.open_table(other_conn, "docs")
  end

  test("invalid S3 credentials produce an error", %{uri: uri, options: options}) do
    bad_options = Map.put(options, "aws_secret_access_key", "incorrect-test-secret")

    result =
      with {:ok, conn} <- LanceDB.connect(uri, bad_options) do
        LanceDB.Connection.table_names(conn)
      end

    assert {:error, reason} = result
    assert is_struct(reason, LanceDB.Error)
  end

  test("branches, clones, metadata and open overrides persist on S3", %{
    conn: conn,
    uri: uri,
    options: options
  }) do
    {:ok, table} =
      LanceDB.Connection.create_table(conn, "source", [%{id: 1}],
        schema: [%{name: :id, type: :int64, metadata: %{unit: "items"}}],
        schema_metadata: %{owner: "main"}
      )

    {:ok, source} = LanceDB.Table.uri(table)
    {:ok, branch} = LanceDB.Branch.create(table, "work")
    {:ok, %{version: _}} = LanceDB.Metadata.update_schema(branch, %{owner: "branch"})
    {:ok, %{version: _}} = LanceDB.Metadata.update_field(branch, :id, %{unit: "branch items"})
    {:ok, version} = LanceDB.History.version(branch)
    {:ok, :created} = LanceDB.History.create_tag(branch, "branch_snapshot", version)

    assert {:ok, copy} =
             LanceDB.Connection.clone_table(conn, "copy", source, source_tag: "branch_snapshot")

    assert {:ok, %{"owner" => "branch"}} = LanceDB.Metadata.schema(copy)
    assert {:ok, %{"owner" => "main"}} = LanceDB.Metadata.schema(table)
    assert {:ok, %{inserted: 1}} = LanceDB.Table.add(branch, [%{id: 2}])
    assert {:ok, 1} = LanceDB.Table.count_rows(copy)
    {:ok, fresh} = LanceDB.connect(uri, options)
    {:ok, persisted} = LanceDB.Connection.open_table(fresh, "copy")
    assert {:ok, %{"owner" => "branch"}} = LanceDB.Metadata.schema(persisted)
    assert {:error, _} = LanceDB.Connection.clone_table(conn, "copy", source)
    assert {:ok, 1} = LanceDB.Table.count_rows(persisted)

    local =
      Path.join(
        LanceDB.TestSupport.tmp_dir(),
        "lancedb_s3_open_#{System.unique_integer([:positive])}"
      )

    on_exit(fn -> File.rm_rf!(local) end)
    {:ok, local_conn} = LanceDB.connect(local, options)

    assert {:ok, local_copy} =
             LanceDB.Connection.clone_table(local_conn, "local_copy", source,
               source_tag: "branch_snapshot"
             )

    assert {:ok, 1} = LanceDB.Table.count_rows(local_copy)
    assert {:ok, ["local_copy"]} = LanceDB.Connection.table_names(local_conn)

    if secondary_base = System.get_env("LANCEDB_TEST_S3_SECONDARY_URI") do
      secondary_uri =
        String.trim_trailing(secondary_base, "/") <>
          "/case_#{System.system_time(:nanosecond)}_#{System.unique_integer([:positive])}"

      assert {:ok, secondary} = LanceDB.connect(secondary_uri, options)

      on_exit(fn ->
        assert {:ok, names} = LanceDB.Connection.table_names(secondary)

        for name <- names do
          assert {:ok, :dropped} = LanceDB.Connection.drop_table(secondary, name)
        end
      end)

      assert {:ok, bucket_copy} =
               LanceDB.Connection.clone_table(secondary, "bucket_copy", source,
                 source_tag: "branch_snapshot"
               )

      assert {:ok, ["bucket_copy"]} = LanceDB.Connection.table_names(secondary)
      assert {:ok, ["copy", "source"]} = LanceDB.Connection.table_names(conn)
      assert {:ok, %{"owner" => "branch"}} = LanceDB.Metadata.schema(bucket_copy)
      assert {:ok, 1} = LanceDB.Table.count_rows(bucket_copy)
      assert {:ok, %{inserted: 1}} = LanceDB.Table.add(bucket_copy, [%{id: 3}])
      assert {:ok, 2} = LanceDB.Table.count_rows(bucket_copy)
      assert {:ok, 1} = LanceDB.Table.count_rows(copy)
    end

    {:ok, other} = LanceDB.connect(local, Map.put(options, "aws_access_key_id", "override-me"))

    assert {:ok, reopened} =
             LanceDB.Connection.open_table(other, "source",
               location: source,
               branch: "work",
               version: version,
               index_cache_size_bytes: 0,
               metadata_cache_size_bytes: 0,
               storage_options: %{aws_access_key_id: options["aws_access_key_id"]}
             )

    assert {:ok, 1} = LanceDB.Table.count_rows(reopened)

    assert {:ok, %LanceDB.Schema{fields: [%LanceDB.Field{metadata: %{"unit" => "branch items"}}]}} =
             LanceDB.Table.schema(reopened)

    assert {:error, _} = LanceDB.Metadata.update_schema(reopened, %{blocked: "yes"})
    assert {:ok, reopened} = LanceDB.History.latest(reopened)
    assert {:ok, 2} = LanceDB.Table.count_rows(reopened)
    assert {:ok, ["copy"]} = LanceDB.Connection.table_names(conn, limit: 1)
    assert {:ok, ["source"]} = LanceDB.Connection.table_names(conn, start_after: "copy")
  end

  test "initial cache sessions preserve S3 storage overrides and consistency", %{
    conn: writer,
    uri: uri,
    options: options
  } do
    alias LanceDB.{Connection, Table}
    {:ok, table} = Connection.create_table(writer, "docs", [%{id: 1}])
    {:ok, location} = Table.uri(table)
    bad_options = Map.put(options, "aws_endpoint", "http://127.0.0.1:1")

    local =
      Path.join(
        LanceDB.TestSupport.tmp_dir(),
        "s3_initial_cache_#{System.unique_integer([:positive])}"
      )

    on_exit(fn -> File.rm_rf!(local) end)

    budgets = [
      index_cache_size_bytes: 0,
      metadata_cache_size_bytes: 0,
      read_consistency_interval_ms: 0
    ]

    readers =
      for {uri, storage} <- [{uri, options}, {local, bad_options}] do
        {:ok, conn} = LanceDB.connect(uri, storage, budgets)

        assert {:ok, reader} =
                 Connection.open_table(conn, "alias",
                   location: location,
                   storage_options: %{aws_endpoint: options["aws_endpoint"]}
                 )

        assert {:ok, [%{"id" => 1}]} = LanceDB.Query.query(reader)
        reader
      end

    assert {:ok, %{inserted: 1}} = Table.add(table, [%{id: 2}])

    for reader <- readers do
      assert {:ok, [%{"id" => 1}, %{"id" => 2}]} = LanceDB.Query.query(reader)
      assert {:ok, stats} = Table.cache_stats(reader)

      for cache <- ["index", "metadata"] do
        assert stats[cache]["size_bytes"] == 0
        assert stats[cache]["num_entries"] == 0
      end
    end
  end
end
