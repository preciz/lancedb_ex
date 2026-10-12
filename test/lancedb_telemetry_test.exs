defmodule LanceDB.TelemetryTest do
  use LanceDB.DataCase, async: false
  alias LanceDB.Telemetry

  def handle_event(event, measurements, metadata, pid) do
    send(pid, {event, measurements, metadata})
  end

  defp attach(events) do
    id = make_ref()
    :ok = :telemetry.attach_many(id, events, &__MODULE__.handle_event/4, self())
    on_exit(fn -> :telemetry.detach(id) end)
  end

  test("operation spans cover successes, returned errors and raised errors", %{path: path}) do
    attach(
      for event <- [:start, :stop, :exception] do
        [:lancedb, :operation, event]
      end
    )

    {:ok, conn} = LanceDB.connect(path)
    assert_receive {[:lancedb, :operation, :start], %{system_time: _}, %{operation: :connect}}

    assert_receive {[:lancedb, :operation, :stop], %{duration: duration},
                    %{operation: :connect, status: :ok}}

    assert duration >= 0
    {:ok, table} = LanceDB.Connection.create_empty_table(conn, "rows", id: :int64)
    assert {:ok, %{inserted: 1}} = LanceDB.Table.add(table, [%{id: 1}])
    assert_receive {[:lancedb, :operation, :stop], _, %{operation: :add, status: :ok}}
    assert {:error, _} = LanceDB.Table.add(table, [%{id: "invalid"}])
    assert_receive {[:lancedb, :operation, :stop], _, %{operation: :add, status: :error}}
    assert_raise ArgumentError, fn -> LanceDB.Table.add(table, [%{id: 2}], mode: :bad) end

    assert_receive {[:lancedb, :operation, :exception], %{duration: _},
                    %{operation: :add, kind: :error}}

    assert {:ok, [_]} = LanceDB.Query.query(table)
    assert_receive {[:lancedb, :operation, :stop], _, %{operation: :query, status: :ok}}
    assert [%{"id" => 1}] = LanceDB.TestSupport.rows(table) |> Enum.to_list()
    assert_receive {[:lancedb, :operation, :stop], _, %{operation: :stream_close, status: :ok}}
  end

  test("native metrics are discoverable cumulative values and poll emits both metric shapes", %{
    path: path
  }) do
    assert :ok = Telemetry.enable_native_metrics()
    assert :ok = Telemetry.enable_native_metrics()
    attach([[:lancedb, :native, :metric]])
    {:ok, conn} = LanceDB.connect(path)
    {:ok, table} = LanceDB.Connection.create_empty_table(conn, "rows", id: :int64)

    assert {:ok, %{inserted: 3}} =
             LanceDB.Table.add(
               table,
               for id <- 1..3 do
                 %{id: id}
               end
             )

    assert {:ok, [_ | _]} = LanceDB.Query.query(table)
    catalog = Telemetry.catalog()
    assert Enum.any?(catalog, &(&1["kind"] == "counter"))
    snapshot = Telemetry.snapshot()
    counters = Enum.filter(snapshot, &(&1["kind"] == "counter"))
    assert Enum.any?(counters, &(&1["measurements"]["value"] > 0))
    assert Enum.any?(snapshot, &(&1["kind"] == "histogram" and &1["measurements"]["count"] > 0))
    assert :ok = Telemetry.poll()

    assert_receive {[:lancedb, :native, :metric], %{value: value},
                    %{name: name, kind: "counter", attributes: attributes}}

    assert is_number(value) and is_binary(name) and is_map(attributes)

    assert_receive {[:lancedb, :native, :metric], %{count: count, sum: sum},
                    %{kind: "histogram", buckets: buckets}}

    assert count >= 0 and is_number(sum) and is_list(buckets)
    later = Telemetry.snapshot()

    for point <- counters do
      next =
        Enum.find(
          later,
          &(&1["name"] == point["name"] and &1["attributes"] == point["attributes"])
        )

      assert next["measurements"]["value"] >= point["measurements"]["value"]
    end
  end

  test "native recorder installation failures return structured errors" do
    # The recorder cannot be reset or replaced through the public API. Stub only
    # the NIF boundary in a child VM, keeping the real backend and public wrapper.
    code = """
    Code.compiler_options(ignore_module_conflict: true)
    defmodule LanceDB.Native do
      def metrics_enable do
        {:error, %{
          kind: :invalid_data,
          message: "Another native metrics recorder is already installed",
          details: %{}
        }}
      end
    end

    {:error, %LanceDB.Error{
      kind: :invalid_data,
      operation: :enable_native_metrics,
      message: "Another native metrics recorder is already installed",
      details: %{}
    }} = LanceDB.Telemetry.enable_native_metrics()
    IO.puts("native recorder failure normalized")
    """

    {output, status} =
      System.cmd(
        System.find_executable("elixir"),
        ["--erl", "+S 2:2", "-pa", Path.join(Mix.Project.build_path(), "lib/*/ebin"), "-e", code],
        stderr_to_stdout: true
      )

    assert status == 0, output
    assert output =~ "native recorder failure normalized"
  end
end
