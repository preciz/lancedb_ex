defmodule LanceDB.ObjectStoreFixture do
  @moduledoc false
  use GenServer

  # A read-only S3 fixture backed by an in-memory copy of a local LanceDB table.
  # Holding responses uses messages, never file I/O or a BEAM dirty scheduler.
  def start_link(root) do
    objects =
      root
      |> Path.join("**/*")
      |> Path.wildcard(match_dot: true)
      |> Enum.filter(&File.regular?/1)
      |> Map.new(&{Path.relative_to(&1, root), File.read!(&1)})

    GenServer.start_link(__MODULE__, objects)
  end

  def port(server), do: GenServer.call(server, :port)

  def hold(server, requests \\ :objects) when requests in [:objects, :all, :indexes],
    do: GenServer.call(server, {:hold, self(), requests})

  def release(server), do: GenServer.call(server, :release)
  def requests(server), do: GenServer.call(server, :requests)

  @impl true
  def init(objects) do
    {:ok, socket} =
      :gen_tcp.listen(0, [:binary, packet: :http_bin, active: false, ip: {127, 0, 0, 1}])

    {:ok, {_, port}} = :inet.sockname(socket)
    server = self()
    acceptor = spawn_link(fn -> accept(socket, server) end)

    {:ok,
     %{
       objects: objects,
       port: port,
       acceptor: acceptor,
       owner: nil,
       requests: :objects,
       held: [],
       observations: []
     }}
  end

  @impl true
  def handle_call(:port, _from, state), do: {:reply, state.port, state}
  def handle_call(:requests, _from, state), do: {:reply, Enum.reverse(state.observations), state}

  def handle_call({:hold, owner, requests}, _from, state),
    do: {:reply, :ok, %{state | owner: owner, requests: requests}}

  def handle_call(:release, _from, state) do
    for {pid, reference} <- state.held, do: send(pid, reference)
    {:reply, :ok, %{state | owner: nil, held: []}}
  end

  def handle_call({:request, method, target, headers}, {pid, _}, state) do
    uri = URI.parse(target)
    query = URI.decode_query(uri.query || "")
    key = uri.path |> URI.decode() |> String.replace_prefix("/probe/", "")

    response = response(state.objects, key, query, headers)
    state = %{state | observations: [{method, key} | state.observations]}

    if not is_nil(state.owner) and hold_request?(state.requests, method, key, state.objects) do
      reference = make_ref()
      send(state.owner, {self(), :blocked, key})
      {:reply, {response, reference}, %{state | held: [{pid, reference} | state.held]}}
    else
      {:reply, {response, nil}, state}
    end
  end

  @impl true
  def terminate(_reason, state) do
    Process.unlink(state.acceptor)
    Process.exit(state.acceptor, :shutdown)
  end

  defp hold_request?(:all, _method, _key, _objects), do: true

  defp hold_request?(:indexes, method, key, _objects),
    do: method == :GET and String.contains?(key, "/_indices/")

  defp hold_request?(:objects, method, key, objects),
    do: method == :GET and Map.has_key?(objects, key)

  # Hold SDK region resolution locally without contacting AWS or using real credentials.
  defp response(_objects, "/latest/api/token", _query, _headers),
    do: {200, "x-aws-ec2-metadata-token-ttl-seconds: 21600\r\n", "fixture"}

  defp response(_objects, "/latest/meta-data/placement/region", _query, _headers),
    do: {200, [], "us-east-1"}

  defp response(objects, key, query, headers) do
    if Map.has_key?(query, "list-type") or Map.has_key?(query, "prefix") do
      {200, [], listing(objects, query)}
    else
      case Map.fetch(objects, key) do
        {:ok, data} -> object(data, headers)
        :error -> {404, [], ""}
      end
    end
  end

  defp accept(socket, server) do
    case :gen_tcp.accept(socket) do
      {:ok, client} ->
        pid =
          spawn_link(fn ->
            receive do
              :serve -> serve(client, server)
            end
          end)

        :ok = :gen_tcp.controlling_process(client, pid)
        send(pid, :serve)
        accept(socket, server)

      {:error, :closed} ->
        :ok
    end
  end

  defp serve(socket, server) do
    {:http_request, method, {:abs_path, target}, _} = read!(socket)
    headers = headers(socket, %{})

    {{status, response_headers, body}, reference} =
      GenServer.call(server, {:request, method, target, headers})

    if reference do
      receive do
        ^reference -> :ok
      after
        10_000 -> raise "S3 fixture response was not released"
      end
    end

    :gen_tcp.send(socket, [
      "HTTP/1.1 #{status} Fixture\r\nConnection: close\r\n",
      "Content-Length: #{byte_size(body)}\r\nETag: \"fixture\"\r\n",
      "Last-Modified: Thu, 01 Jan 2026 00:00:00 GMT\r\n",
      response_headers,
      "\r\n",
      if(method == :HEAD, do: "", else: body)
    ])
  catch
    :client_closed -> :ok
  after
    :gen_tcp.close(socket)
  end

  defp headers(socket, headers) do
    case read!(socket) do
      :http_eoh ->
        headers

      {:http_header, _, name, _, value} ->
        headers(socket, Map.put(headers, name |> to_string() |> String.downcase(), value))
    end
  end

  defp read!(socket) do
    case :gen_tcp.recv(socket, 0, 5_000) do
      {:ok, packet} -> packet
      {:error, :closed} -> throw(:client_closed)
      {:error, reason} -> raise "S3 fixture socket read failed: #{inspect(reason)}"
    end
  end

  defp object(data, %{"range" => range}) do
    [_, first, last] = Regex.run(~r/\Abytes=(\d+)-(\d*)\z/, range)
    first = String.to_integer(first)
    last = if last == "", do: byte_size(data) - 1, else: String.to_integer(last)
    last = min(last, byte_size(data) - 1)
    headers = "Content-Range: bytes #{first}-#{last}/#{byte_size(data)}\r\n"
    {206, headers, binary_part(data, first, last - first + 1)}
  end

  defp object(data, _headers), do: {200, [], data}

  defp listing(objects, query) do
    prefix = Map.get(query, "prefix", "")
    delimiter = Map.get(query, "delimiter", "")

    contents =
      for {key, data} <- Enum.sort(objects), String.starts_with?(key, prefix) do
        suffix = String.replace_prefix(key, prefix, "")

        if delimiter != "" and String.contains?(suffix, delimiter) do
          common = prefix <> hd(String.split(suffix, delimiter)) <> delimiter
          "<CommonPrefixes><Prefix>#{escape(common)}</Prefix></CommonPrefixes>"
        else
          "<Contents><Key>#{escape(key)}</Key><Size>#{byte_size(data)}</Size>" <>
            "<LastModified>2026-01-01T00:00:00.000Z</LastModified>" <>
            "<ETag>\"fixture\"</ETag></Contents>"
        end
      end

    IO.iodata_to_binary([
      "<ListBucketResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">",
      "<Name>probe</Name><Prefix>#{escape(prefix)}</Prefix><IsTruncated>false</IsTruncated>",
      Enum.uniq(contents),
      "</ListBucketResult>"
    ])
  end

  defp escape(value) do
    value
    |> String.replace("&", "&amp;")
    |> String.replace("<", "&lt;")
    |> String.replace(">", "&gt;")
  end
end
