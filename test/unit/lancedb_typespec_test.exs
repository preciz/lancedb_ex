defmodule LanceDB.TypespecTest do
  use ExUnit.Case, async: true

  test "every library public function has a typespec" do
    root = Path.expand("../..", __DIR__)

    missing =
      for module <- Application.spec(:lancedb_ex, :modules),
          source = module.module_info(:compile)[:source] |> to_string(),
          String.starts_with?(Path.relative_to(source, root), "lib/"),
          {:docs_v1, _, _, _, _, _, docs} = Code.fetch_docs(module),
          {:ok, specs} = Code.Typespec.fetch_specs(module),
          {{:function, name, arity}, _, _, _, _} <- docs,
          name != :__struct__,
          not (module == LanceDB.Error and name in [:exception, :message]),
          not (module == LanceDB.Native and
                 name in [:rustler_init, :load_rustler_precompiled]),
          {name, arity} not in Enum.map(specs, &elem(&1, 0)),
          do: "#{inspect(module)}.#{name}/#{arity}"

    assert missing == [], "Public functions without specs:\n" <> Enum.join(missing, "\n")
  end
end
