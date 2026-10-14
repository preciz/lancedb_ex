#!/usr/bin/env bash
# Fresh consumer verification of public artifacts, without credentials or Rust.
# Optionally install from an unpacked Hex package instead of the Git checkout.
set -euo pipefail
package_root=$(cd "$(dirname "$0")/.." && pwd)
if [[ "$#" -gt 1 ]]; then
  echo 'Usage: scripts/validate_consumer.sh [UNPACKED_HEX_PACKAGE_DIRECTORY]' >&2
  exit 1
fi
hex_package=""
if [[ "$#" -eq 1 ]]; then
  hex_package=$(cd "$1" && pwd)
  test -f "$hex_package/mix.exs"
fi
validation_root=$(mktemp -d)
trap 'rm -rf "$validation_root"' EXIT
mkdir -p "$validation_root/consumer/vendor/lancedb_ex" "$validation_root/bin"
if [[ -n "$hex_package" ]]; then
  cp -R "$hex_package/." "$validation_root/consumer/vendor/lancedb_ex/"
else
  git -C "$package_root" archive HEAD | tar -x -C "$validation_root/consumer/vendor/lancedb_ex"
  # In publication CI these two files come directly from the trusted build jobs.
  cp "$package_root/native-build.exs" "$package_root/checksum-Elixir.LanceDB.Native.exs" "$validation_root/consumer/vendor/lancedb_ex/"
fi
cat > "$validation_root/consumer/mix.exs" <<'MIX'
defmodule NativeConsumer.MixProject do
  use Mix.Project
  def project, do: [app: :native_consumer, version: "0.1.0", deps: [{:lancedb_ex, path: "vendor/lancedb_ex"}]]
  def application, do: [extra_applications: [:logger]]
end
MIX
# Keep BEAM and standard shell utilities; no GitHub CLI, Rust tools or shims.
beam_bin=$(dirname "$(command -v erl)")
elixir_bin=$(dirname "$(command -v elixir)")
for tool in bash sh rebar3 uname dirname basename readlink getconf sed awk cut tr cat sort wc env mkdir rm cp ln find grep chmod head tail gzip tar git curl tee mktemp nice xargs openssl; do
  if tool_path=$(command -v "$tool"); then
    ln -s "$tool_path" "$validation_root/bin/$tool"
  fi
done
# Keep the original BEAM directories so launchers can locate their installation.
export PATH="$beam_bin:$elixir_bin:$validation_root/bin"
if command -v gh || command -v rustc || command -v cargo || command -v rustup; then
  echo 'GitHub CLI or Rust toolchain unexpectedly available in consumer PATH' >&2
  exit 1
fi
unset LANCEDB_BUILD LANCEDB_NIF_BASE_URL RUSTLER_PRECOMPILED_FORCE_BUILD_ALL
unset GH_TOKEN GITHUB_TOKEN GH_ENTERPRISE_TOKEN GH_HOST
unset TARGET_ARCH TARGET_OS TARGET_ABI TARGET_VENDOR
# This disposable project must use its own default build/dependency directories
# and the host target, regardless of the maintainer's current Mix configuration.
unset MIX_ENV MIX_TARGET MIX_BUILD_PATH MIX_BUILD_ROOT MIX_DEPS_PATH MIX_LOCKFILE MIX_EXS
export RUSTLER_PRECOMPILED_GLOBAL_CACHE_PATH="$validation_root/empty-nif-cache"
cd "$validation_root/consumer"
mix deps.get
mix compile --warnings-as-errors 2>&1 | tee "$validation_root/install.log"
mix run "$package_root/scripts/consumer_smoke.exs"
test "$(find "$RUSTLER_PRECOMPILED_GLOBAL_CACHE_PATH" -name '*.so.tar.gz' | wc -l)" -eq 1
if grep -q 'Compiling crate' "$validation_root/install.log"; then
  echo 'Consumer install attempted a native source build' >&2
  exit 1
fi

# The release carries the NIF. Runtime does not need the artifact host.
MIX_ENV=prod mix release --overwrite
mkdir -p "$validation_root/runtime-bin"
for tool_path in "$validation_root/bin/"*; do
  tool_name=$(basename "$tool_path")
  if [[ "$tool_name" != gh ]]; then
    ln -s "$tool_path" "$validation_root/runtime-bin/$tool_name"
  fi
done
env -u GH_TOKEN -u GITHUB_TOKEN -u GH_ENTERPRISE_TOKEN \
  PATH="$validation_root/runtime-bin" \
  _build/prod/rel/native_consumer/bin/native_consumer eval "
    Enum.each(~w(gh cargo rustc rustup), fn tool ->
      if System.find_executable(tool), do: raise(\"Unexpected runtime tool: \" <> tool)
    end)
    false = Enum.any?(Application.started_applications(), fn {app, _, _} -> app in [:rustler, :rustler_precompiled] end)
    Code.require_file(\"$package_root/scripts/consumer_smoke.exs\")
  "
echo 'Packaged release verified without GitHub credentials, gh or Rust.'

expect_failure() {
  local expected="$1"
  shift
  if "$@" > "$validation_root/failure.log" 2>&1; then
    echo "Expected installation failure: $expected" >&2
    exit 1
  fi
  if ! grep -q "$expected" "$validation_root/failure.log"; then
    cat "$validation_root/failure.log" >&2
    exit 1
  fi
  if grep -Eq 'Compiling crate|calling `cargo|Failed to compile .*rustc' "$validation_root/failure.log"; then
    cat "$validation_root/failure.log" >&2
    exit 1
  fi
  echo "Verified failure without Rust: $expected"
}
expect_failure 'precompiled NIF is not available for this target' env TARGET_ARCH=riscv64gc mix deps.compile lancedb_ex --force
export RUSTLER_PRECOMPILED_GLOBAL_CACHE_PATH="$validation_root/unavailable-cache"
expect_failure 'Error while downloading precompiled NIF' env LANCEDB_NIF_BASE_URL=http://127.0.0.1:1/ mix deps.compile lancedb_ex --force
# Populate the cache with an invalid archive under the exact immutable filename.
mkdir -p "$RUSTLER_PRECOMPILED_GLOBAL_CACHE_PATH"
archive_name=$(find "$validation_root/empty-nif-cache" -name '*.so.tar.gz' -exec basename {} \;)
printf 'invalid archive\n' > "$RUSTLER_PRECOMPILED_GLOBAL_CACHE_PATH/$archive_name"
expect_failure 'checksum of files does not match' mix deps.compile lancedb_ex --force
rm vendor/lancedb_ex/checksum-Elixir.LanceDB.Native.exs
expect_failure 'trusted checksum manifest is missing' mix deps.compile lancedb_ex --force
rm vendor/lancedb_ex/native-build.exs
expect_failure 'native-build.exs is missing' mix deps.compile lancedb_ex --force
echo 'Fresh consumer verification passed with an empty initial NIF cache, default URL, and no Rust or Cargo.'
