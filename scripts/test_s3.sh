#!/usr/bin/env bash
# Run S3 integration tests against a checksum-verified, disposable MinIO server.
set -euo pipefail
cd "$(dirname "$0")/.."

# Digests from the official GitHub release assets. Do not trust checksums fetched
# alongside a binary at runtime. The release matches the original Docker fixture.
minio_release="RELEASE.2025-09-07T16-13-09Z"
case "$(uname -s)-$(uname -m)" in
  Linux-x86_64) minio_platform=linux-amd64; minio_digest=7c5bd8512c6e966455b1d198209358b2d191c77a83ab377c4073281065fb855f ;;
  Linux-aarch64|Linux-arm64) minio_platform=linux-arm64; minio_digest=5c83cd2cf151717ba0243f73e1c7802ff36e272b67144bdd7f1f7d684fd6f03d ;;
  *) echo "Unsupported MinIO test platform: $(uname -s)-$(uname -m)" >&2; exit 1 ;;
esac

minio_cache="_build/minio/$minio_release"
minio_binary="$minio_cache/minio.$minio_platform"
mkdir -p "$minio_cache"
minio_tmp=$(mktemp -d "${TMPDIR:-/tmp}/lancedb-minio.XXXXXXXX")
minio_pid=""
cleanup() {
  minio_status=$?
  trap - EXIT
  if [[ "$minio_status" != 0 && -f "$minio_tmp/server.log" ]]; then
    cat "$minio_tmp/server.log" >&2
  fi
  if [[ -n "$minio_pid" ]]; then
    kill "$minio_pid" 2>/dev/null || true
    wait "$minio_pid" 2>/dev/null || true
  fi
  rm -rf "$minio_tmp"
  exit "$minio_status"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

verify_minio() {
  local digest
  if command -v sha256sum >/dev/null; then
    digest=$(sha256sum "$1")
  else
    digest=$(shasum -a 256 "$1")
  fi
  [[ "${digest%% *}" == "$minio_digest" ]]
}

if [[ -f "$minio_binary" ]] && ! verify_minio "$minio_binary"; then
  echo "Discarding invalid cached MinIO binary: $minio_binary" >&2
  rm "$minio_binary"
fi
if [[ ! -f "$minio_binary" ]]; then
  curl --fail --location --silent --show-error --retry 3 \
    "https://github.com/minio/minio/releases/download/$minio_release/minio.$minio_platform.$minio_release" \
    --output "$minio_tmp/download"
  if ! verify_minio "$minio_tmp/download"; then
    echo "MinIO test binary checksum mismatch; download was not cached" >&2
    exit 1
  fi
  mv "$minio_tmp/download" "$minio_binary"
fi
chmod +x "$minio_binary"

# Reserve an ephemeral localhost port briefly. Startup/readiness also checks that
# our own child is alive, so a bind failure cannot silently use another server.
minio_port=$(elixir --erl '+S 2:2' -e '{:ok, socket} = :gen_tcp.listen(0, ip: {127, 0, 0, 1}); {:ok, port} = :inet.port(socket); IO.puts(port)')
MINIO_ROOT_USER=lancedb-test MINIO_ROOT_PASSWORD=lancedb-test-secret \
  "$minio_binary" server "$minio_tmp/data" --address "127.0.0.1:$minio_port" \
  --console-address "127.0.0.1:0" >"$minio_tmp/server.log" 2>&1 &
minio_pid=$!

export LANCEDB_TEST_S3_ENDPOINT="http://127.0.0.1:$minio_port"
export LANCEDB_TEST_S3_URI="s3://lancedb-ci/integration"
export LANCEDB_TEST_S3_SECONDARY_URI="s3://lancedb-ci-clone/integration"
export LANCEDB_TEST_S3_ACCESS_KEY="lancedb-test"
export LANCEDB_TEST_S3_SECRET_KEY="lancedb-test-secret"

minio_ready=false
for _ in {1..60}; do
  if ! kill -0 "$minio_pid" 2>/dev/null; then
    echo "MinIO test server exited before becoming ready" >&2
    exit 1
  fi
  if curl --fail --silent --max-time 2 "$LANCEDB_TEST_S3_ENDPOINT/minio/health/live" >/dev/null; then
    minio_ready=true
    break
  fi
  sleep 1
done
if [[ "$minio_ready" != true ]]; then
  echo "MinIO did not become ready" >&2
  exit 1
fi

# Older curl versions omit the ephemeral port when signing Host.
for bucket in lancedb-ci lancedb-ci-clone; do
  curl --fail-with-body --silent --show-error --max-time 10 \
    --aws-sigv4 "aws:amz:us-east-1:s3" \
    --user "$LANCEDB_TEST_S3_ACCESS_KEY:$LANCEDB_TEST_S3_SECRET_KEY" \
    --header "Host: 127.0.0.1" \
    --request PUT "$LANCEDB_TEST_S3_ENDPOINT/$bucket"
done

# An explicit command can reuse the verified, disposable server for benchmarks.
if [[ "$#" -gt 0 ]]; then
  "$@"
else
  mix test test/lancedb_s3_test.exs --include s3 --warnings-as-errors
fi
