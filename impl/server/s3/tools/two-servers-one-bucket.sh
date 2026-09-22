#!/usr/bin/env bash
#
# Two servers, one bucket, over real sockets.
#
# The design claims that several servers sharing a bucket need nothing but a
# conditional write to agree: no log, no lease, no lock, no coordination. The
# simulator exercises that claim thoroughly and in one process, driving several
# server state machines over one in-memory store. Nothing exercised it across
# processes, where each server has its own document cache, its own deadline queue
# and its own event loop, and the only thing joining them is the object store.
#
# So: a stand-in S3, two server processes over one bucket, a round-robin front so
# a recorded history is a history of the *system*, and both checkers on the
# result — this directory's search and the specification's Porcupine.
#
# Then the same run again with `--sole-writer` on both, which tells each server it
# is alone and lets it answer from its cache without revalidating. That is a lie
# when there are two of them, and both checkers have to catch it. A check that
# cannot fail is not evidence.
#
# Usage: impl/server/s3/tools/two-servers-one-bucket.sh [operations] [clients]

set -uo pipefail

ops="${1:-1000}"
clients="${2:-12}"
store_port="${STORE_PORT:-9151}"
front_port="${FRONT_PORT:-8151}"
a_port="${A_PORT:-8152}"
b_port="${B_PORT:-8153}"
here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
zig_root="$(dirname "$here")"
# impl/server/s3 -> impl/server -> impl -> the repository root.
repo_root="$(cd "$zig_root/../../.." && pwd)"
bin="${BIN:-$zig_root/zig-out/bin}"
conctrace="${CONCTRACE:-$repo_root/impl/server/core/target/release/examples/conctrace}"
work="$(mktemp -d)"

pids=()
cleanup() {
  for pid in "${pids[@]:-}"; do [[ -n "$pid" ]] && kill "$pid" 2>/dev/null; done
  rm -rf "$work"
  return 0
}
trap cleanup EXIT

fail() { echo "FAILED: $*" >&2; exit 1; }

[[ -x "$conctrace" ]] || fail "no conctrace at $conctrace — cargo build --release --example conctrace"
command -v go > /dev/null || fail "go is not on PATH"

# `spec/` at the repository root, so nothing is fetched. A checkout split out of
# the monorepo falls back to cloning it.
spec="${SPEC_DIR:-$repo_root/spec}"
if [[ ! -d "$spec/valid/porc" ]]; then
  spec="$work/spec"
  GIT_LFS_SKIP_SMUDGE=1 git clone --depth 1 -q \
    https://github.com/resonatehq/resonate-specification "$spec" ||
    fail "no specification at $repo_root/spec and it could not be cloned"
fi
( cd "$spec/valid/porc" && GOFLAGS=-mod=readonly go build -o "$work/conccheck" ./cmd/conccheck ) ||
  fail "could not build conccheck"

"$bin/fakes3" --port "$store_port" > "$work/store.log" 2>&1 &
pids+=($!)
for _ in $(seq 1 30); do
  curl -sf -o /dev/null "http://127.0.0.1:$store_port/probe?list-type=2&prefix=" && break
  sleep 1
done

python3 "$here/two-servers-one-bucket.py" --port "$front_port" \
  --to "$a_port" --to "$b_port" > "$work/front.log" 2>&1 &
pids+=($!)

# One round: start two servers over `bucket`, record through the front, and print
# what each checker said. $1 is the bucket, the rest are extra server flags.
round() {
  local bucket="$1"; shift
  local a b
  "$bin/resonate" serve --debug --store s3 --endpoint "http://127.0.0.1:$store_port" \
    --bucket "$bucket" --port "$a_port" "$@" > "$work/$bucket-a.log" 2>&1 &
  a=$!
  pids+=("$a")
  "$bin/resonate" serve --debug --store s3 --endpoint "http://127.0.0.1:$store_port" \
    --bucket "$bucket" --port "$b_port" "$@" > "$work/$bucket-b.log" 2>&1 &
  b=$!
  pids+=("$b")
  for port in "$a_port" "$b_port" "$front_port"; do
    local up=false
    for _ in $(seq 1 30); do
      curl -sf -o /dev/null "http://127.0.0.1:$port/ready" && { up=true; break; }
      sleep 1
    done
    [[ "$up" == true ]] || fail "$bucket: nothing came ready on $port"
  done

  "$conctrace" --url "http://127.0.0.1:$front_port/" --out "$work/$bucket" \
    --clients "$clients" --ops "$ops" --seed 1 > "$work/$bucket.log" 2>&1
  grep -q "events" "$work/$bucket.log" || fail "$bucket: the producer refused the run"

  local served_a served_b
  served_a="$(curl -s "http://127.0.0.1:$a_port/metrics" | awk '/^resonate_requests_total/ {print $2}')"
  served_b="$(curl -s "http://127.0.0.1:$b_port/metrics" | awk '/^resonate_requests_total/ {print $2}')"
  [[ "${served_a:-0}" -gt 0 && "${served_b:-0}" -gt 0 ]] ||
    fail "$bucket: one server answered nothing, so this was not two servers"
  echo "  $served_a requests to one, $served_b to the other"

  mine=refuted
  "$bin/simulator" check "$work/$bucket.history" > "$work/$bucket.mine" 2>&1 &&
    grep -q "LINEARIZABLE" "$work/$bucket.mine" && mine=linearizable
  grep -q "gave up" "$work/$bucket.mine" && mine="the search gave up"
  theirs="$("$work/conccheck" -partition=false < "$work/$bucket.history" 2>&1 |
    grep -E '^-m' | sed -E 's/-m \(materialized\) +//; s/ +\(.*//; s/ +$//')"
  echo "  this repository's search: $mine"
  echo "  the specification's Porcupine: ${theirs:-no verdict}"

  kill "$a" "$b" 2>/dev/null
  wait "$a" "$b" 2>/dev/null
  sleep 1
}

echo "── two servers over one bucket, revalidating their caches"
round shared
[[ "$mine" == linearizable ]] || fail "this repository's search did not prove the honest run"
[[ "$theirs" == LINEARIZABLE ]] || fail "Porcupine did not prove the honest run: $theirs"

echo "── the same two, each told it is the only writer, which is a lie"
round lying --sole-writer
[[ "$mine" != linearizable ]] ||
  fail "this repository's search accepted two servers answering from stale caches"
[[ "$theirs" != LINEARIZABLE ]] ||
  fail "Porcupine accepted two servers answering from stale caches"

echo "two servers over one bucket agree, and both checkers catch it when they stop revalidating"
