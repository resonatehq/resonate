#!/usr/bin/env bash
#
# A deadline survives a restart.
#
# The one path neither the simulator nor the differential exercises, because both
# of them run in debug mode where the clock belongs to the caller: a server that
# comes up, reads the deadlines out of the bucket, and fires one on wall time with
# nobody asking. That is the whole durability promise of a timer, so it is checked
# here, end to end, over the code that talks HTTP to an object store.
#
# Usage: zig/tools/deadline-survives-a-restart.sh [path-to-zig-out/bin]
#
# Exits non-zero, loudly, on the first thing that is not true.

set -euo pipefail

bin="${1:-zig-out/bin}"
store_port="${STORE_PORT:-9111}"
server_port="${SERVER_PORT:-8111}"
bucket="restart-check"
work="$(mktemp -d)"

cleanup() {
  [[ -n "${server_pid:-}" ]] && kill "$server_pid" 2>/dev/null || true
  [[ -n "${store_pid:-}" ]] && kill "$store_pid" 2>/dev/null || true
  rm -rf "$work"
}
trap cleanup EXIT

fail() {
  echo "FAILED: $*" >&2
  echo "--- the store's keys:" >&2
  curl -sS "http://127.0.0.1:$store_port/$bucket?list-type=2&prefix=" >&2 || true
  echo >&2
  echo "--- the server said:" >&2
  cat "$work/server.log" "$work/server2.log" 2>/dev/null >&2 || true
  exit 1
}

post() {
  curl -sS -m 10 -X POST "http://127.0.0.1:$server_port/" \
    -H 'Content-Type: application/json' -d "$1"
}

keys() {
  curl -sS -m 10 "http://127.0.0.1:$store_port/$bucket?list-type=2&prefix=${1:-}"
}

wait_for() {
  local what="$1" tries="$2"
  shift 2
  for _ in $(seq 1 "$tries"); do
    if "$@" >/dev/null 2>&1; then return 0; fi
    sleep 1
  done
  fail "$what"
}

# ── A stand-in S3, and a server with no debug mode: the clock is the wall's ───
"$bin/fakes3" --port "$store_port" > "$work/store.log" 2>&1 &
store_pid=$!
wait_for "the stand-in S3 never came up" 30 curl -sf -o /dev/null "http://127.0.0.1:$store_port/$bucket?list-type=2&prefix="

"$bin/resonate" serve --store s3 --endpoint "http://127.0.0.1:$store_port" \
  --bucket "$bucket" --port "$server_port" > "$work/server.log" 2>&1 &
server_pid=$!
wait_for "the server never came ready" 30 curl -sf -o /dev/null "http://127.0.0.1:$server_port/ready"

# ── A promise that times out in eight seconds, and owes a deadline ────────────
now="$(python3 -c 'import time; print(int(time.time()*1000))')"
deadline=$((now + 8000))
created="$(post "{\"kind\":\"promise.create\",\"head\":{\"corrId\":\"restart\",\"version\":\"2026-04-01\"},
  \"data\":{\"id\":\"restart:root.a\",\"timeoutAt\":$deadline,
  \"tags\":{\"resonate:target\":\"http://127.0.0.1:9/nobody\",\"resonate:timer\":\"true\"}}}")"
echo "$created" | grep -q '"status":200' || fail "the promise was not created: $created"

keys "t/" | grep -q "<Key>" || fail "no deadline object was written"

# ── Stop it before the deadline, and start it again ───────────────────────────
kill "$server_pid"
wait "$server_pid" 2>/dev/null || true
server_pid=""

"$bin/resonate" serve --store s3 --endpoint "http://127.0.0.1:$store_port" \
  --bucket "$bucket" --port "$server_port" > "$work/server2.log" 2>&1 &
server_pid=$!
wait_for "the server never came ready again" 30 curl -sf -o /dev/null "http://127.0.0.1:$server_port/ready"

grep -q "deadline queue seeded from the store: 1 armed" "$work/server2.log" ||
  fail "the restarted server did not read the deadline out of the bucket"

# ── Nobody asks it to: the deadline fires on its own ─────────────────────────
settled=false
for _ in $(seq 1 30); do
  got="$(post '{"kind":"promise.get","head":{"corrId":"restart","version":"2026-04-01"},
    "data":{"id":"restart:root.a"}}')"
  # The *stored* state, not a projection: `promise.get` settles what it reads, so
  # the deadline object going is what says the sweep did it.
  if [[ "$(keys 't/')" != *"<Key>"* ]]; then
    settled=true
    break
  fi
  sleep 1
done
[[ "$settled" == true ]] && : || fail "the deadline never fired: $got"

got="$(post '{"kind":"promise.get","head":{"corrId":"restart","version":"2026-04-01"},
  "data":{"id":"restart:root.a"}}')"
# A timer resolves where anything else would be rejected as timed out.
echo "$got" | grep -q '"state":"resolved"' || fail "the promise did not resolve: $got"
echo "$got" | grep -q "\"settledAt\":$deadline" || fail "it settled at the wrong instant: $got"

echo "a deadline written before a restart fired after it, on wall time, with nobody asking"
