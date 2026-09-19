#!/usr/bin/env bash
#
# A request that changes nothing costs nothing to have answered.
#
# Not a performance check: a check that the server does not grow by a few
# kilobytes per request. That is the one defect none of the other checks can see —
# the simulator drives the state machine in process, the tests answer a handful of
# requests and exit, and a differential compares answers rather than resident
# memory — and it is the one that kills a server that is otherwise correct.
#
# Usage: zig/tools/memory-stays-flat.sh [path-to-zig-out/bin]

set -euo pipefail

bin="${1:-zig-out/bin}"
store_port="${STORE_PORT:-9121}"
server_port="${SERVER_PORT:-8121}"
bucket="memory-check"
# Reads to take the measurement over, and the growth allowed across all of them.
# Allocators round up and hold pages back, so this is not zero; a leak of a few
# kilobytes a request would be a hundred times it.
reads="${READS:-3000}"
allowed_kb="${ALLOWED_KB:-1024}"
work="$(mktemp -d)"

cleanup() {
  [[ -n "${server_pid:-}" ]] && kill "$server_pid" 2>/dev/null || true
  [[ -n "${store_pid:-}" ]] && kill "$store_pid" 2>/dev/null || true
  rm -rf "$work"
}
trap cleanup EXIT

fail() {
  echo "FAILED: $*" >&2
  cat "$work/server.log" >&2 2>/dev/null || true
  exit 1
}

"$bin/fakes3" --port "$store_port" > "$work/store.log" 2>&1 &
store_pid=$!
for _ in $(seq 1 30); do
  curl -sf -o /dev/null "http://127.0.0.1:$store_port/$bucket?list-type=2&prefix=" && break
  sleep 1
done

"$bin/resonate" serve --debug --store s3 --endpoint "http://127.0.0.1:$store_port" \
  --bucket "$bucket" --port "$server_port" > "$work/server.log" 2>&1 &
server_pid=$!
for _ in $(seq 1 30); do
  curl -sf -o /dev/null "http://127.0.0.1:$server_port/ready" && break
  sleep 1
done
curl -sf -o /dev/null "http://127.0.0.1:$server_port/ready" || fail "the server never came ready"

READS="$reads" ALLOWED_KB="$allowed_kb" PORT="$server_port" PID="$server_pid" python3 - <<'PY' || fail "the measurement said no"
import http.client, json, os, sys

pid, port = os.environ["PID"], int(os.environ["PORT"])
reads, allowed = int(os.environ["READS"]), int(os.environ["ALLOWED_KB"])

def rss_kb():
    for line in open(f"/proc/{pid}/status"):
        if line.startswith("VmRSS"):
            return int(line.split()[1])
    raise SystemExit("the server is gone")

def send(c, body):
    c.request("POST", "/", json.dumps(body), {"Content-Type": "application/json"})
    r = c.getresponse()
    r.read()
    return r.status

def envelope(kind, data, now=1_000_000_000):
    return {
        "kind": kind,
        "head": {"corrId": "flat", "version": "2026-04-01", "resonate:debug_time": now},
        "data": data,
    }

# One connection, so nothing about this measures connection setup.
c = http.client.HTTPConnection("127.0.0.1", port)
status = send(c, envelope("promise.create", {"id": "flat:root.a", "timeoutAt": 9_000_000_000_000}))
if status != 200:
    raise SystemExit(f"the promise was not created: {status}")

# Warm up: the first requests grow the buffers everything after them reuses.
read = envelope("promise.get", {"id": "flat:root.a"})
for _ in range(300):
    send(c, read)

before = rss_kb()
for _ in range(reads):
    send(c, read)
after = rss_kb()

grew = after - before
print(f"{reads} reads: {before} -> {after} kB, {grew} kB, {grew * 1024 / reads:.0f} bytes a request")
if grew > allowed:
    raise SystemExit(f"grew {grew} kB over {reads} reads, which is more than the {allowed} kB allowed")
PY

# And the precise version of the same question, which the resident set is too
# coarse for: the allocator reports every allocation still outstanding when the
# process exits, and a graceful stop is what makes it exit rather than be killed.
kill -TERM "$server_pid"
for _ in $(seq 1 20); do
  kill -0 "$server_pid" 2>/dev/null || break
  sleep 1
done
kill -0 "$server_pid" 2>/dev/null && fail "the server did not stop when asked"
server_pid=""

if grep -q "leaked" "$work/server.log"; then
  echo "FAILED: the allocator reported memory still held after a clean stop:" >&2
  grep -c "leaked" "$work/server.log" >&2
  grep -m 5 "leaked" "$work/server.log" >&2
  exit 1
fi

echo "answering a request that changes nothing costs nothing to keep, and a clean stop holds nothing"
