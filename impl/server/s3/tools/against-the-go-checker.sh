#!/usr/bin/env bash
#
# This server's histories, put to somebody else's checker.
#
# Every other check here grades this server against something written alongside it
# — the simulator's search replays the same state machine, and the differential
# compares against the core server. Agreement there is agreement with ourselves.
# So this one hands a recorded history to Porcupine, driven by the model under
# `spec/`, which nobody working on this directory wrote.
#
# `conccheck`, not `lincheck`: `lincheck` asks whether the one order the recorder
# happened to write down satisfies the model, and on a concurrent run refutes
# almost anything, because return order is one legal linearization out of many.
# `conccheck` reads the real call and return instants and asks whether ANY order
# consistent with them works. Only its refutation is a claim about the server.
#
# `-partition=false` because upstream's `originOf` splits an id on '.', so it
# reads every ':'-id as its own partition. Unpartitioned replays against whole
# state, which is the stronger question anyway.
#
# Usage: impl/server/s3/tools/against-the-go-checker.sh [seeds]
#
# Needs `go`, a `conctrace` built from the Rust tree
# (`cargo build --release --example conctrace`), and this directory's own
# binaries. It reads the specification out of `spec/`, so it fetches nothing.

set -uo pipefail

seeds="${1:-10}"
ops="${OPS:-1000}"
clients="${CLIENTS:-12}"
store_port="${STORE_PORT:-9131}"
server_port="${SERVER_PORT:-8131}"
bucket="porcupine"
# Resolved from this script rather than from the caller's directory, so it runs
# the same from the repository root and from this directory.
here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
zig_root="$(dirname "$here")"
# impl/server/s3 -> impl/server -> impl -> the repository root.
repo_root="$(cd "$zig_root/../../.." && pwd)"
bin="${BIN:-$zig_root/zig-out/bin}"
conctrace="${CONCTRACE:-$repo_root/impl/server/core/target/release/examples/conctrace}"
work="$(mktemp -d)"

cleanup() {
  [[ -n "${server_pid:-}" ]] && kill "$server_pid" 2>/dev/null
  [[ -n "${store_pid:-}" ]] && kill "$store_pid" 2>/dev/null
  rm -rf "$work"
  return 0
}
trap cleanup EXIT

fail() { echo "FAILED: $*" >&2; exit 1; }

[[ -x "$conctrace" ]] || fail "no conctrace at $conctrace — cargo build --release --example conctrace"
command -v go > /dev/null || fail "go is not on PATH"

# ── The checker, from the specification, which is a sibling of this tree ──────
#
# `spec/` at the repository root is the specification: the abstract machine, the
# properties, the TLA+ model and the Go trace checker under `valid/porc`. Nothing
# is fetched when it is there, which is what makes this check runnable offline.
# SPEC_DIR overrides it, and a checkout that has been split out of the monorepo
# falls back to cloning it.
spec="${SPEC_DIR:-$repo_root/spec}"
if [[ ! -d "$spec/valid/porc" ]]; then
  spec="$work/spec"
  GIT_LFS_SKIP_SMUDGE=1 git clone --depth 1 -q \
    https://github.com/resonatehq/resonate-specification "$spec" ||
    fail "no specification at $repo_root/spec and it could not be cloned"
fi
( cd "$spec/valid/porc" && GOFLAGS=-mod=readonly go build -o "$work/conccheck" ./cmd/conccheck ) ||
  fail "could not build conccheck"
# Its workload generator too, so a run can be theirs end to end.
( cd "$spec/valid/porc" && GOFLAGS=-mod=readonly go build -o "$work/loadgen" ./cmd/loadgen ) ||
  fail "could not build loadgen"

# ── A server over a stand-in S3, so this needs no bucket ──────────────────────
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

# ── A check that cannot fail is not evidence: break one answer on purpose ─────
"$conctrace" --url "http://127.0.0.1:$server_port/" --out "$work/control" \
  --clients 4 --ops 200 --seed 1 > "$work/control.log" 2>&1 ||
  fail "the producer refused the control run"
"$work/conccheck" -partition=false < "$work/control.history" > /dev/null 2>&1 ||
  fail "the control history was refuted before anything was changed to it"
python3 - "$work/control.history" "$work/lied.history" <<'PY'
import json, sys
lines = [json.loads(l) for l in open(sys.argv[1]) if l.strip()]
for ev in lines:
    res = ev.get("res") or {}
    if res.get("head", {}).get("status") != 200:
        continue
    p = (res.get("data") or {}).get("promise") if isinstance(res.get("data"), dict) else None
    if isinstance(p, dict) and p.get("state") == "pending":
        p["state"] = "resolved"
        break
else:
    raise SystemExit("no answer to change")
with open(sys.argv[2], "w") as f:
    for ev in lines:
        f.write(json.dumps(ev) + "\n")
PY
if "$work/conccheck" -partition=false < "$work/lied.history" > /dev/null 2>&1; then
  fail "the checker accepted a history with one answer changed, so it is not checking"
fi
echo "control: one answer changed and the checker refuses the file"

# ── The real thing, over seeds ────────────────────────────────────────────────
proved=0; slow=0; refuted=0
for seed in $(seq 1 "$seeds"); do
  "$conctrace" --url "http://127.0.0.1:$server_port/" --out "$work/h" \
    --clients "$clients" --ops "$ops" --seed "$seed" > "$work/h.log" 2>&1
  grep -q "events" "$work/h.log" || fail "seed $seed: the producer refused the run"
  conc="$(grep -oE 'max concurrency: [0-9]+' "$work/h.log" | grep -oE '[0-9]+')"
  out="$("$work/conccheck" -partition=false < "$work/h.history" 2>&1)"
  verdict="$(echo "$out" | grep -E '^-m' | sed -E 's/-m \(materialized\) +//; s/ +\(.*//; s/ +$//')"
  case "$verdict" in
    LINEARIZABLE) proved=$((proved+1)); echo "seed $seed: $conc at once, linearizable";;
    TIMEOUT*)     slow=$((slow+1));     echo "seed $seed: $conc at once, the search ran out of time";;
    *)            refuted=$((refuted+1)); echo "seed $seed: $conc at once, REFUTED"
                  echo "$out" >&2
                  cp "$work/h.history" "$repo_root/refuted-$seed.history"
                  echo "the history is in $repo_root/refuted-$seed.history" >&2;;
  esac
done

echo "$proved proved, $slow timed out, $refuted refuted, over $seeds seeds"
[[ "$refuted" -eq 0 ]] || exit 1

# -- And the same repository's own generator, so nothing in the loop is ours ----
#
# `conctrace` above is this repository's. `loadgen` is theirs, and it reaches
# states `conctrace` never builds: callbacks, heartbeats, awaits across origins,
# sub-origins. It also probes the boundary the whole design rests on -- an awaiter
# and an awaited in different origins, a heartbeat spanning two, an origin with a
# ':' in it -- all of which the protocol refuses, so a large share of any run is
# refusals by design. `resonate-server-blob` refuses the same ones: at one client
# the two servers' status profiles are identical, 101 answered, 185 refused as
# malformed, 297 not found, 17 conflicted.
#
# Its ids are the limit on how much state it builds: the workflow index is
# `i / 6` over a counter shared by every client, while the origin is per client,
# so at eight clients a given id is touched about once and nothing accumulates.
# One client gives depth and no concurrency; eight give concurrency across origins
# and little depth. Both are run, and each has a floor on how much has to have
# succeeded, because a history in which nothing worked is linearizable for free.
for lg_clients in 1 2 8; do
  "$work/loadgen" --url "http://127.0.0.1:$server_port/" --out "$work/lg$lg_clients" \
    --clients "$lg_clients" --ops 600 --seed 1 > "$work/lg$lg_clients.log" 2>&1 ||
    fail "loadgen failed at $lg_clients clients"
  read -r events ok < <(python3 "$here/count-successes.py" "$work/lg$lg_clients.history")
  # Floors, not targets: at one client it reaches about a hundred, at eight about
  # sixty. Anything far below means the generator stopped building state.
  [[ "$events" -ge 500 ]] || fail "loadgen at $lg_clients clients recorded only $events events"
  [[ "$ok" -ge 40 ]] ||
    fail "loadgen at $lg_clients clients: only $ok of $events succeeded, which checks nothing"
  verdict="$("$work/conccheck" -partition=false < "$work/lg$lg_clients.history" 2>&1 |
    grep -E '^-m' | sed -E 's/-m \(materialized\) +//; s/ +\(.*//; s/ +$//')"
  echo "loadgen, $lg_clients clients: $events events, $ok succeeded, $verdict"
  [[ "$verdict" == LINEARIZABLE ]] || exit 1
done
