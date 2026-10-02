"""Combine the RESULT lines of several load generators into one line."""
import json, sys
rs = [json.loads(l) for l in sys.stdin if l.strip()]
if not rs:
    print("no results"); sys.exit(1)
ops = sum(r["ops_per_s"] for r in rs)
req = sum(r["requests_per_s"] for r in rs)
failed = sum(r["ops_failed"] for r in rs)
v = {k: sum((r.get("verify") or {}).get(k, 0) for r in rs) for k in ("checked", "missing", "regressed", "unreadable")}
errors = {}
for r in rs:
    for k, n in {**r["errors"], **{"worker " + k: n for k, n in r["worker_errors"].items()}}.items():
        errors[k] = errors.get(k, 0) + n
srv = rs[0].get("server", {})
def s(k):
    x = srv.get(k)
    return f'{x["mean_ms"]:.2f}/{x["p99_ms"]:.2f}' if x else "-"
print(f'ops/s {ops:,.0f}  req/s {req:,.0f}  failed {failed}  '
      f'p50 {max(r["p50_ms"] for r in rs):.1f} ms  p99 {max(r["p99_ms"] for r in rs):.1f} ms  '
      f'p99.9 {max(r["p999_ms"] for r in rs):.1f} ms  max {max(r["max_ms"] for r in rs):.0f} ms')
print(f'server mean/p99 ms: edge {s("edge")}  queue {s("queue")}  round {s("round")}  commit {s("commit")}  '
      f'req/round {srv.get("requests_per_round", 0):.1f}  forwarded {srv.get("forwarded", 0):.0f}/{srv.get("served", 0):.0f}')
print(f'verify: {v["checked"]:,} read back, {v["missing"]} missing, {v["regressed"]} regressed, {v["unreadable"]} unreadable')
if errors:
    print("errors:", json.dumps(errors))
print("JSON", json.dumps({"ops_per_s": ops, "requests_per_s": req, "failed": failed,
      "p99_ms": max(r["p99_ms"] for r in rs), "p999_ms": max(r["p999_ms"] for r in rs), "verify": v, "server": srv}))
