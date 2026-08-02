#!/usr/bin/env python3
"""resonate-on-convex NDJSON -> the unified model's canonical trace shape.

Same contract as from_pg.py: normalise SHAPE and ACTION NAMES, invent no
action arguments.

Two convex-specific points.

  * Convex's deadline jobs fire whenever the scheduler runs them and let the
    HANDLER decide, so a recorded job event is often a no-op.  The unified
    model's rules are guarded and cannot fire when not due, so an event whose
    normalised state is identical to its predecessor is emitted as `NoOp` --
    which the validator replays as a stutter that still checks the state.
    This is faithful, not a workaround: the event really did nothing.

  * Convex has no listeners, so `listeners` is NOT in `observed`.
"""
import json, sys

OBSERVED = ["promises", "tasks", "callbacks", "resumes", "outbox"]
INF = -1

NAMES = {
    "CreateRooted": "PromiseCreate", "CreatePlain": "PromiseCreate",
    "Acquire": "TaskAcquire", "Consume": "Deliver",
    "Suspend": "TaskSuspend", "Fulfill": "TaskFulfill",
    "SettleExternal": "PromiseSettle", "Heartbeat": "TaskHeartbeat",
    "TimeoutJob": "OnPromiseTimeout", "RetryJob": "OnTaskRetryTimeout",
    "LeaseJob": "OnTaskLeaseTimeout", "Tick": "AdvanceClock",
    # tryEagerTimeout on a read path: materialises one promise past its deadline
    "Converge": "OnPromiseTimeout",
}

# convex collapses the rejected family; the unified model keeps the protocol set
PSTATE = {"pending": "pending", "resolved": "resolved",
          "timedout": "rejected_timedout"}

def state(s):
    live_p = [i for i, v in s["pstate"].items() if v != "none"]
    live_t = [i for i, v in s["tstate"].items() if v != "none"]

    def timer(i):
        if s["tstate"][i] == "acquired" and s["tlease"][i] != INF:
            return ("lease", s["tlease"][i])
        if s["tstate"][i] == "pending" and s["tretry"][i] != INF:
            return ("retry", s["tretry"][i])
        return ("none", 0)

    return {
        "promises": {i: {"state": PSTATE[s["pstate"][i]],
                         "timeoutAt": s["ptimeout"][i],
                         # convex has no timer/external tags: targeted or plain
                         "kind": "target" if s["ptarget"][i] else "plain"}
                     for i in live_p},
        "tasks": {i: {"state": s["tstate"][i], "version": s["tver"][i],
                      "timerKind": timer(i)[0], "timerAt": timer(i)[1],
                      "ttl": (0 if s["tttl"][i] == INF else s["tttl"][i])}
                  for i in live_t},
        "callbacks": [list(c) for c in s["cbs"]],
        "listeners": [],
        "resumes": [[t, a] for t, aws in s["resumes"].items() for a in aws],
        "outbox": [{"kind": "execute", "id": i, "version": v, "addr": "-"}
                   for i, v in s["outbox"].items() if v != INF],
    }

def main():
    cfg, out, prev = None, [], None
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        d = json.loads(line)
        if d.get("tag") == "config":
            c = d["config"]
            cfg = {"tag": "config", "config": {
                "impl": "resonate-on-convex",
                "scenario": c.get("scenario", "?").split("/")[-1],
                "ids": c["ids"], "addrs": ["L"], "pids": c["pids"],
                "retry": c["retryTTL"], "ttl": c["leaseTTL"],
                "observed": OBSERVED}}
        elif d.get("tag") == "trace":
            e = d["event"]
            if e["name"] not in NAMES:
                sys.exit("unmapped action: " + e["name"])
            st = state(e["state"])
            # a scheduler job that changed nothing really did nothing
            name = NAMES[e["name"]]
            if prev is not None and st == prev and e["name"] in (
                    "TimeoutJob", "RetryJob", "LeaseJob", "Converge"):
                name = "NoOp"
            out.append({"tag": "trace", "event": {
                "name": name, "raw": e["name"], "now": e["now"], "state": st}})
            prev = st
    if cfg is None:
        sys.exit("no config line")
    print(json.dumps(cfg))
    for ev in out:
        print(json.dumps(ev))

main()
