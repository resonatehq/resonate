#!/usr/bin/env python3
"""resonate-pg NDJSON  ->  the unified model's canonical trace shape.

The adapter normalises SHAPE and ACTION NAMES only.  It never invents an
action argument: the recorded traces carry none, so UTrace.tla recovers them
by existential quantification constrained by post-state agreement -- the same
technique resonate-pg's own Trace.tla uses.

resonate-pg records six state components.  Everything the unified model has
beyond them (task pid, worker claims, the delivery stage, and the response
channel) is UNOBSERVED and is declared as such in the config line, so the
validator can state exactly what it checked instead of implying it checked
everything.
"""
import json, sys

OBSERVED = ["promises", "tasks", "callbacks", "listeners", "resumes", "outbox"]

# resonate-pg's action vocabulary -> the unified model's.
NAMES = {
    "PromiseCreate": "PromiseCreate", "PromiseSettle": "PromiseSettle",
    "RegisterCallback": "RegisterCallback", "RegisterListener": "RegisterListener",
    "TaskClaim": "TaskClaim", "TaskAcquire": "TaskAcquire",
    "TaskSuspend": "TaskSuspend", "TaskSuspend300": "TaskSuspend",
    "TaskFulfill": "TaskFulfill", "TaskRelease": "TaskRelease",
    "TaskHalt": "TaskHalt", "TaskContinue": "TaskContinue",
    "OnPromiseTimeout": "OnPromiseTimeout",
    "OnTaskRetryTimeout": "OnTaskRetryTimeout",
    "OnTaskLeaseTimeout": "OnTaskLeaseTimeout",
    "Dequeue": "Deliver", "Tick": "AdvanceClock",
}

def kind(p):
    # the unified model collapses three booleans into one tag
    if p.get("hasTarget"): return "target"
    if p.get("isTimer"):   return "timer"
    if p.get("external"):  return "ext"
    return "plain"

def timer_kind(t):
    # resonate-pg keeps ONE deadline column disambiguated by state; the
    # unified model names the timer, so the name is derived from the state
    return {"pending": "retry", "acquired": "lease"}.get(t["state"], "none")

def state(s):
    return {
        "promises": {i: {"state": p["state"], "timeoutAt": p["timeoutAt"],
                         "kind": kind(p)}
                     for i, p in s["promises"].items()},
        "tasks": {i: {"state": t["state"], "version": t["version"],
                      "timerKind": timer_kind(t), "timerAt": t["timeoutAt"]}
                  for i, t in s["tasks"].items()},
        "callbacks": [list(c) for c in s["callbacks"]],
        "listeners": [list(l) for l in s["listeners"]],
        "resumes":   [list(r) for r in s["resumes"]],
        "outbox":    [{"kind": "execute", "id": e[0], "version": e[1], "addr": "-"}
                      for e in s["execs"]]
                   + [{"kind": "unblock", "id": u[0], "version": 0, "addr": u[1]}
                      for u in s["unblocks"]],
    }

def main():
    cfg = None
    out = []
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        d = json.loads(line)
        if d.get("tag") == "config":
            c = d["config"]
            cfg = {"tag": "config", "config": {
                "impl": "resonate-pg", "scenario": c.get("scenario", "?"),
                "ids": c["ids"], "addrs": c["addrs"],
                "retry": c["retry"], "ttl": c["ttl"],
                "observed": OBSERVED}}
        elif d.get("tag") == "trace":
            e = d["event"]
            if e["name"] not in NAMES:
                sys.exit("unmapped action: " + e["name"])
            out.append({"tag": "trace", "event": {
                "name": NAMES[e["name"]], "raw": e["name"],
                "now": e["now"], "state": state(e["state"])}})
    if cfg is None:
        sys.exit("no config line")
    # the addresses actually used, so the model's Addrs can be exact
    addrs = sorted({l[1] for ev in out for l in ev["event"]["state"]["listeners"]}
                   | set(cfg["config"]["addrs"]))
    cfg["config"]["addrs"] = addrs
    print(json.dumps(cfg))
    for ev in out:
        print(json.dumps(ev))

main()
