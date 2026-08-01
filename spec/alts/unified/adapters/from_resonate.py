#!/usr/bin/env python3
"""resonate (Rust) NDJSON -> the unified model's canonical trace shape.

The richest of the three harnesses: it is the only one that records the
delivery stage, worker-side claims, and per-task versions, so its traces are
the only ones that can check the FENCING layer -- at-most-once execution.

Three resonate-specific points.

  * There is no config line, so one is synthesised.  Retry and Ttl are
    DERIVED from the trace itself (the gap between the clock and a freshly
    armed timer) rather than assumed.

  * `process_timeouts` is recorded as ONE event although it runs three SQL
    statements.  It is emitted as `Batch`, which the validator replays as a
    sequence of internal rules.  `ClientGet` is the same shape: the ghost
    `try_timeout` step may settle a promise.

  * resonate's model collapses resolved / rejected / rejected_canceled into
    a single `settled`.  The adapter maps it to `resolved`.  The DISTINCTION
    is therefore not checked for this implementation -- the harness did not
    record it.  Every settled state is terminal and non-pending, so nothing
    in the model's behaviour turns on which one it was.
"""
import json, sys

OBSERVED = ["promises", "tasks", "callbacks", "outbox",
            "delivered", "claim", "resumeCounts"]

NAMES = {
    "AdvanceClock": "AdvanceClock", "CreatePromise": "PromiseCreate",
    "DeliverExecute": "Deliver", "DropExecute": "DropMsg",
    "Acquire": "TaskAcquire", "Suspend": "TaskSuspend",
    "Fulfill": "TaskFulfill", "Release": "TaskRelease",
    "Heartbeat": "TaskHeartbeat", "ExternalSettle": "PromiseSettle",
    "WorkerCrash": "WorkerCrash",
    # batches: one recorded event, several model rules
    "ProcessTimeouts": "Batch", "ClientGet": "Touch",
}

PSTATE = {"pending": "pending", "settled": "resolved",
          "timedout": "rejected_timedout"}

def state(s):
    live_p = [i for i, v in s["pstate"].items() if v != "none"]
    live_t = [i for i, v in s["tstate"].items() if v != "none"]
    return {
        "promises": {i: {"state": PSTATE[s["pstate"][i]],
                         "timeoutAt": s["deadline"][i],
                         "kind": "target" if s["hasTarget"][i] else "plain"}
                     for i in live_p},
        "tasks": {i: {"state": s["tstate"][i], "version": s["tversion"][i],
                      "timerKind": s["tTimer"][i]["kind"],
                      "timerAt": s["tTimer"][i]["at"]}
                  for i in live_t},
        # resonate keeps a callback row after it fires, flagged ready; the
        # harness exports only the UNREADY ones, which is exactly the unified
        # model's `callbacks`.  The fired ones are the resume counts below.
        "callbacks": [[c["awaited"], c["awaiter"]] for c in s["unreadyCallbacks"]],
        "listeners": [],
        "resumes": [],
        "resumeCounts": {i: s["resumes"][i] for i in s["resumes"]},
        "outbox": [{"kind": "execute", "id": m["task"], "version": m["version"],
                    "addr": "-"} for m in s["outbox"]],
        "delivered": [{"id": m["task"], "version": m["version"]}
                      for m in s["delivered"]],
        "claim": {w: {"task": ("-" if c["task"] == "NULL" else c["task"]),
                      "version": c["version"]}
                  for w, c in s["claim"].items()},
    }

def derive_ttls(events):
    """Retry and Ttl, read off the trace instead of assumed."""
    retry = ttl = None
    for e in events:
        now, s = e["state"]["clock"], e["state"]
        for i, t in s["tTimer"].items():
            if t["kind"] == "retry" and t["at"] > now and retry is None:
                retry = t["at"] - now
            if t["kind"] == "lease" and t["at"] > now and ttl is None:
                ttl = t["at"] - now
    return retry or 1, ttl or 1

def main():
    raw = [json.loads(l) for l in sys.stdin if l.strip()]
    events = [d["event"] for d in raw if "event" in d]
    if not events:
        sys.exit("no events")
    s0 = events[0]["state"]
    retry, ttl = derive_ttls(events)
    print(json.dumps({"tag": "config", "config": {
        "impl": "resonate", "scenario": "?",
        "ids": sorted(s0["pstate"].keys()), "addrs": ["L"],
        "pids": sorted(s0["claim"].keys()),
        "retry": retry, "ttl": ttl, "observed": OBSERVED}}))
    for e in events:
        if e["name"] not in NAMES:
            sys.exit("unmapped action: " + e["name"])
        print(json.dumps({"tag": "trace", "event": {
            "name": NAMES[e["name"]], "raw": e["name"],
            "now": e["state"]["clock"], "state": state(e["state"])}}))

main()
