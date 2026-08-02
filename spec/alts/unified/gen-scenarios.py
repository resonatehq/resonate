#!/usr/bin/env python3
"""Turn a TLC counterexample into a scenario and a canonical trace.

The model already knows how to reach every bug condition -- a mutation's
counterexample IS the shortest such execution.  This converts one into:

  scenarios/<bug>.md      the ordered calls, for a harness author to script
  traces/gen-<bug>.ndjson a canonical trace whose STATES are the MUTANT's,
                          i.e. what an implementation with this defect would
                          actually record

Replaying the second against the (unmutated) model must REFUSE.  That is the
point: it demonstrates the detection path end to end, without waiting for
someone to write the scenario by hand.  It is not circular -- the states come
from the buggy machine and are checked against the correct one.
"""
import json, re, sys

A2NAME = {
    "APromiseCreate": "PromiseCreate", "APromiseSettle": "PromiseSettle",
    "ARegisterCallback": "RegisterCallback", "ARegisterListener": "RegisterListener",
    "APromiseGet": "PromiseGet", "ATaskGet": "TaskGet",
    "ATaskCreate": "TaskClaim", "ATaskAcquire": "TaskAcquire",
    "ATaskSuspend": "TaskSuspend", "ATaskFulfill": "TaskFulfill",
    "ATaskRelease": "TaskRelease", "ATaskHeartbeat": "TaskHeartbeat",
    "ATaskHalt": "TaskHalt", "ATaskContinue": "TaskContinue",
    "AOnPromiseTimeout": "OnPromiseTimeout", "AClientTouch": "ClientTouch",
    "AOnTaskRetry": "OnTaskRetryTimeout", "AOnTaskLease": "OnTaskLeaseTimeout",
    "ADeliver": "Deliver", "ADropMsg": "DropMsg",
    "AWorkerCrash": "WorkerCrash", "ATick": "AdvanceClock",
}
RPC = {  # how a harness would actually call it
    "PromiseCreate": "promise.create", "PromiseSettle": "promise.settle",
    "RegisterCallback": "promise.register_callback",
    "RegisterListener": "promise.register_listener",
    "PromiseGet": "promise.get", "TaskGet": "task.get",
    "TaskClaim": "task.create", "TaskAcquire": "task.acquire",
    "TaskSuspend": "task.suspend", "TaskFulfill": "task.fulfill",
    "TaskRelease": "task.release", "TaskHeartbeat": "task.heartbeat",
    "TaskHalt": "task.halt", "TaskContinue": "task.continue",
    "OnPromiseTimeout": "(background) promise timeout",
    "ClientTouch": "(any read that names the promise)",
    "OnTaskRetryTimeout": "(background) task retry",
    "OnTaskLeaseTimeout": "(background) lease expiry",
    "Deliver": "(transport) deliver execute", "DropMsg": "(fault) drop message",
    "WorkerCrash": "(fault) worker dies holding its claim",
    "AdvanceClock": "wait until",
}

def live(st, i, now):
    p = st["promises"][i]
    if not p["exists"]: return "none"
    if p["state"] == "pending" and p["timeoutAt"] <= now:
        return "resolved" if p["kind"] == "timer" else "rejected_timedout"
    return p["state"]

def canon(st, now):
    return {
        "promises": {i: {"state": p["state"], "timeoutAt": p["timeoutAt"],
                         "kind": p["kind"]}
                     for i, p in st["promises"].items() if p["exists"]},
        "tasks": {i: {"state": t["state"], "version": t["version"],
                      "timerKind": t["timerKind"], "timerAt": t["timerAt"],
                      "ttl": t["ttl"]}
                  for i, t in st["tasks"].items() if t["exists"]},
        "callbacks": [list(c) for c in st["callbacks"]],
        "listeners": [list(c) for c in st["listeners"]],
        "resumes":   [list(c) for c in st["resumes"]],
        "outbox":    [dict(m) for m in st["outbox"]],
    }

def describe(name, prev, cur, now):
    """One human-readable line, from the state delta."""
    if name == "PromiseCreate":
        new = [i for i in cur["promises"]
               if cur["promises"][i]["exists"] and not prev["promises"][i]["exists"]]
        if new:
            i = new[0]; p = cur["promises"][i]
            tag = {"target": "resonate:target", "timer": "resonate:timer",
                   "ext": "resonate:external", "plain": "(no tags -- INTERNAL)"}[p["kind"]]
            return f'promise.create id={i} timeoutAt={p["timeoutAt"]} tags={tag}'
    if name == "AdvanceClock":
        return f"wait until now = {now}"
    for i in cur["tasks"]:
        if cur["tasks"][i] != prev["tasks"][i]:
            return f'{RPC[name]} id={i}'
    for i in cur["promises"]:
        if cur["promises"][i] != prev["promises"][i]:
            return f'{RPC[name]} id={i}'
    return RPC[name]

def main(textout, jsonout, bug, ids_note=""):
    text = open(textout).read()
    acts = [a for a in re.findall(r"^State \d+: <(\w+) ", text, re.M) if a != "Initial"]
    prop = re.search(r"Invariant (\w+) is violated", text)
    prop = prop.group(1) if prop else "?"
    states = [a[0][1] for a in json.load(open(jsonout))["counterexample"]["action"]]
    names = ["Init"] + [A2NAME[a] for a in acts]

    cfg = {"tag": "config", "config": {
        "impl": "MODEL-GENERATED (" + bug + ")", "scenario": bug,
        "ids": sorted(states[0]["promises"].keys()), "addrs": ["L"],
        "pids": sorted(states[0]["claim"].keys()),
        "retry": 1, "ttl": 1,
        "observed": ["promises", "tasks", "callbacks", "listeners", "resumes", "outbox"]}}
    lines = [json.dumps(cfg)]
    md = [f"# Scenario: {bug}", "",
          f"Model-generated from the counterexample that violates `{prop}`.",
          f"{len(states)-1} steps. Run this against a real implementation, record the",
          "trace, and replay it: a conformant server cannot produce it.", "",
          "| # | call | why |", "|---|---|---|"]
    for k in range(1, len(states)):
        st, prev = states[k], states[k-1]
        now = st["now"]
        nm = names[k]
        lines.append(json.dumps({"tag": "trace", "event": {
            "name": nm, "raw": acts[k-1], "now": now, "state": canon(st, now)}}))
        dead = [i for i in st["promises"]
                if st["promises"][i]["exists"] and live(st, i, now) not in ("none", "pending")
                and st["promises"][i]["state"] == "pending"]
        why = "promise is logically dead: " + ",".join(dead) if dead else ""
        md.append(f"| {k} | `{describe(nm, prev, st, now)}` | {why} |")
    # TLC's -dumpTrace json omits the VIOLATING state, so the JSON is one
    # short of the text.  The text's action list is authoritative; the final
    # call is described from its name and the last state we do have.
    final = names[len(states)] if len(names) > len(states) else None
    if final:
        last = states[-1]
        cand = [i for i in last["tasks"] if last["tasks"][i]["exists"]] or \
               [i for i in last["promises"] if last["promises"][i]["exists"]]
        tgt = f" id={cand[0]}" if cand else ""
        md.append(f"| {len(states)} | `{RPC[final]}{tgt}` | **this is the call that must be refused** |")
    md += ["", f"**Violates** `{prop}` at step {len(states)}.", "",
           "The recorded prefix is in `traces/gen-" + bug + ".ndjson`; it stops",
           "one step short because TLC's JSON dump omits the violating state.",
           "Run the full call list against a real server and record ITS trace --",
           "that is the artifact worth replaying, because its states come from",
           "the implementation rather than from the model."]
    open(f"traces/gen-{bug}.ndjson", "w").write("\n".join(lines) + "\n")
    open(f"scenarios/{bug}.md", "w").write("\n".join(md) + "\n")
    nsteps = len(states) if final else len(states)-1
    print(f"  {bug:<26} {nsteps} calls  violates {prop}")

main(*sys.argv[1:])
