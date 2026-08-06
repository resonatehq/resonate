#!/usr/bin/env python3
"""Drive a real resonate server through many workflows and tee the traffic
as NDJSON the Lean checker can read."""
import json, os, sys, urllib.request

URL = os.environ.get("URL", "http://127.0.0.1:8001/")
OUT = os.environ.get("OUT", "/tmp/big-trace.ndjson")
N   = int(os.environ.get("N", "50"))
V   = "2026-04-01"

def post(kind, data, now=None):
    head = {"corrId": "c", "version": V}
    if now is not None:
        head["resonate:debug_time"] = now
    body = json.dumps({"kind": kind, "head": head, "data": data}).encode()
    req = urllib.request.Request(URL, body, {"content-type": "application/json"})
    try:
        with urllib.request.urlopen(req) as r:
            return json.loads(r.read())
    except urllib.error.HTTPError as e:
        # non-2xx is a legitimate protocol response, not a transport failure
        return json.loads(e.read())

events = []
def rpc(kind, now, data):
    res = post(kind, data, now)
    events.append({"kind": kind, "now": now, "req": data, "res": res})
    return res

post("debug.start", {})   # THIS is what pauses the background loops
post("debug.reset", {})

for i in range(N):
    o  = f"o{i}"
    a  = f"{o}.a"
    x  = f"{o}.x"
    t0 = 1000 + i * 100
    ext = {"resonate:external": "true", "resonate:origin": o}
    tgt = {"resonate:target": "poll://any@w1", "resonate:origin": o}

    rpc("promise.create", t0,      {"id": a, "timeoutAt": 900000, "param": {}, "tags": ext})
    rpc("promise.create", t0,      {"id": x, "timeoutAt": 900000, "param": {}, "tags": tgt})
    rpc("task.get",       t0,      {"id": x})
    rpc("task.acquire",   t0 + 10, {"id": x, "version": 0, "pid": "p0", "ttl": 60000})
    rpc("task.suspend",   t0 + 20, {"id": x, "version": 1, "actions": [
            {"kind": "promise.register_callback", "head": {},
             "data": {"awaited": a, "awaiter": x}}]})
    rpc("promise.settle", t0 + 30, {"id": a, "state": "resolved", "value": {}})
    rpc("task.get",       t0 + 40, {"id": x})
    rpc("task.acquire",   t0 + 50, {"id": x, "version": 1, "pid": "p1", "ttl": 60000})
    rpc("task.fulfill",   t0 + 60, {"id": x, "version": 2, "action": {
            "kind": "promise.settle", "head": {},
            "data": {"id": x, "state": "resolved", "value": {}}}})
    rpc("promise.get",    t0 + 70, {"id": x})
    rpc("promise.get",    t0 + 70, {"id": a})

with open(OUT, "w") as f:
    for e in events:
        f.write(json.dumps(e) + "\n")

bad = [e for e in events if e["res"]["head"]["status"] // 100 != 2]
print(f"{len(events)} events -> {OUT}   non-2xx: {len(bad)}")
for e in bad[:5]:
    print("   ", e["kind"], e["res"]["head"]["status"], str(e["res"]["data"])[:90])
