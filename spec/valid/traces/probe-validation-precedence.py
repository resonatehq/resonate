import json, urllib.request, urllib.error
URL="http://127.0.0.1:8001/"
def rpc(kind, data, now=1000):
    head={"corrId":"c","version":"2026-04-01","resonate:debug_time":now}
    body=json.dumps({"kind":kind,"head":head,"data":data}).encode()
    req=urllib.request.Request(URL, body, {"content-type":"application/json"})
    try:
        with urllib.request.urlopen(req) as r: j=json.loads(r.read())
    except urllib.error.HTTPError as e: j=json.loads(e.read())
    return j["head"]["status"], str(j.get("data"))[:60]

rpc("debug.start", {}); rpc("debug.reset", {})
G="o.ghost"
cases=[
 ("promise.settle non-settable state",     "promise.settle", {"id":G,"state":"pending","value":{}}),
 ("promise.settle rejected_timedout",      "promise.settle", {"id":G,"state":"rejected_timedout","value":{}}),
 ("register_callback awaits itself",       "promise.register_callback", {"awaited":G,"awaiter":G}),
 ("register_listener bad address",         "promise.register_listener", {"awaited":G,"address":"nonsense"}),
 ("task.suspend empty awaited list",       "task.suspend", {"id":G,"version":0,"actions":[]}),
 ("task.suspend awaits itself",            "task.suspend", {"id":G,"version":0,"actions":[
       {"kind":"promise.register_callback","head":{},"data":{"awaited":G,"awaiter":G}}]}),
 ("task.suspend duplicate awaited",        "task.suspend", {"id":G,"version":0,"actions":[
       {"kind":"promise.register_callback","head":{},"data":{"awaited":"o.a","awaiter":G}},
       {"kind":"promise.register_callback","head":{},"data":{"awaited":"o.a","awaiter":G}}]}),
 ("task.fulfill non-settable state",       "task.fulfill", {"id":G,"version":0,"action":{
       "kind":"promise.settle","head":{},"data":{"id":G,"state":"pending","value":{}}}}),
]
print(f"{'case':40s} {'spec':>5s} {'server':>7s}  verdict")
bad=0
for name,kind,data in cases:
    st,msg = rpc(kind,data)
    ok = (st==400)
    if not ok: bad+=1
    print(f"{name:40s} {400:>5d} {st:>7d}  {'ok' if ok else 'DIFFERS  '+msg}")
print()
print(f"{'--- well-formed at a missing object (expect 404) ---'}")
for name,kind,data in [
 ("promise.get",            "promise.get", {"id":G}),
 ("promise.settle resolved","promise.settle", {"id":G,"state":"resolved","value":{}}),
 ("register_listener ok addr","promise.register_listener", {"awaited":G,"address":"poll://any@w1"}),
 ("task.get",               "task.get", {"id":G}),
]:
    st,msg = rpc(kind,data)
    print(f"{name:40s} {404:>5d} {st:>7d}  {'ok' if st==404 else 'DIFFERS  '+msg}")
print()
print(f"{bad} of {len(cases)} validation cases differ from the specification")
