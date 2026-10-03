"""A stand-in for an SDK worker, for rn8's and the sandbox plugin's tests.

It does what a push-mode worker does, and no more: listen on $PORT, take the
pushed task, acquire it from $RESONATE_URL, fulfill it, and answer the push
once the step is over. Along the way it tries two requests that are not its
task's to make, and reports what came back in the value it fulfills with.

WORKER_MODE=crash makes it die on the push instead. WORKER_MODE=hang makes
it acquire with a short lease and then never heartbeat, fulfill or answer, after
writing its pid to $WORKER_PIDFILE.
"""

import base64
import json
import os
import sys
import time
import urllib.error
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

URL = os.environ["RESONATE_URL"]
MODE = os.environ.get("WORKER_MODE", "ok")
seq = 0


def call(kind, data):
    global seq
    seq += 1
    body = json.dumps(
        {"kind": kind, "head": {"corrId": f"py-{seq}", "version": "2026-04-01"}, "data": data}
    ).encode()
    req = urllib.request.Request(URL, data=body, headers={"content-type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            return r.status, json.loads(r.read())
    except urllib.error.HTTPError as e:
        return e.code, json.loads(e.read())


class Push(BaseHTTPRequestHandler):
    def log_message(self, *args):
        pass

    def do_POST(self):
        msg = json.loads(self.rfile.read(int(self.headers["content-length"])))
        task = msg["data"]["task"]
        print(f"worker: executing {task['id']}", flush=True)
        print("worker: a line on stderr", file=sys.stderr, flush=True)
        if MODE == "crash":
            os._exit(3)
        if MODE == "hang":
            with open(os.environ["WORKER_PIDFILE"], "w") as f:
                f.write(str(os.getpid()))
            call("task.acquire", {"id": task["id"], "version": task["version"], "pid": "py", "ttl": 500})
            while True:
                time.sleep(60)

        acquire, acquired = call(
            "task.acquire", {"id": task["id"], "version": task["version"], "pid": "py", "ttl": 30000}
        )
        # Fulfilled at the version the acquire returned, not the message's.
        version = (acquired.get("data") or {}).get("task", {}).get("version", task["version"])
        other, _ = call("task.acquire", {"id": "someone-else", "version": 0, "pid": "py", "ttl": 30000})
        schedule, _ = call(
            "schedule.create",
            {"id": "s", "cron": "* * * * *", "promiseId": "p{{.timestamp}}", "promiseTimeout": 1000},
        )
        report = {
            "acquire": acquire,
            "other_task": other,
            "schedule": schedule,
            "server_url_rewritten": msg.get("head", {}).get("serverUrl") == URL,
        }
        value = base64.b64encode(json.dumps(report).encode()).decode()
        fulfill, _ = call(
            "task.fulfill",
            {
                "id": task["id"],
                "version": version,
                "action": {
                    "kind": "promise.settle",
                    "head": {"corrId": "settle", "version": "2026-04-01"},
                    "data": {"id": task["id"], "state": "resolved", "value": {"data": value}},
                },
            },
        )
        self.send_response(200 if fulfill == 200 else 500)
        self.send_header("content-type", "application/json")
        self.end_headers()
        self.wfile.write(b"{}")


ThreadingHTTPServer(("127.0.0.1", int(os.environ["PORT"])), Push).serve_forever()
