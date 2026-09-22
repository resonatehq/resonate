# Conformance shim: HTTP -> resonate_rpc, with built-in test/debug ops.
#
# Lets the Resonate conformance harness (https://github.com/imnaseer/resonate-conformance)
# drive a resonate-pg database as if it were a Resonate server:
#
#   1. Apply resonate.sql to any Postgres 16+ database
#   2. Run:  DATABASE_URL=postgres://... python test/conformance.py
#   3. Run: RESONATE_URL=http://localhost:8001 dotnet run all
#
# Requires: pip install 'psycopg[binary,pool]'

import json
import os
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from psycopg.types.json import Json
from psycopg_pool import ConnectionPool

DSN = os.environ.get("DATABASE_URL", "postgresql://postgres:resonate@pg:5432/postgres")
PORT = int(os.environ.get("PORT", "8001"))
pool = ConnectionPool(DSN, min_size=2, max_size=16)

RESET_SQL = """
TRUNCATE resonate.outbox, resonate.listeners, resonate.callbacks,
         resonate.task_resumes, resonate.tasks, resonate.schedules,
         resonate.promises CASCADE
"""

SNAP_SQL = """
SELECT jsonb_build_object(
  'promises', COALESCE((SELECT jsonb_agg(to_jsonb(p)) FROM resonate.promises p), '[]'::jsonb),
  'tasks',    COALESCE((SELECT jsonb_agg(to_jsonb(t)) FROM resonate.tasks t),    '[]'::jsonb),
  'callbacks',COALESCE((SELECT jsonb_agg(to_jsonb(c)) FROM resonate.callbacks c),'[]'::jsonb))
"""


def handle_debug(conn, kind, head, data):
    if kind == "debug.reset":
        conn.execute(RESET_SQL)
        body = {}
    elif kind == "debug.tick":
        t = data.get("time", head.get("resonate:debug_time"))
        if t is None:
            return {"status": 400, "data": "debug.tick needs a time"}
        conn.execute("SELECT resonate.process_timeouts(%s)", (int(t),))
        body = {"time": int(t)}
    elif kind == "debug.snap":
        body = conn.execute(SNAP_SQL).fetchone()[0]
    else:
        return {"status": 501, "data": "not implemented"}
    return {"status": 200, "data": body}


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def _read_body(self):
        if "chunked" in self.headers.get("Transfer-Encoding", "").lower():
            chunks = []
            while True:
                size = int(self.rfile.readline().split(b";")[0], 16)
                if size == 0:
                    while self.rfile.readline() not in (b"\r\n", b"\n", b""):
                        pass
                    return b"".join(chunks)
                chunks.append(self.rfile.read(size))
                self.rfile.readline()
        return self.rfile.read(int(self.headers.get("Content-Length", 0)))

    def do_POST(self):
        body = self._read_body().decode()
        try:
            env = json.loads(body)
            kind = env.get("kind", "")
            head = env.get("head") or {}
            with pool.connection() as conn:
                if kind.startswith("debug."):
                    r = handle_debug(conn, kind, head, env.get("data") or {})
                    reply = {
                        "kind": kind,
                        "head": {"corrId": head.get("corrId", ""),
                                 "status": r["status"],
                                 "version": head.get("version", "1")},
                        "data": r["data"],
                    }
                else:
                    row = conn.execute(
                        "SELECT resonate.resonate_rpc(%s::jsonb)", (Json(env),)
                    ).fetchone()
                    reply = row[0]
            out, status = json.dumps(reply).encode(), 200
        except Exception as e:
            out, status = json.dumps(
                {"kind": "error",
                 "head": {"corrId": "", "status": 500, "version": "1"},
                 "data": f"shim/db error: {e}"}).encode(), 500
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(out)))
        self.end_headers()
        self.wfile.write(out)

    def log_message(self, *args):
        pass


print(f"conformance shim listening on :{PORT}", flush=True)
ThreadingHTTPServer(("0.0.0.0", PORT), Handler).serve_forever()
