#!/usr/bin/env python3
"""A round-robin front for several Resonate servers sharing one bucket.

The whole design claims two servers over one bucket need nothing but a
conditional write to agree: no log, no lease, no lock, no coordination. The
simulator exercises that claim in one process, driving several server state
machines over one in-memory store. Nothing exercised it over real sockets, where
each server has its own document cache, its own deadline queue and its own event
loop, and the only thing joining them is the object store.

This sits in front of them and sends each request to the next one in turn, so a
recorded history is a history of the *system* rather than of a server. It is a
test fixture, not a load balancer: one thread, one request at a time per
connection, and no attempt at anything a real proxy would do.

`debug.reset` goes to every server rather than to the next one, because each
holds its own cache and resetting one would leave the others remembering a
bucket that no longer exists.

Usage: two-servers-one-bucket.py --port 8141 --to 8142 --to 8143
"""

import http.client
import json
import sys
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from itertools import cycle


class Front(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, *_args):
        pass

    def _send(self, port, body):
        c = http.client.HTTPConnection("127.0.0.1", port, timeout=30)
        try:
            c.request("POST", "/", body, {"Content-Type": "application/json"})
            r = c.getresponse()
            return r.status, r.read()
        finally:
            c.close()

    def do_GET(self):
        status, payload = 200, b"ready\n"
        try:
            c = http.client.HTTPConnection("127.0.0.1", self.server.ports[0], timeout=10)
            c.request("GET", self.path)
            r = c.getresponse()
            status, payload = r.status, r.read()
            c.close()
        except OSError as e:
            status, payload = 503, str(e).encode()
        self.send_response(status)
        self.send_header("Content-Type", "text/plain")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def do_POST(self):
        body = self.rfile.read(int(self.headers.get("Content-Length", 0)))
        broadcast = False
        try:
            broadcast = json.loads(body).get("kind") == "debug.reset"
        except ValueError:
            pass
        if broadcast:
            for port in self.server.ports:
                status, payload = self._send(port, body)
        else:
            with self.server.lock:
                port = next(self.server.turn)
            status, payload = self._send(port, body)
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)


def main(argv):
    port, ports = 8141, []
    i = 1
    while i < len(argv) - 1:
        if argv[i] == "--port":
            port = int(argv[i + 1])
        elif argv[i] == "--to":
            ports.append(int(argv[i + 1]))
        i += 2
    if not ports:
        raise SystemExit("give at least one --to <port>")
    import threading

    server = ThreadingHTTPServer(("127.0.0.1", port), Front)
    server.ports = ports
    server.turn = cycle(ports)
    server.lock = threading.Lock()
    print(f"spreading requests across {ports}", flush=True)
    server.serve_forever()


if __name__ == "__main__":
    main(sys.argv)
