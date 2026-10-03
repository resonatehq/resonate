#!/usr/bin/env node
// A host for the wasm build of the server: Node's event loop, `fetch` and
// `http`, wired to the module's five imports.
//
//   node wasm/host.mjs serve --store s3 --endpoint http://127.0.0.1:9000 --bucket resonate
//   node wasm/host.mjs serve --store memory --debug --port 8021
//
// The module owns the protocol, the store logic and its own timers; this file
// owns everything with a socket in it. See `src/wasm.zig` for the contract.
//
// The same imports work in a browser or a worker runtime: replace the HTTP
// server with whatever delivers requests there, and keep the rest.

import { readFile } from "node:fs/promises";
import http from "node:http";

const usage = `Usage: host.mjs serve [options]

  --store <memory|s3>     where the state lives               [default: memory]
  --endpoint <url>        the S3 endpoint                     [default: http://127.0.0.1:9000]
  --bucket <name>         the bucket                          [default: resonate]
  --prefix <p>            a key prefix inside the bucket      [default: none]
  --bind <addr>           address to listen on                [default: 0.0.0.0]
  --port <n>              port to listen on                   [default: 8001]
  --server-url <url>      the URL workers answer              [default: http://<bind>:<port>]
  --debug                 the clock belongs to the caller
  --wasm <file>           the module                          [default: zig-out/bin/resonate.wasm]
`;

function parseArgs(argv) {
  if (argv[0] !== "serve") {
    process.stderr.write(usage);
    process.exit(argv[0] === "--help" || argv[0] === "-h" ? 0 : 1);
  }
  const args = {
    store: "memory",
    endpoint: "http://127.0.0.1:9000",
    bucket: "resonate",
    prefix: "",
    bind: "0.0.0.0",
    port: 8001,
    serverUrl: null,
    debug: false,
    wasm: new URL("../zig-out/bin/resonate.wasm", import.meta.url),
  };
  for (let i = 1; i < argv.length; i++) {
    const flag = argv[i];
    const value = () => {
      if (i + 1 >= argv.length) fail(`${flag} needs a value`);
      return argv[++i];
    };
    switch (flag) {
      case "--store": args.store = value(); break;
      case "--endpoint": args.endpoint = value(); break;
      case "--bucket": args.bucket = value(); break;
      case "--prefix": args.prefix = value(); break;
      case "--bind": args.bind = value(); break;
      case "--port": args.port = Number(value()); break;
      case "--server-url": args.serverUrl = value(); break;
      case "--debug": args.debug = true; break;
      case "--wasm": args.wasm = value(); break;
      default: fail(`unknown option: ${flag}`);
    }
  }
  if (args.store !== "memory" && args.store !== "s3") fail("--store is memory or s3");
  args.serverUrl ??= `http://${args.bind}:${args.port}`;
  return args;
}

function fail(message) {
  process.stderr.write(`resonate: ${message}\n\n${usage}`);
  process.exit(1);
}

const args = parseArgs(process.argv.slice(2));
const encoder = new TextEncoder();
const decoder = new TextDecoder();

let wasm; // the instance's exports, once it exists

// The module's memory can grow during any call, which detaches every view of
// the old buffer — so a view is made fresh for every read and write.
const bytes = () => new Uint8Array(wasm.memory.buffer);
const read = (ptr, len) => bytes().slice(ptr, ptr + len);
const readText = (ptr, len) => decoder.decode(read(ptr, len));

/// Copy `data` into the module, call `fn(ptr, len)`, and free it after.
function withBuffer(data, fn) {
  const buf = typeof data === "string" ? encoder.encode(data) : data;
  // Even an empty buffer is a real allocation: a pointer the module receives is
  // never null, and `alloc`/`free` round a length of zero up to one alike.
  const ptr = wasm.alloc(buf.length);
  if (ptr === 0) throw new Error("the module is out of memory");
  bytes().set(buf, ptr);
  try {
    return fn(ptr, buf.length);
  } finally {
    wasm.free(ptr, buf.length);
  }
}

// ── The imports ──────────────────────────────────────────────────────────────

/** Requests waiting on `host_respond`, by id. */
const responders = new Map();
let nextRequestId = 1;
let timer = null;

const imports = {
  env: {
    host_now_ms: () => Date.now(),

    host_set_timer(atMs) {
      if (timer !== null) clearTimeout(timer);
      timer = null;
      if (atMs < 0) return;
      timer = setTimeout(() => {
        timer = null;
        wasm.on_timer();
      }, Math.max(0, atMs - Date.now()));
    },

    host_respond(id, status, ctPtr, ctLen, bodyPtr, bodyLen) {
      const respond = responders.get(id);
      if (!respond) return;
      responders.delete(id);
      // Copied now: the buffer is the module's and may be gone on return.
      respond(status, readText(ctPtr, ctLen), read(bodyPtr, bodyLen));
    },

    host_log(ptr, len) {
      process.stderr.write(readText(ptr, len) + "\n");
    },

    host_fetch(id, mPtr, mLen, uPtr, uLen, hPtr, hLen, bPtr, bLen) {
      // Everything is copied before the first await: the module reuses these.
      const method = readText(mPtr, mLen);
      const url = readText(uPtr, uLen);
      const headers = {};
      for (const line of readText(hPtr, hLen).split("\n")) {
        const colon = line.indexOf(":");
        if (colon > 0) headers[line.slice(0, colon).trim()] = line.slice(colon + 1).trim();
      }
      const body = method === "GET" || method === "DELETE" ? undefined : read(bPtr, bLen);
      // The answer is always delivered on a later turn, never inside this call.
      fetch(url, { method, headers, body }).then(
        async (res) => {
          const payload = new Uint8Array(await res.arrayBuffer());
          let head = "";
          // The ETag first: the module keeps a bounded number of headers, and
          // that is the one the whole design rests on.
          const etag = res.headers.get("etag");
          if (etag !== null) head += `etag: ${etag}\n`;
          for (const [name, value] of res.headers) if (name !== "etag") head += `${name}: ${value}\n`;
          deliver(id, res.status, head, payload);
        },
        (err) => deliver(id, 0, "", encoder.encode(String(err?.cause?.message ?? err?.message ?? err))),
      );
    },
  },
};

function deliver(id, status, head, payload) {
  withBuffer(head, (hPtr, hLen) =>
    withBuffer(payload, (bPtr, bLen) => wasm.on_fetch(id, status, hPtr, hLen, bPtr, bLen)),
  );
}

// ── Start ────────────────────────────────────────────────────────────────────

const module = await WebAssembly.compile(await readFile(args.wasm));
wasm = (await WebAssembly.instantiate(module, imports)).exports;

const started = withBuffer(args.endpoint, (eP, eL) =>
  withBuffer(args.bucket, (bP, bL) =>
    withBuffer(args.prefix, (pP, pL) =>
      withBuffer(args.serverUrl, (uP, uL) =>
        wasm.init(eP, eL, bP, bL, pP, pL, uP, uL, (args.debug ? 1 : 0) | (args.store === "memory" ? 2 : 0)),
      ),
    ),
  ),
);
if (started !== 0) process.exit(1);

// ── The HTTP surface, as `main.zig` serves it ────────────────────────────────

const server = http.createServer((req, res) => {
  const path = req.url.split("?")[0];
  const send = (status, contentType, body) => {
    res.writeHead(status, { "content-type": contentType, "content-length": body.length });
    res.end(body);
  };
  const expect = () => {
    const id = nextRequestId++;
    responders.set(id, (status, contentType, body) => send(status, contentType, Buffer.from(body)));
    return id;
  };

  if (path === "/" && req.method === "POST") {
    const chunks = [];
    req.on("data", (c) => chunks.push(c));
    req.on("end", () => {
      const id = expect();
      withBuffer(new Uint8Array(Buffer.concat(chunks)), (ptr, len) => wasm.rpc(id, ptr, len));
    });
    return;
  }
  if (path === "/ready" && req.method === "GET") return wasm.ready(expect());
  if (path === "/metrics" && req.method === "GET") return wasm.metrics(expect());
  for (const legacy of ["/promises", "/schedules", "/tasks"]) {
    if (path === legacy || path.startsWith(legacy + "/")) {
      return send(410, "application/json", Buffer.from(
        '{"error":"This endpoint is no longer supported. Please update to the latest SDK."}'));
    }
  }
  if (path === "/") return send(405, "text/plain", Buffer.from("the protocol endpoint takes POST\n"));
  send(404, "text/plain", Buffer.from("not found\n"));
});

server.keepAliveTimeout = 60_000;
server.listen(args.port, args.bind, () => {
  process.stderr.write(`resonate: listening on ${args.bind}:${args.port} (wasm)\n`);
});

for (const signal of ["SIGINT", "SIGTERM"]) {
  process.on(signal, () => {
    process.stderr.write("resonate: stopped\n");
    process.exit(0);
  });
}
