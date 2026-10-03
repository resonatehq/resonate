#!/usr/bin/env node
// `resonate serve`, from the wasm build: `resonate.mjs` behind Node's `http`.
//
//   node wasm/host.mjs serve --store s3 --endpoint http://127.0.0.1:9000 --bucket resonate
//   node wasm/host.mjs serve --store memory --debug --port 8021
//
// Everything with a protocol in it is in `resonate.mjs`; this file only parses
// flags and moves bytes between a socket and `resonate.fetch`.

import { readFile } from "node:fs/promises";
import http from "node:http";
import { createResonate } from "./resonate.mjs";

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

const resonate = await createResonate({
  wasm: await readFile(args.wasm),
  store: args.store,
  endpoint: args.endpoint,
  bucket: args.bucket,
  prefix: args.prefix,
  serverUrl: args.serverUrl,
  debug: args.debug,
}).catch((e) => fail(e.message));

const server = http.createServer(async (req, res) => {
  // A caller that hangs up mid-request is the caller's problem, not a reason
  // for the process to die of an unhandled rejection.
  try {
    const chunks = [];
    for await (const chunk of req) chunks.push(chunk);
    const request = new Request(`http://${req.headers.host ?? "localhost"}${req.url}`, {
      method: req.method,
      body: req.method === "GET" || req.method === "HEAD" ? undefined : Buffer.concat(chunks),
    });
    const response = await resonate.fetch(request);
    const body = Buffer.from(await response.arrayBuffer());
    res.writeHead(response.status, {
      "content-type": response.headers.get("content-type"),
      "content-length": body.length,
    });
    res.end(body);
  } catch (e) {
    process.stderr.write(`resonate: request failed: ${e?.message ?? e}\n`);
    if (!res.headersSent) res.writeHead(500, { "content-type": "text/plain" });
    res.end();
  }
});

server.keepAliveTimeout = 60_000;
server.listen(args.port, args.bind, () => {
  process.stderr.write(`resonate: listening on ${args.bind}:${args.port} (wasm)\n`);
});

for (const signal of ["SIGINT", "SIGTERM"]) {
  process.on(signal, () => {
    resonate.close();
    process.stderr.write("resonate: stopped\n");
    process.exit(0);
  });
}
