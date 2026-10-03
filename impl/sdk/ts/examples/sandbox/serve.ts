// serve: run registered async functions as a sandbox guest.
//
// Inside a sandbox the worker does not poll the server and holds no
// connection to it. rn8, the image's entrypoint, starts this process with
// RESONATE_URL pointing at rn8's loopback relay and PORT where it will push
// the task, then pushes it: one `execute` message, one HTTP request. This
// answers that request when the step is over — the function completed or
// suspended — which is rn8's signal to exit.
//
// The same shape as @resonatehq/gcp's handler, with two differences: a real
// heartbeat, because the sandbox plugin destroys a guest whose lease lapses;
// and no anycast address of its own — a sandbox is not reachable, so a target
// that is not a URL names a group, as it would from any other worker.

import { randomUUID } from "node:crypto";
import http from "node:http";
import { type AnyFunc, Core } from "../../src/async/index.js";
import {
  AsyncHeartbeat,
  Codec,
  ConsoleLogger,
  HttpNetwork,
  NoopEncryptor,
  OptionsBuilder,
  Registry,
  WallClock,
} from "../../src/index.js";
import type { Func } from "../../src/types.js";
import { isUrl } from "../../src/util.js";

export function serve(
  funcs: Record<string, AnyFunc>,
  { port = Number(process.env.PORT ?? 8080), ttl = 60_000 }: { port?: number; ttl?: number } = {},
): http.Server {
  const logger = new ConsoleLogger("warn");
  const registry = new Registry();
  for (const [name, func] of Object.entries(funcs)) {
    registry.add(func as unknown as Func, name);
  }

  const server = http.createServer(async (req, res) => {
    const chunks: Buffer[] = [];
    for await (const chunk of req) chunks.push(chunk as Buffer);

    // The relay, wherever the message says the server is: rn8 sets both.
    const network = new HttpNetwork({ url: process.env.RESONATE_URL, logger });
    const pid = randomUUID().replace(/-/g, "");
    const heartbeat = new AsyncHeartbeat(pid, ttl / 3, network.send, logger);
    const core = new Core({
      pid,
      ttl,
      clock: new WallClock(),
      send: network.send,
      codec: new Codec(new NoopEncryptor()),
      registry,
      heartbeat,
      dependencies: new Map(),
      optsBuilder: new OptionsBuilder({
        match: (target: string) => (isUrl(target) ? target : `poll://any@${target}`),
      }),
      logger,
    });

    try {
      const status = await core.onMessage(JSON.parse(Buffer.concat(chunks).toString("utf8")));
      res.writeHead(200, { "content-type": "application/json" });
      res.end(JSON.stringify({ status: status?.kind === "done" ? "completed" : "suspended" }));
    } catch (error) {
      res.writeHead(500, { "content-type": "application/json" });
      res.end(JSON.stringify({ error: String(error) }));
    } finally {
      heartbeat.stop();
    }
  });
  server.listen(port, "127.0.0.1");
  return server;
}
