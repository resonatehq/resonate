// rpc shows one worker dispatching to another by group.
//
// Two Resonate instances share a server but live in different **groups**:
//
//   * backend  registers `greet` and does the work.
//   * frontend registers nothing -- it only *dispatches*.
//
// rpc dispatches by **name**, not by a local function object, so the caller
// need not have the target registered at all. `options({ target: "backend" })`
// routes the call to the backend group's anycast address; the server hands the
// execute message to a worker subscribed there, which runs `greet` and settles
// the promise. The whole round trip crosses the durability boundary -- it is
// not an in-process call, so this example needs a running server.
//
// Start a Resonate server on localhost:8001 first (`resonate dev`), then:
//
//   npx tsx examples/generator/rpc.ts

import { type Context, Resonate } from "../../src/index.js";

// Runs on the backend worker -- the side effect lives in the leaf.
async function greet(_ctx: Context, name: string): Promise<string> {
  console.log(`backend: greeting ${name}`);
  return `hello from backend, ${name}!`;
}

const url = process.env.RESONATE_URL ?? "http://localhost:8001";

// The worker: owns `greet` and listens on the "backend" group.
const backend = new Resonate({ url, group: "backend" });
backend.register("greet", greet);

// The caller: a different group, with `greet` deliberately NOT registered.
const frontend = new Resonate({ url, group: "frontend" });

try {
  const id = `rpc-${Date.now()}`;
  // Dispatch by name + target to the backend group, then await the result.
  const result = await frontend.rpc(id, "greet", "world", frontend.options({ target: "backend" }));
  if (result !== "hello from backend, world!") throw new Error(`unexpected: ${result}`);
  console.log(`frontend: got ${JSON.stringify(result)}`);
} finally {
  await frontend.stop();
  await backend.stop();
}
