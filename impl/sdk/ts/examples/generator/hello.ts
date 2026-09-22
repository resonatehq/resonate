// hello is a minimal example of using the Resonate SDK's generator engine.
// It registers a function, invokes it durably, and prints the result.
//
// Start a Resonate server on localhost:8001 first (`resonate dev`), then:
//
//   npx tsx examples/generator/hello.ts
//
// (Drop the `url` below to run fully in-process against the local network.)

import { type Context, Resonate } from "../../src/index.js";

// foo runs work locally via ctx.run (same worker), which itself dispatches
// baz remotely via ctx.rpc. Both are durable checkpoints: if this process dies
// mid-call, Resonate replays/recovers from where it left off.
function* foo(ctx: Context, name: string): Generator<any, string, any> {
  return yield* ctx.run(bar, name);
}

function* bar(ctx: Context, name: string): Generator<any, string, any> {
  // ctx.rpc dispatches by name; the callee may run on any worker in the group.
  return yield* ctx.rpc<string>("baz", name);
}

// baz is a leaf: it does its work and returns without any ctx.* durable op.
async function baz(_ctx: Context, name: string): Promise<string> {
  return `hello, ${name}!`;
}

const resonate = new Resonate({ url: process.env.RESONATE_URL ?? "http://localhost:8001" });
resonate.register("foo", foo);
resonate.register("baz", baz);

const id = `hello-${Date.now()}`;
console.log(await resonate.run(id, foo, "world"));

await resonate.stop();
