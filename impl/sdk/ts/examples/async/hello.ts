// hello is a minimal example of using the Resonate SDK's async/await engine.
// It registers a function, invokes it durably, and prints the result.
//
// Run a Resonate server on localhost:8001 first, then:
//
//   npx tsx examples/async/hello.ts
//
// (Omit the `url` below to run fully in-process against the local network.)

import { type Info, Resonate } from "../../src/async/index.js";

// greet is a leaf: it does its work and returns without spawning children, so
// it takes `Info` (the read-only view) rather than the full `Context`.
async function greet(_info: Info, name: string): Promise<string> {
  return `hello, ${name}!`;
}

const resonate = new Resonate({ url: process.env.RESONATE_URL ?? "http://localhost:8001" });
const greetFn = resonate.register("greet", greet);

const id = `hello-${Date.now()}`;
const handle = await greetFn.run(id, "world");

console.log(await handle.result());

await resonate.stop();
