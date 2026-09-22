// versioning shows how to run several versions of one function side by side.
//
// The registry is keyed on (name, version), so the *same* function name can
// have multiple implementations registered at once -- the bread and butter of a
// rolling deploy, where in-flight work keeps running the version it started on
// while new work picks up the new code.
//
// The version is **explicit, never "latest"**: a durable promise records its
// version at create time, so replay resolves the *same* implementation every
// time, no matter what has been registered since.
//
// Two ways to pick the version, depending on how you dispatch:
//
//   * run(id, fn, ...)     takes a function OBJECT, so the version is whatever
//                          `fn` was registered as -- recovered by identity.
//   * rpc(id, "name", ...) dispatches by NAME string, so the version comes from
//                          options({ version }) (default 1).
//
// Start a Resonate server on localhost:8001 first (`resonate dev`), then:
//
//   npx tsx examples/async/versioning.ts

import { type Info, Resonate } from "../../src/async/index.js";

// v1: charge the amount as-is.
async function chargeV1(_info: Info, amount: number): Promise<number> {
  return amount;
}

// v2: the billing rules changed -- add a 3% processing fee.
async function chargeV2(_info: Info, amount: number): Promise<number> {
  return Math.round(amount * 1.03 * 100) / 100;
}

const resonate = new Resonate({ url: process.env.RESONATE_URL ?? "http://localhost:8001" });

// Same name "charge", two coexisting implementations.
resonate.register("charge", chargeV1, { version: 1 });
resonate.register("charge", chargeV2, { version: 2 });

const ts = Date.now();
try {
  // run() -- version is taken from the function OBJECT you hand it.
  const v1 = await (await resonate.run(`charge-run-v1-${ts}`, chargeV1, 100)).result();
  const v2 = await (await resonate.run(`charge-run-v2-${ts}`, chargeV2, 100)).result();
  console.log(`run  charge_v1(100) = ${v1}`); // 100
  console.log(`run  charge_v2(100) = ${v2}`); // 103

  // rpc() -- dispatched by NAME; version comes from options (default 1).
  const rpcV1 = await (await resonate.rpc(`charge-rpc-v1-${ts}`, "charge", 100)).result();
  const rpcV2 = await (
    await resonate.rpc(`charge-rpc-v2-${ts}`, "charge", 100, resonate.options({ version: 2 }))
  ).result();
  console.log(`rpc  charge v1 (100) = ${rpcV1}`); // 100 -- default version 1
  console.log(`rpc  charge v2 (100) = ${rpcV2}`); // 103 -- selected via options
} finally {
  await resonate.stop();
}
