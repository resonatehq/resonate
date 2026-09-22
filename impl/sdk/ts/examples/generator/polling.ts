// polling shows non-blocking progress tracking over many concurrent durable
// workflows using ResonateHandle.done().
//
// `resonate.beginRun` returns a handle immediately -- the durable invocation
// runs in the background. That makes it trivial to fan several workflows out at
// once: hand each its own id, collect the handles, and you have N independent
// executions in flight.
//
// `handle.done()` is the non-blocking observation: it answers "settled yet?"
// per handle without committing to wait on any one of them, so you can scan
// every handle on every tick and decide what to render or whom to harvest.
// (In the TS SDK `done()` returns a Promise<boolean>.)
//
// Start a Resonate server on localhost:8001 first (`resonate dev`), then:
//
//   npx tsx examples/generator/polling.ts

import { type Context, Resonate } from "../../src/index.js";

// -- Leaf steps (each prints once, settles once) --------------------------

async function shade(ctx: Context, frame: string, ms: number): Promise<string> {
  // Pretend this is GPU work. ctx.sleep stands in for I/O or compute that takes
  // a while -- different frames take different wall time, so handles settle out
  // of order.
  await new Promise((r) => setTimeout(r, ms));
  console.log(`  [shade]    frame=${frame} ms=${ms}`);
  return `shaded-${frame}`;
}

async function encode(_ctx: Context, shaded: string): Promise<string> {
  return shaded.replace("shaded-", "encoded-");
}

// -- Orchestrator ---------------------------------------------------------

function* renderFrame(ctx: Context, frame: string, ms: number): Generator<any, string, any> {
  const shaded: string = yield* ctx.run(shade, frame, ms);
  return yield* ctx.run(encode, shaded);
}

const resonate = new Resonate({ url: process.env.RESONATE_URL ?? "http://localhost:8001" });
resonate.register("renderFrame", renderFrame);

try {
  // Fan out: three workflows of different weights, all dispatched up front.
  const batch = Date.now();
  const jobs: [string, number][] = [
    ["frame-1", 200],
    ["frame-2", 600],
    ["frame-3", 400],
  ];
  const handles = new Map(
    await Promise.all(
      jobs.map(
        async ([frame, ms]) =>
          [frame, await resonate.beginRun(`render-${batch}-${frame}`, renderFrame, frame, ms)] as const,
      ),
    ),
  );
  console.log(`[polling] dispatched ${handles.size} render workflows`);

  // Non-blocking progress dashboard: scan every handle each tick, stop the
  // first tick on which all are done.
  let tick = 0;
  while (true) {
    const states = await Promise.all([...handles].map(async ([frame, h]) => [frame, await h.done()] as const));
    const doneCount = states.filter(([, s]) => s).length;
    const bar = states.map(([frame, s]) => `${frame}=${s ? "✓" : "…"}`).join(" ");
    console.log(`[polling] tick=${String(tick).padStart(2)}  ${doneCount}/${handles.size}  ${bar}`);
    if (doneCount === handles.size) break;
    await new Promise((r) => setTimeout(r, 250));
    tick++;
  }

  // Every handle is done: harvest the results.
  for (const [frame, handle] of handles) {
    const result = await handle.result();
    console.log(`[polling] ${frame} -> ${result}`);
    if (result !== `encoded-${frame}`) throw new Error(`unexpected ${frame} result: ${result}`);
  }
} finally {
  await resonate.stop();
}
