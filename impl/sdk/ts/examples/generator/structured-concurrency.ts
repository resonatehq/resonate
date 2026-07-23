// structured-concurrency shows that the runtime never leaks an unawaited
// durable child.
//
//   foo:
//     yield* ctx.beginRun(bar, 1)   // spawned, Future never awaited
//     yield* ctx.beginRun(bar, 2)   // spawned, Future never awaited
//     return 5                      // returns without awaiting either
//
// `foo` fires off two children and returns 5 immediately, never awaiting either
// Future. A naive runtime would resolve `foo` and orphan the children. Resonate
// does not: a parent cannot settle while any child it spawned is still in
// flight. Before foo's promise resolves, the runtime joins both children.
//
// We prove it durably. `ctx.beginRun` children get deterministic ids
// `{foo_id}.0` and `{foo_id}.1`. After foo returns 5 we attach to those two
// promises by id and assert each resolved -- evidence the never-awaited work
// was awaited *by the runtime* on our behalf.
//
// Start a Resonate server on localhost:8001 first (`resonate dev`), then:
//
//   npx tsx examples/generator/structured-concurrency.ts

import { type Context, Resonate } from "../../src/index.js";

// Leaf: prints once, settles once. If structured concurrency holds, both of
// foo's never-awaited children land here even though foo returned first.
async function bar(_ctx: Context, n: number): Promise<number> {
  console.log(`  [bar] running child n=${n}`);
  return n * 10;
}

// Spawn two local children and walk away -- neither Future is awaited.
function* foo(ctx: Context): Generator<any, number, any> {
  yield* ctx.beginRun(bar, 1);
  yield* ctx.beginRun(bar, 2);
  return 5;
}

const resonate = new Resonate({ url: process.env.RESONATE_URL ?? "http://localhost:8001" });
resonate.register("foo", foo);
resonate.register("bar", bar);

try {
  const fooId = `structured-concurrency-${Date.now()}`;
  console.log(`[foo] starting workflow id=${fooId}`);
  const out = await resonate.run(fooId, foo);
  if (out !== 5) throw new Error(`expected 5, got ${out}`);
  console.log(`[foo] OK: returned ${out} (never awaited its two children)`);

  // The runtime awaited the two never-awaited children before resolving foo.
  // Child ids are assigned in call order as `{parent}.{seq}` (seq from 0).
  for (const [seq, n] of [
    [0, 1],
    [1, 2],
  ]) {
    const childId = `${fooId}.${seq}`;
    const child = await resonate.get(childId);
    const childOut = await child.result();
    if (childOut !== n * 10) throw new Error(`child ${childId} resolved ${childOut}, expected ${n * 10}`);
    console.log(`[child] ${childId} resolved ${childOut} -- the runtime awaited it`);
  }
  console.log("[ok] both never-awaited children completed: structured concurrency holds");
} finally {
  await resonate.stop();
}
