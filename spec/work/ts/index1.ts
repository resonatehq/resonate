// W1 — sequential LOCAL invocation.
//
//   foo calls bar n times with ctx.run, awaiting each before the next.
//
// ctx.run is a LOCAL function invocation: the child promise is created with
// `resonate:scope: "local"` and NO `resonate:target`, so the server creates no
// task for it and dispatches nothing. The same worker executes it inline. The
// promises still exist and are still durable — what is absent is the
// dispatch.
//
// Usage:
//   npx tsx index1.ts <id> [n] [--via run|rpc]
//
// <id> is the root invocation id, and every id below it is derived from it.
// --via chooses how the ROOT is started (resonate.run = claim it here,
// resonate.rpc = ask the server to dispatch it); the body is identical either
// way, which is the point of having both.

import { type Context, Resonate } from "@resonatehq/sdk/async";

async function foo(ctx: Context, n: number): Promise<number> {
  report(ctx, "foo", n);
  for (let i = 0; i < n; i++) {
    await ctx.run("bar", i);
  }
  return n;
}

async function bar(ctx: Context, i: number): Promise<number> {
  report(ctx, "bar", i);
  return i;
}

/**
 * The four ids every invocation carries. They are the reason these workloads
 * are canonical: the shape of this table is what any implementation has to
 * reproduce.
 *
 *   id      this invocation           `${parent.id}.${seq}`
 *   prefix  the id-generation root    set once at the top, never re-rooted
 *   origin  the lineage root          re-rooted by `detached`
 *   parent  the calling invocation
 *   branch  the sibling group
 */
function report(ctx: Context, fn: string, arg: number): void {
  console.log(
    [
      `${fn}(${arg})`.padEnd(8),
      `id=${ctx.id}`.padEnd(34),
      `prefix=${ctx.prefixId}`.padEnd(28),
      `origin=${ctx.originId}`.padEnd(28),
      `parent=${ctx.parentId}`.padEnd(34),
      `branch=${ctx.branchId}`,
    ].join(" "),
  );
}

function flag(name: string, fallback: string): string {
  const hit = process.argv.find((a) => a.startsWith(`--${name}=`));
  if (hit) return hit.slice(name.length + 3);
  const i = process.argv.indexOf(`--${name}`);
  return i >= 0 && process.argv[i + 1] ? process.argv[i + 1] : fallback;
}

const positional = process.argv.slice(2).filter((a) => !a.startsWith("--"));
const id = positional[0];
if (!id) {
  console.error("usage: tsx index1.ts <id> [n] [--via run|rpc]");
  process.exit(1);
}
const n = Number(positional[1] ?? 3);
const via = flag("via", "run");

const resonate = new Resonate({ url: process.env.RESONATE_URL ?? "http://localhost:8001" });
resonate.register("foo", foo);
resonate.register("bar", bar);

console.log(`W1  ctx.run   id=${id}  n=${n}  via=resonate.${via}\n`);

// resonate.run claims the root here; resonate.rpc asks the server to dispatch
// it — to this worker, since it is the only one in the group.
const handle = via === "rpc"
  ? await resonate.rpc(id, "foo", n)
  : await resonate.run(id, "foo", n);

console.log(`\nresult          = ${await handle.result()}`);

// resonate.get reads the same promise back by id — durable execution's whole
// point: the result outlives the invocation that produced it.
const fetched = await resonate.get(id);
console.log(`resonate.get    = ${await fetched.result()}`);

await resonate.stop();
