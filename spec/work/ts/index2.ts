// W2 — sequential REMOTE invocation.
//
//   foo calls bar n times with ctx.rpc, awaiting each before the next.
//
// Identical to W1 except `ctx.run` becomes `ctx.rpc`, and that one word
// changes what the server does. A remote invocation carries
// `resonate:target`, so the server creates a TASK for the child and dispatches
// an `execute` message; any worker in the group may claim it. foo cannot
// continue while it waits, so it parks — `task.suspend` on the child — and is
// resumed when the child settles.
//
// W1 exercises promises. W2 exercises promises, tasks, dispatch, suspend and
// resume. Diffing the two traces is the cheapest way to see which parts of the
// protocol a local call actually uses.
//
// Usage:
//   npx tsx index2.ts <id> [n] [--via run|rpc]

import { type Context, Resonate } from "@resonatehq/sdk/async";

async function foo(ctx: Context, n: number): Promise<number> {
  report(ctx, "foo", n);
  for (let i = 0; i < n; i++) {
    await ctx.rpc("bar", i);
  }
  return n;
}

async function bar(ctx: Context, i: number): Promise<number> {
  report(ctx, "bar", i);
  return i;
}

/**
 * The four ids every invocation carries. See index1.ts — the shapes are the
 * same, which is the point: `run` and `rpc` differ in dispatch, not in
 * identity.
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
  console.error("usage: tsx index2.ts <id> [n] [--via run|rpc]");
  process.exit(1);
}
const n = Number(positional[1] ?? 3);
const via = flag("via", "run");

const resonate = new Resonate({ url: process.env.RESONATE_URL ?? "http://localhost:8001" });
resonate.register("foo", foo);
resonate.register("bar", bar);

console.log(`W2  ctx.rpc   id=${id}  n=${n}  via=resonate.${via}\n`);

const handle = via === "rpc"
  ? await resonate.rpc(id, "foo", n)
  : await resonate.run(id, "foo", n);

console.log(`\nresult          = ${await handle.result()}`);

const fetched = await resonate.get(id);
console.log(`resonate.get    = ${await fetched.result()}`);

await resonate.stop();
