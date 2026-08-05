// W3 — one function, both invocation kinds, chosen by parity.
//
//   foo(n, m):  n == 0  ->  1
//               n even  ->  sum of m LOCAL  calls to foo(n-1, m)
//               n odd   ->  sum of n REMOTE calls to foo(n-1, m)
//
// W1 and W2 each use one invocation kind throughout. W3 alternates them
// *within a single call tree*, so local and remote children interleave in one
// id space — and the tags differ level by level. That is the thing to look at:
// `resonate:branch` is the parent's branch on an even level and the child's own
// id on an odd one, so a sibling group is not a property of the tree, it is a
// property of how each edge was made.
//
// NOTE THE ASYMMETRY, which is as specified rather than a typo on our part:
// the even branch fans out `m` times, the odd branch fans out `n` times. So
// remote fan-out shrinks as the recursion descends while local fan-out stays
// fixed.
//
//   value(n, m):   value(0,m) = 1
//                  n even     = m * value(n-1, m)
//                  n odd      = n * value(n-1, m)
//
//   size(n, m):    invocations in the tree, 1 + k*size(n-1,m), k = m or n
//
// Usage:
//   npx tsx index3.ts <id> [n] [m] [--via run|rpc]

import { type Context, Resonate } from "@resonatehq/sdk/async";

async function foo(ctx: Context, n: number, m: number): Promise<number> {
  report(ctx, n, m);

  if (n === 0) return 1;

  let r = 0;
  if (n % 2 === 0) {
    for (let i = 0; i < m; i++) {
      r = r + (await ctx.run<number>("foo", n - 1, m));
    }
  } else {
    for (let i = 0; i < n; i++) {
      r = r + (await ctx.rpc<number>("foo", n - 1, m));
    }
  }
  return r;
}

/** The same recursion without durability, to check the answer. */
function value(n: number, m: number): number {
  if (n === 0) return 1;
  return (n % 2 === 0 ? m : n) * value(n - 1, m);
}

/** Invocations in the tree, for comparing against the promise count. */
function size(n: number, m: number): number {
  if (n === 0) return 1;
  return 1 + (n % 2 === 0 ? m : n) * size(n - 1, m);
}

function report(ctx: Context, n: number, m: number): void {
  const kind = n === 0 ? "leaf" : n % 2 === 0 ? "even/run" : "odd/rpc";
  console.log(
    [
      `foo(${n},${m})`.padEnd(11),
      kind.padEnd(9),
      `id=${ctx.id}`.padEnd(30),
      `parent=${ctx.parentId}`.padEnd(30),
      `branch=${ctx.branchId}`.padEnd(30),
      `origin=${ctx.originId}`.padEnd(22),
      `prefix=${ctx.prefixId}`,
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
  console.error("usage: tsx index3.ts <id> [n] [m] [--via run|rpc]");
  process.exit(1);
}
const n = Number(positional[1] ?? 4);
const m = Number(positional[2] ?? 2);
const via = flag("via", "run");

const resonate = new Resonate({ url: process.env.RESONATE_URL ?? "http://localhost:8001" });
resonate.register("foo", foo);

console.log(
  `W3  mixed  id=${id}  n=${n}  m=${m}  via=resonate.${via}` +
  `   expect value=${value(n, m)} over ${size(n, m)} invocations\n`,
);

const handle = via === "rpc"
  ? await resonate.rpc(id, "foo", n, m)
  : await resonate.run(id, "foo", n, m);

const result = await handle.result();
console.log(`\nresult          = ${result}`);
console.log(`expected        = ${value(n, m)}`);

const fetched = await resonate.get(id);
console.log(`resonate.get    = ${await fetched.result()}`);

if (result !== value(n, m)) {
  console.error(`MISMATCH: got ${result}, expected ${value(n, m)}`);
  process.exit(1);
}

await resonate.stop();
