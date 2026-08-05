// M1 — W1 driven by hand, with no SDK.
//
// This is `index1.ts` with the SDK removed: the same promises, the same ids,
// the same tags, the same result — produced by seven raw protocol calls and a
// `fetch`. It exists to show what an SDK actually *is* over this protocol, and
// to give the specification a client that has no library between it and the
// wire.
//
// The call sequence is not invented; it was captured from `index1.ts` through
// a logging proxy and reproduced:
//
//   1  task.create                              root promise + claim it
//   2  task.fence  ( promise.create <id>.0 )    the first child
//   3  task.fence  ( promise.settle <id>.0 )    ...and its result
//   4  task.fence  ( promise.create <id>.1 )
//   5  task.fence  ( promise.settle <id>.1 )
//   .. once per iteration of the loop ..
//   6  task.fulfill( promise.settle <id>   )    the root's own result
//   7  promise.get                              read it back
//
// Two things make this possible at all, and both are worth stating:
//
//   * The children are LOCAL (`resonate:scope: local`, no `resonate:target`).
//     The server creates no task for them and dispatches nothing, so nobody
//     has to be listening. A driver for `index2.ts` could not be written this
//     way — it would have to hold a poll connection and service dispatches.
//
//   * Every write after the claim goes through `task.fence`, which is what
//     makes them safe: the fencing token (`version`) is checked as part of the
//     same transaction as the write. A driver that lost its lease would be
//     refused rather than silently corrupting the tree.
//
// The version stays 1 for the whole run. `task.create` acquires at version 1
// and nothing here bumps it — only a claim does.
//
// Usage:
//   npx tsx manual1.ts <id> [n]

const URL_ = process.env.RESONATE_URL ?? "http://localhost:8001";
const VERSION = "2026-04-01";

let seq = 0;
const corr = () => `manual-${++seq}`;

/** The SDK's codec: base64 of the JSON encoding. */
const enc = (v: unknown) => Buffer.from(JSON.stringify(v)).toString("base64");
const dec = (s: string) => JSON.parse(Buffer.from(s, "base64").toString());

interface Res { kind: string; head: { corrId: string; status: number }; data: any }

async function call(kind: string, data: any): Promise<Res> {
  const res = await fetch(URL_, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ kind, head: { corrId: corr(), version: VERSION }, data }),
  });
  const out = (await res.json()) as Res;
  const detail = out.head.status === 200 ? "" : `  ${JSON.stringify(out.data)}`;
  console.log(`  ${kind.padEnd(26)} -> ${out.head.status}${detail}`);
  if (out.head.status !== 200 && out.head.status !== 300) {
    throw new Error(`${kind} failed: ${out.head.status} ${JSON.stringify(out.data)}`);
  }
  return out;
}

/** An inner action is a full envelope, not a bare payload. */
const envelope = (kind: string, data: any) =>
  ({ kind, head: { corrId: corr(), version: VERSION }, data });

const id = process.argv[2];
if (!id) {
  console.error("usage: tsx manual1.ts <id> [n]");
  process.exit(1);
}
const n = Number(process.argv[3] ?? 3);

// A pid identifies this driver as a worker. The target has to be a routable
// address even though nothing will ever be dispatched to it: the root promise
// is claimed by the same call that creates it.
const pid = `manual-${Math.abs(Date.now() % 1e9)}`;
const target = `poll://any@default/${pid}`;
const timeoutAt = Date.now() + 60_000;
const ttl = 60_000;

const tags = (extra: Record<string, string>) => ({
  "resonate:origin": id,
  "resonate:prefix": id,
  "resonate:branch": id,
  "resonate:parent": id,
  ...extra,
});

console.log(`M1  manual  id=${id}  n=${n}  pid=${pid}\n`);

// 1. Create the root invocation AND claim it in one transition. The task comes
//    back already `acquired` at version 1 — that is what makes task.create
//    different from promise.create followed by task.acquire.
const created = await call("task.create", {
  pid, ttl,
  action: envelope("promise.create", {
    id, timeoutAt,
    param: { headers: {}, data: enc({ func: "foo", args: [n], version: 1 }) },
    tags: tags({ "resonate:scope": "global", "resonate:target": target }),
  }),
});
const version = created.data.task.version;
console.log(`  claimed at version ${version}\n`);

// 2..5. The loop body. Each iteration is a child created and then settled,
//       both under the fence. This is `await ctx.run(bar, i)` unrolled.
for (let i = 0; i < n; i++) {
  const child = `${id}.${i}`;
  console.log(`  iteration ${i}  (${child})`);

  await call("task.fence", {
    id, version,
    action: envelope("promise.create", {
      id: child, timeoutAt,
      param: { headers: {}, data: enc({ func: "bar", version: 1 }) },
      // LOCAL: no target, so no task and no dispatch. The driver executes the
      // body itself — here, trivially, by returning i.
      tags: tags({ "resonate:scope": "local" }),
    }),
  });

  await call("task.fence", {
    id, version,
    action: envelope("promise.settle", {
      id: child, state: "resolved", value: { headers: {}, data: enc(i) },
    }),
  });
  console.log();
}

// 6. The root's own result. Not a fence: task.fulfill settles the task's OWN
//    promise and fulfils the task in one transition.
await call("task.fulfill", {
  id, version,
  action: envelope("promise.settle", {
    id, state: "resolved", value: { headers: {}, data: enc(n) },
  }),
});

// 7. Read it back, exactly as resonate.get would.
const got = await call("promise.get", { id });
const value = dec(got.data.promise.value.data);
console.log(`\nresult          = ${value}`);
console.log(`expected        = ${n}`);

for (let i = 0; i < n; i++) {
  const c = await call("promise.get", { id: `${id}.${i}` });
  const p = c.data.promise;
  console.log(
    `  ${p.id.padEnd(18)} ${p.state.padEnd(9)} value=${dec(p.value.data)}` +
    `  scope=${p.tags["resonate:scope"]}  target=${p.tags["resonate:target"] ?? "none"}`,
  );
}

if (value !== n) {
  console.error(`MISMATCH: got ${value}, expected ${n}`);
  process.exit(1);
}
