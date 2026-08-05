// M2 — W2 driven by hand, with no SDK.
//
// M1 could be a straight line of seven calls because `index1.ts`'s children
// are LOCAL: nothing is ever dispatched, so nobody has to be listening. W2's
// children are REMOTE, and that changes the shape of the client completely.
// This file is what the difference costs:
//
//   * a poll connection, opened BEFORE anything is created, because dispatch
//     is at-most-once and a message with no live connection is dropped;
//   * an event loop over incoming `execute` messages, since work now arrives
//     rather than being called;
//   * replay — a resumed invocation re-runs from the top and re-creates the
//     children it already made, relying on `promise.create` being idempotent
//     by id;
//   * version tracking, because every resume bumps it.
//
// That list is not a description of this file so much as a description of
// what an SDK is. M1 shows the protocol can be driven with `fetch`; M2 shows
// where that stops being true.
//
// The sequence was captured from `index2.ts` through a logging proxy. For
// n = 2 it is, in order:
//
//   task.create   w2t      claim at version 1
//   task.fence    v1       promise.create w2t.0  (global, targeted)
//   task.acquire  w2t.0    v0 -> v1              the dispatch arrives
//   task.suspend  v1       park the root on w2t.0
//   task.fulfill  w2t.0    v1  resolved 0        -> resumes the root
//   task.acquire  w2t      v1 -> v2              RESUME BUMPS THE VERSION
//   task.fence    v2       promise.create w2t.0  (replay, idempotent)
//   task.fence    v2       promise.create w2t.1
//   task.acquire  w2t.1    v0 -> v1
//   task.suspend  v2       park the root on w2t.1
//   task.fulfill  w2t.1    v1  resolved 1
//   task.acquire  w2t      v2 -> v3
//   task.fence    v3       replay both children
//   task.fulfill  w2t      v3  resolved 2
//
// The root ends at version 1 + n. That is the clearest single number in the
// trace: a resume is not a continuation, it is a re-claim.
//
// Usage:
//   npx tsx manual2.ts <id> [n]

const URL_ = process.env.RESONATE_URL ?? "http://localhost:8001";
const VERSION = "2026-04-01";
const GROUP = "default";

let seq = 0;
const corr = () => `manual-${++seq}`;
const enc = (v: unknown) => Buffer.from(JSON.stringify(v)).toString("base64");
const dec = (s: string) => JSON.parse(Buffer.from(s, "base64").toString());

interface Res { kind: string; head: { corrId: string; status: number }; data: any }

async function call(kind: string, data: any, quiet = false): Promise<Res> {
  const res = await fetch(URL_, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ kind, head: { corrId: corr(), version: VERSION }, data }),
  });
  const out = (await res.json()) as Res;
  if (!quiet) {
    const id = data.id ?? data.action?.data?.id ?? "";
    console.log(`  ${kind.padEnd(16)} ${String(id).padEnd(12)} -> ${out.head.status}`);
  }
  return out;
}

const envelope = (kind: string, data: any) =>
  ({ kind, head: { corrId: corr(), version: VERSION }, data });

// --- the poll connection ------------------------------------------------
//
// Hand-rolled SSE so this file depends on nothing. `execute` messages arrive
// as `data: {...}` frames; anything starting with `:` is a comment.

interface Execute { id: string; version: number }

class Dispatches {
  private queue: Execute[] = [];
  private waiters: Array<(e: Execute) => void> = [];
  private done = false;

  push(e: Execute): void {
    const w = this.waiters.shift();
    if (w) w(e); else this.queue.push(e);
  }
  stop(): void { this.done = true; }

  async next(timeoutMs: number): Promise<Execute | null> {
    if (this.queue.length) return this.queue.shift()!;
    if (this.done) return null;
    return await new Promise<Execute | null>((resolve) => {
      const t = setTimeout(() => resolve(null), timeoutMs);
      this.waiters.push((e) => { clearTimeout(t); resolve(e); });
    });
  }
}

/** Opens the stream and resolves once the server has acknowledged it. */
async function connect(pid: string, sink: Dispatches): Promise<void> {
  const res = await fetch(`${URL_}/poll/${GROUP}/${pid}`, {
    headers: { accept: "text/event-stream" },
  });
  if (!res.body) throw new Error("no stream");
  const reader = res.body.getReader();
  const decoder = new TextDecoder();
  let buf = "";
  let opened: () => void;
  const ready = new Promise<void>((r) => { opened = r; });

  (async () => {
    for (;;) {
      const { done, value } = await reader.read();
      if (done) { sink.stop(); return; }
      buf += decoder.decode(value, { stream: true });
      let nl: number;
      while ((nl = buf.indexOf("\n\n")) !== -1) {
        const frame = buf.slice(0, nl);
        buf = buf.slice(nl + 2);
        opened!();                                   // ": connected" counts
        for (const line of frame.split("\n")) {
          if (!line.startsWith("data: ")) continue;
          try {
            const msg = JSON.parse(line.slice(6));
            if (msg.kind === "execute") sink.push(msg.data.task as Execute);
          } catch { /* not for us */ }
        }
      }
    }
  })();

  await ready;
}

// --- the driver ---------------------------------------------------------

const id = process.argv[2];
if (!id) {
  console.error("usage: tsx manual2.ts <id> [n]");
  process.exit(1);
}
const n = Number(process.argv[3] ?? 3);

const pid = `manual-${Math.abs(Date.now() % 1e9)}`;
const anycast = `poll://any@${GROUP}/${pid}`;
const timeoutAt = Date.now() + 60_000;
const ttl = 60_000;

const tags = (extra: Record<string, string>) => ({
  "resonate:origin": id,
  "resonate:prefix": id,
  "resonate:branch": id,
  "resonate:parent": id,
  ...extra,
});

console.log(`M2  manual  id=${id}  n=${n}  pid=${pid}\n`);

const dispatches = new Dispatches();
// FIRST: be reachable. Dispatch is at-most-once, so a child created before
// the stream is open would have its `execute` dropped and the run would hang
// until the task retry timer fired.
await connect(pid, dispatches);
console.log("  poll connection open\n");

// The root. task.create claims it, and claiming means no dispatch — so the
// driver has to run the root itself rather than wait to be told to.
const created = await call("task.create", {
  pid, ttl,
  action: envelope("promise.create", {
    id, timeoutAt,
    param: { headers: {}, data: enc({ func: "foo", args: [n], version: 1 }) },
    tags: tags({ "resonate:scope": "global", "resonate:target": anycast }),
  }),
});
let pending: Execute | null = { id, version: created.data.task.version };

// task.create already CLAIMED the root, so the first turn must not acquire it
// again — that would be a 409, the task being `acquired` rather than
// `pending`. The task record task.create handed back is the claim.
let held: Res | null = created;

let turns = 0;
let rootValue: number | undefined;

while (pending) {
  const msg: Execute = pending;
  turns++;
  console.log(`turn ${turns}: ${msg.id} @ v${msg.version}`);

  const acq = held ?? await call("task.acquire",
    { id: msg.id, version: msg.version, pid, ttl });
  held = null;
  if (acq.head.status === 200) {
    const version = acq.data.task.version;
    const param = dec(acq.data.promise.param.data);

    if (param.func === "bar") {
      // A leaf. Nothing to await, so settle it and let the parent resume.
      await call("task.fulfill", {
        id: msg.id, version,
        action: envelope("promise.settle", {
          id: msg.id, state: "resolved",
          value: { headers: {}, data: enc(param.args[0]) },
        }),
      });
    } else {
      // foo: replay from the top. Re-create every child made so far — creates
      // are idempotent by id, so this is cheap and is what makes replay safe —
      // and park on the first one that has not settled.
      const total: number = param.args[0];
      let parked = false;
      for (let i = 0; i < total; i++) {
        const child = `${msg.id}.${i}`;
        await call("task.fence", {
          id: msg.id, version,
          action: envelope("promise.create", {
            id: child, timeoutAt,
            param: { headers: {}, data: enc({ func: "bar", args: [i], version: 1 }) },
            // REMOTE: a target, so the server makes a task and dispatches it.
            tags: tags({ "resonate:scope": "global", "resonate:target": anycast }),
          }),
        });
        const got = await call("promise.get", { id: child }, true);
        if (got.data.promise.state === "pending") {
          await call("task.suspend", {
            id: msg.id, version,
            actions: [envelope("promise.register_callback",
              { awaiter: msg.id, awaited: child })],
          });
          parked = true;
          break;
        }
      }
      if (!parked) {
        await call("task.fulfill", {
          id: msg.id, version,
          action: envelope("promise.settle", {
            id: msg.id, state: "resolved", value: { headers: {}, data: enc(total) },
          }),
        });
        rootValue = total;
      }
    }
  }
  // 409 is normal and expected: a duplicate or superseded dispatch.

  console.log();
  if (rootValue !== undefined) break;
  pending = await dispatches.next(15_000);
}

if (rootValue === undefined) {
  console.error("gave up waiting for a dispatch");
  process.exit(1);
}

const got = await call("promise.get", { id });
const value = dec(got.data.promise.value.data);
const rootTask = await call("task.get", { id });

console.log(`\nresult          = ${value}`);
console.log(`expected        = ${n}`);
console.log(`turns           = ${turns}`);
console.log(`root version    = ${rootTask.data.task.version}   (expected ${1 + n})`);

if (value !== n) {
  console.error(`MISMATCH: got ${value}, expected ${n}`);
  process.exit(1);
}
process.exit(0);
