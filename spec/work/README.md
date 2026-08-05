# Canonical workloads

Three programs. The first two differ in exactly one word, so that diffing
their traces isolates what that word costs; the third uses both words in one
tree, which separates things the first two cannot.

```
W1  index1.ts   foo calls bar n times with ctx.run    — LOCAL
W2  index2.ts   foo calls bar n times with ctx.rpc    — REMOTE
W3  index3.ts   foo recurses, alternating both by parity of n
M1  manual1.ts  W1 again, driven by hand with no SDK
M2  manual2.ts  W2 again, driven by hand — and what that costs
```

Each takes the root invocation id from the command line, so a run is
reproducible and an id can be pointed at afterwards with `resonate.get`.

```
npx tsx index1.ts <id> [n] [--via run|rpc]
npx tsx index2.ts <id> [n] [--via run|rpc]
npx tsx index3.ts <id> [n] [m] [--via run|rpc]
npx tsx manual1.ts <id> [n]
npx tsx manual2.ts <id> [n]

RESONATE_URL=http://localhost:8001 npx tsx index1.ts demo 3
```

`--via` chooses how the **root** is started — `resonate.run` claims it in this
process, `resonate.rpc` asks the server to dispatch it — and is orthogonal to
what the body does. Both entry points are expected to produce the same result
and the same ids.

## What they show

**W1 — `ctx.run`.** The child promise carries `resonate:scope: local` and **no
target**, so the server creates no task and dispatches nothing. The same worker
executes it inline. `foo` runs exactly once.

```
foo(3)   id=demo      prefix=demo  origin=demo  parent=demo  branch=demo
bar(0)   id=demo.0    prefix=demo  origin=demo  parent=demo  branch=demo
bar(1)   id=demo.1    prefix=demo  origin=demo  parent=demo  branch=demo
bar(2)   id=demo.2    prefix=demo  origin=demo  parent=demo  branch=demo
```

**W2 — `ctx.rpc`.** The child carries a target, so the server creates a task
and dispatches an `execute`. `foo` cannot continue while it waits, so it
suspends and is resumed when the child settles — which is why `foo` appears
**n+1 times**: once per entry, plus the initial one.

```
foo(3)   id=demo      parent=demo      branch=demo
bar(0)   id=demo.0    parent=demo.0    branch=demo.0
foo(3)   id=demo      parent=demo      branch=demo      <- resumed
bar(1)   id=demo.1    parent=demo.1    branch=demo.1
foo(3)   id=demo      parent=demo      branch=demo      <- resumed
bar(2)   id=demo.2    parent=demo.2    branch=demo.2
foo(3)   id=demo      parent=demo      branch=demo      <- resumed
```

So W1 exercises promises. W2 exercises promises, tasks, dispatch, suspend and
resume. The ids are identical in both: `run` and `rpc` differ in **dispatch**,
not in **identity**.

## W3 — both kinds in one tree

```
foo(n, m):  n == 0  ->  1
            n even  ->  sum of m LOCAL  calls to foo(n-1, m)
            n odd   ->  sum of m REMOTE calls to foo(n-1, m)
```

Fan-out is `m` on both sides, so the **shape is fixed and known** — a full
m-ary tree of depth n — and the only thing varying with depth is whether the
edge was local or remote. That is what makes it a clean probe rather than two
variables at once.

```
value(n,m)  = m^n                    each level multiplies by m
size(n,m)   = 1 + m + ... + m^n      a full m-ary tree of depth n
remote(n,m) = 1 + sum of m^d over the levels reached by an rpc edge
```

`remote` is the interesting one: a task exists **iff** the promise carries a
target, so it should equal the task count exactly. Levels alternate, so it is
the root plus every other level.

Observed, against `resonate-on-do`:

| n | m | result | promises | expected | tasks | expected |
|---|---|---|---|---|---|---|
| 4 | 2 | 16 = 2⁴ | 31 | 1+2+4+8+16 | 21 | 1+4+16 |
| 3 | 3 | 27 = 3³ | 40 | 1+3+9+27 | 31 | 1+3+27 |

Every figure predicted before the run and matched after it, via both
`resonate.run` and `resonate.rpc`, with nothing left pending. **Counting
promises and counting tasks is the cheapest conformance check in this folder**
— two closed forms, and an implementation that creates a task where it should
not, or misses one, fails on the count alone.

### What only W3 shows

W1 and W2 each use one invocation kind, so they cannot separate *how an
invocation was started* from *where it sits in the tree*. W3 alternates, and
the two come apart:

```
                        Context reports            promise tag says
w3a.0    local child    parent=w3a                 resonate:parent=w3a
w3a.0.2  remote child   parent=w3a.0.2  (itself)   resonate:parent=w3a.0
```

A **locally** run child executes inline, so its `Context` inherits the running
parent's `parentId`/`branchId`. A **remotely** dispatched child is claimed by a
worker as a root computation, so its `Context` reports *itself* as parent and
branch. The promise's tags record the real edge in both cases.

So: the tags are the tree; the Context fields are the execution. An
implementation that reconstructs lineage must read the tags, and anything that
tests lineage through the client will agree with the tree only for local calls.

## M1 — the same thing without an SDK

`manual1.ts` is `index1.ts` with the library removed: same promises, same ids,
same tags, same result, produced by raw protocol calls and a `fetch`. The
sequence is not invented — it was captured from `index1.ts` through a logging
proxy and reproduced:

```
task.create                            root promise + claim, version 1
task.fence ( promise.create <id>.0 )   \
task.fence ( promise.settle <id>.0 )   |  once per iteration
task.fence ( promise.create <id>.1 )   |  of the loop
task.fence ( promise.settle <id>.1 )   /
task.fulfill( promise.settle <id>  )   the root's own result
promise.get                            read it back
```

Running both and normalising the root id, the resulting server state is
**identical** — 4 promises, 1 task, same states, same tags, same encoded
values — differing only in the random pid inside the target address.

Three things it makes concrete:

- **`task.create` claims what it creates.** The task comes back `acquired` at
  version 1 and no `execute` is dispatched, because the caller already holds
  it. That is `T-02`'s fresh path, and an implementation that reuses its
  `promise.create` path here will emit a dispatch nobody wants — carrying
  version 0, while the task is already at 1, so it is stale the moment it is
  sent. *(`resonate-on-do` did exactly that. Writing this driver is what
  surfaced it: deciding whether a hand-driven client needs a poll connection
  forces you to say what `task.create` does with the target.)*
- **The version never moves.** `task.create` acquires at 1, and nothing in the
  loop bumps it — only a claim does. Every fence in the run presents 1.
- **This can only be written for W1.** The children are local, so nothing is
  ever dispatched and the driver need not be reachable. A manual driver for W2
  would have to hold a poll connection and service `execute` messages — which
  is precisely the work the SDK's transport layer does, and precisely what
  `ctx.run` avoids.

Every write after the claim goes through `task.fence`, which is what makes a
hand-written driver safe: the fencing token is checked in the same transaction
as the write, so a driver that lost its lease is refused rather than quietly
corrupting the tree.

## M2 — the same again, remotely, and what it costs

`manual1.ts` is a straight line of calls. `manual2.ts` cannot be, and the
difference is the point of having both. W2's children are remote, so:

- a **poll connection must be open before anything is created** — dispatch is
  at-most-once, so a child created first would have its `execute` dropped;
- work **arrives** rather than being called, so there is an event loop;
- a resumed invocation **replays from the top**, re-creating children it
  already made and relying on `promise.create` being idempotent by id;
- and **every resume bumps the version**, so it has to be tracked.

That list is not a description of the file so much as a description of what an
SDK is. M1 shows the protocol can be driven with `fetch`; M2 shows where that
stops being comfortable.

The captured shape for n = 2, reproduced exactly by the driver:

```
turn 1   task.create  root          claim at v1
         task.fence   v1            promise.create root.0 (global, targeted)
         task.suspend v1            park on root.0
turn 2   task.acquire root.0  v0->v1
         task.fulfill root.0  v1    resolved 0        -> resumes the root
turn 3   task.acquire root   v1->v2   RESUME IS A RE-CLAIM
         task.fence   v2            replay root.0, create root.1
         task.suspend v2
turn 4   task.acquire root.1 v0->v1
         task.fulfill root.1 v1     resolved 1
turn 5   task.acquire root   v2->v3
         task.fence   v3            replay both
         task.fulfill root   v3     resolved 2
```

**The root ends at version `1 + n`**, and turns come to `1 + 2n`. Those are
the two numbers to check: a resume is not a continuation, it is a re-claim,
and the version says so.

### The bug this found

Running M2 against a long-lived server hung — no dispatch ever arrived. It
reproduces in three lines: open a few SSE connections, kill the clients, then
dispatch. Everything is swallowed.

The cause was in `resonate-on-do`'s gateway, and the fix is in the addressing
rather than in liveness detection, because **liveness is not detectable here**:
after a client disappears, the writer's `closed` promise does not settle,
`write()` does not throw, and `desiredSize` stays at 1 — the runtime keeps
draining the response body into a void. A dead stream is indistinguishable
from a live one.

What saves it is reading the reference more carefully. `PollRegistry::send_poll`
treats an **anycast address as a preference, not a free choice**: for
`poll://any@<group>/<pid>` it delivers to the connection whose id is `<pid>`
and only falls back to another member of the group if that one is absent.
Since a dispatch address carries the pid of the worker that asked for the
work, the preferred connection is nearly always right — and picking an
arbitrary group member instead is exactly how one abandoned connection
swallows every message for everyone.

"Anycast" reads like "pick any". It is not.

## Tags on the wire

Read back from the server with `promise.get` — this is the authoritative form,
not what the client's `Context` reports:

| tag | W1 local child | W2 remote child |
|---|---|---|
| `resonate:prefix` | `demo` | `demo` |
| `resonate:origin` | `demo` | `demo` |
| `resonate:parent` | `demo` | `demo` |
| `resonate:branch` | `demo` — the parent's | `demo.0` — **its own id** |
| `resonate:scope` | `local` | `global` |
| `resonate:target` | *absent* | `poll://any@default` |

Three things worth knowing, all of which bite an implementation:

- **`branch` differs between local and remote children.** A local child joins
  its parent's sibling group; a remote child starts its own. Anything keyed on
  `branch` (sibling preload, for instance) behaves differently across the two.
- **A dispatch target has no pid.** `resonate:target` is
  `poll://any@<group>`, while the worker's own addresses are
  `poll://<cast>@<group>/<pid>`. An address parser must accept both.
- **`ctx.parentId` and `resonate:parent` disagree for a dispatched child.**
  The tag says `demo`; the `Context` inside the dispatched invocation reports
  its own id. The tag is the durable fact; the Context field reflects that the
  invocation is a root computation on the worker that claimed it.

## Where they have been run

`@resonatehq/sdk` 0.11.4 against `resonate-on-do` (Cloudflare Durable Objects),
all four combinations at n=3. They are written against the protocol, not
against any one server: `RESONATE_URL` points them anywhere, including
`resonatehq/resonate` via `resonate dev`.
