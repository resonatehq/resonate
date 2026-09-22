# Canonical workloads

This directory holds the TypeScript canonical workloads (`index1.ts`–`index3.ts`,
below). [`go/`](go/README.md) is a separate suite: the Go SDK scenario driver
that records NDJSON traces for the checkers.

Three programs. The first two differ in exactly one word, so that diffing
their traces isolates what that word costs; the third uses both words in one
tree, which separates things the first two cannot.

```
W1  index1.ts   foo calls bar n times with ctx.run    — LOCAL
W2  index2.ts   foo calls bar n times with ctx.rpc    — REMOTE
W3  index3.ts   foo recurses, alternating both by parity of n
```

Each takes the root invocation id from the command line, so a run is
reproducible and an id can be pointed at afterwards with `resonate.get`.

```
npx tsx index1.ts <id> [n] [--via run|rpc]
npx tsx index2.ts <id> [n] [--via run|rpc]
npx tsx index3.ts <id> [n] [m] [--via run|rpc]

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
