# Canonical workloads

Two programs that differ in exactly one word, so that diffing their traces
isolates what that word costs.

```
W1  index1.ts   foo calls bar n times with ctx.run    — LOCAL
W2  index2.ts   foo calls bar n times with ctx.rpc    — REMOTE
```

Both take the root invocation id from the command line, so a run is
reproducible and an id can be pointed at afterwards with `resonate.get`.

```
npx tsx index1.ts <id> [n] [--via run|rpc]
npx tsx index2.ts <id> [n] [--via run|rpc]

RESONATE_URL=http://localhost:8001 npx tsx index1.ts demo 3
```

`--via` chooses how the **root** is started — `resonate.run` claims it in this
process, `resonate.rpc` asks the server to dispatch it — and is orthogonal to
what the body does. All four combinations are expected to produce the same
result and the same ids.

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
