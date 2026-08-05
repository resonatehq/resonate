# What must an implementation decide?

Every Resonate implementation answers the same questions. `resonate` (Rust,
three SQL backends), `resonate-pg` (Postgres), `resonate-on-convex` (Convex)
and `resonate-on-do` (Cloudflare Durable Objects) each rediscovered them from
scratch, and the ones that were rediscovered badly are in
[`COVERAGE.md`](../alts/unified/COVERAGE.md) under their bug numbers.

This is the catalogue. It exists so the next implementation starts by
answering a list instead of by shipping a defect someone already shipped.

## The three kinds of question

The distinction matters more than any individual entry, and it is not
obvious from a blank page — it comes out of the switch table in
[`ACCEPTANCE.md`](../alts/unified/ACCEPTANCE.md), where some constants are
labelled *conformance guard* and others *latitude*:

* **Conformance (C).** Looks like a design choice, is not. There is one
  correct answer; every other answer is a defect that a named property in
  [`Unified.tla`](../alts/unified/Unified.tla) catches. Answer these by
  reading the specification, not by weighing trade-offs.
* **Latitude (L).** The machines genuinely differ and the specification
  brackets the difference. `TheSquare` places `-p`, `-m`, abstract and
  concrete in one weak-bisimilarity class precisely to say that these are
  unobservable. Pick what suits the substrate.
* **Platform (P).** Not about the protocol at all. The specification has
  nothing to say, and the answer is forced by the substrate — but the
  questions recur unchanged, and the failure modes recur with them.

A useful heuristic: if getting it wrong changes what a client can *observe*,
it is C. If it changes only which internal schedule produced the observation,
it is L. If it changes only what breaks at 3 a.m., it is P.

---

# Part C — Conformance

One right answer each. The middle column is the property that fires when you
choose otherwise; the right column is who chose otherwise.

| | Question | Correct answer | Property | Got it wrong |
|---|---|---|---|---|
| **C1** | Which promises carry a durable (armed) timeout? | External ones — tagged `resonate:external`, targeted, or timers | `ArmingIsExternalOnly` (over-arming), `ObligationsAreDischargeable` (under-arming) | `resonate` arms on target only (**under**, BUG-1); `convex` arms everything (**over**) |
| **C2** | May `promise.register_callback` attach to an internal promise? | No — `422` | `ObligationsAreDischargeable` | `resonate`, `convex` |
| **C3** | May `promise.register_listener` attach to an internal promise? | No — `422` | `NoStrandedListener` | **all three** (`pg` BUG-1) |
| **C4** | Do `task.create` / `task.halt` / `task.continue` gate on the projection? | Yes | `NoDeadDispatch`, `NoHaltOnDead` | `resonate`, `pg` (BUG-2, BUG-2b, BUG-4) |
| **C5** | Do lease expiry and dispatch gate on the projection? | Yes | `NoDeadDispatch` | `resonate`, `pg` (BUG-5) |
| **C6** | Does the resume drain skip an awaiter that is itself logically dead? | Yes | `NoDeadDispatch` | **all three** — the shared resume gap |
| **C7** | Does `task.heartbeat` gate on the projection? | Yes | `NoDeadDispatch` | `resonate` |
| **C8** | Do responses serve the projected view? | Yes, every one | `ResponsesAreProjected`, `ResponsesNeverRegress`, `Stickiness` | `pg` serves unprojected rows (BUG-2, response half) |
| **C9** | May the server dispatch a task version it has not issued? | No | `OutboxNeverAhead` | — |
| **C10** | Can two workers hold a valid claim on one task? | No | `AtMostOneValidClaim` | — |
| **C11** | May a settled promise accept a new callback or listener? | No | `SettledPromiseHasNoSubscriptions` | — |
| **C12** | Is the `debug.*` surface authenticated? | Yes | *not modelled* — protocol state only | `convex` (BUG-2) |

Two notes that cost someone a bug each.

**C1 is two properties, not one, and they are not symmetric.**
`ObligationsAreDischargeable` catches *under*-arming — an obligation recorded
against a promise whose deadline will never fire. It says nothing about
over-arming, because an over-armed machine discharges everything it accepts.
Over-arming is caught only by `ArmingIsExternalOnly`. An implementation that
checks one and not the other has checked half the question, and the two known
failures sit on opposite sides of it.

**C6 is the one every implementation missed.** All three models contained the
same gap and none probed it; it was found by mutation experiment, not by
review. If you are answering this catalogue for a new implementation, this is
the entry to be suspicious of, because the rule reads like an optimisation and
is not one.

---

# Part L — Latitude

Real choices. The specification brackets all of them.

### L1 — Read discipline: projected or materialised?

* **`-p`, projected.** A read serves the projection of a timed-out object and
  writes nothing; the timeout transition persists the fact later.
* **`-m`, materialised.** A read materialises first — fires the anticipated
  timeout at the moment of observation — then serves stored state.

Indistinguishable by construction (`Indistinguishable`, and lockstep on both
channels at the abstract level). Choose on write cost: `-p` keeps reads pure,
which matters when reads are billed or when a read path would otherwise need a
write lock. `-m` keeps stored state close to observed state, which matters when
something other than the protocol — a snapshot differ, an operator — reads the
store directly.

*Answered by:* `pg` projects. `resonate` and `convex` materialise.

### L2 — Which state components are stored, and which are derived?

The concrete machine has five components: objects, deferred, timeouts, outbox,
config. The abstract machine has fewer — no timeout components (deadlines live
on the objects), no deferred queue (awaiters stay on the settled promise,
drained by chosen-element rules). Both are valid; the refinement holds in both
directions.

So an implementation picks a point on that line:

* Store the deferred queue as its own relation (or a readiness flag on the
  callback), as the concrete machine does.
* Or derive readiness by joining the callback to the awaited promise's state,
  as the abstract machine effectively does — no flag, and readiness cannot go
  stale because it is not stored.

**The cost is stated invariants, not correctness.** `SettledPromiseHasNoSubscriptions`
is written against the concrete shape: a settled promise holds no callbacks.
Derive readiness and callbacks *do* persist on settled promises — that is
precisely where the readiness signal lives — so the invariant must be restated
under `alpha` before it can be checked. Decide this deliberately, and record
which shape your conformance harness assumes.

### L3 — Where do timers live?

A separate timer relation keyed by object id, or fields on the object itself.
`TaskHasAtMostOneTimer` tells you the shape either way: a task's timer kind is
a *function of its state* — `pending ⇒ retry`, `acquired ⇒ lease`,
`suspended | halted | fulfilled ⇒ none`. An implementation storing a timer
kind alongside the state is storing something it can compute, and inviting the
two to disagree.

### L4 — Is there a delivery stage between dispatch and acquire?

A modelled worker layer — the message sits delivered-but-unclaimed — or
direct dispatch to acquisition. Only `resonate` has one.

### L5 — Who chooses the lease TTL?

Server-configured, or presented per-task by the acquiring worker. The
specification carries the TTL on the task, which permits both; every model
configuration to date fixes a single value.

### L6 — What happens to the unspecified handlers?

`promise.search`, `task.search` and `schedule.search` are `501` in the
specification. An implementation may serve them, but then it is defining
protocol, and nothing checks the definition. Note it explicitly rather than
letting it arrive as a feature.

### L7 — In what order do internal transitions fire?

Whether the promise-timeout driver drains before the task-timeout driver is a
scheduling choice — but `SequencedDriver` exists as a switch because `pg`'s
ordering is observable in combination with an ungated guard. Latitude only
once C4–C6 are answered correctly.

---

# Part P — Platform

The specification is silent. The questions are not optional.

### P1 — What is the unit of serialisation?

One global writer, one per execution tree, one per object, or a fixed shard
set. Everything downstream follows from this.

*Options and consequences:* a single writer makes every handler trivially
atomic and caps throughput at one thread. Per-tree matches the await topology
— parent awaits child is intra-tree — so most operations stay local. Per-object
maximises concurrency and turns every await into a distributed operation.

### P2 — How does a request find its partition?

Derivable from the request (a prefix of the promise id, a tag), or a lookup.
A derivable key needs no index and no registry; it also means a routing bug
writes plausible state into the wrong partition rather than failing. If the
key is derivable, consider storing it in the partition and asserting it.

### P3 — How are the multi-object write sets realised?

Settlement's write set is `{p.id}` by design. Exactly two handlers exceed one
object: `promise.register_callback` (reads awaited and awaiter, writes awaited)
and `task.suspend` (reads task and N awaited, writes task and a callback on
each). `task.fence` composes a guard on one object with a write to another.
`task.heartbeat` takes a list of task refs that need not share a partition.

Enumerate these four against your P1 answer before writing any code. They are
the entire cross-partition surface, and `task.heartbeat` is the one that
surprises people, because it is the only handler whose fan-out is a property of
the *worker* rather than the workflow.

### P4 — What happens when an operation spans partitions?

Reject it, co-locate to avoid it, do it optimistically and let a re-checking
drain absorb the staleness, or run a real two-phase protocol. Rejecting is a
protocol profile and must be documented as one. Optimism is defensible where
the write is idempotent and the drain re-checks — but not for `task.fence`,
where the whole point is that guard and write are atomic.

### P5 — What fires the internal transitions, and what repairs a missed firing?

A polling sweeper, a per-partition timer, an external scheduler, or nothing at
all (transitions happen only on touch).

The second half of the question is the one that gets skipped. A global sweeper
forgives every bookkeeping error: miss an arm and the next sweep catches it. A
per-partition timer does not — a partition that fails to re-arm is silently
dead, and if there is no registry of partitions, nothing can even observe that
it is missing. If you answer this with a per-partition timer, say what your
recovery story is, and consider a bounded re-arm ceiling so the failure mode is
delay rather than permanence.

Every timeout transition re-checks its own due time — *an armed timer means
not before* — so late, early and duplicate firings are all safe. That is a
property of the machine, and it is what makes an unreliable timer source
acceptable in the first place.

### P6 — How is work per firing bounded?

The set of simultaneously-due transitions in one partition is unbounded. If
the substrate caps execution time, the driver processes a batch and re-arms to
continue. A batch size inherited from a polling implementation means something
different here — it is a time budget, not a contention control — and will be
tuned wrongly by anyone who does not know that.

### P7 — What are the delivery semantics, per message kind?

`execute` and `unblock` do not need the same guarantee, and most
implementations give them the same one by accident.

A dropped `execute` is recovered by the task retry timer, so at-most-once
delivery is sound. A dropped `unblock` has **nothing** behind it — the listener
simply never hears. Decide whether that is acceptable; if it is not,
`unblock` needs delete-after-acknowledge and the listener needs to tolerate
duplicates.

### P8 — Is the outbox stored or derived, and what is its key?

Both known outbox relations are keyed, not append-only: `execute` keyed by
task id *coalesces* repeated enqueues to the latest version, and `unblock`
keyed by (promise, address) deduplicates. Losing that by modelling the outbox
as a log turns load-shedding into a backlog.

Also decide whether a message carries its payload or references it. A message
that references a promise pins that promise for as long as the message is
outstanding — which is invisible until something wants to reclaim storage.

### P9 — How do workers receive messages?

Push to an address, or a connection the server holds open. A held connection
needs a home, an eviction story, and a keepalive; addressed push needs the
worker to be reachable, which rules out the laptop development loop. Most
implementations end up needing both.

### P10 — What may be reclaimed, and what blocks reclamation?

No implementation to date deletes a settled promise. Every substrate with a
size limit eventually must. Work out now what references a promise after it
settles — outstanding outbox rows, unconsumed callbacks — because those are
the edges that make reclamation unsafe, and they are much easier to find
before there is data.

### P11 — Where does `now` come from?

Handlers are pure in `now`; there is no hidden clock. One value per operation,
threaded through. If the substrate freezes time within a turn, that matches
the machine exactly. If it does not, freeze it yourself — a handler that reads
the clock twice can observe a promise as pending and then as timed out inside
one transition.

### P12 — What is the operational surface?

Authentication (C12 — including the debug surface). Configuration: the
specification's only genuine knobs are the retry cadence and the lease TTL;
anything else in a config file describes an implementation's internals.
Time control for conformance: `debug.*` and an injectable clock are not
optional if the harness must drive the machine deterministically, and a
substrate that does not let you control the clock makes them mandatory.

### P13 — What are the limits, and which one binds first?

Throughput per partition, storage per partition, message fan-out, connection
count. Write them down next to the P1 answer, because partitioning is what
converts a global limit into a per-tenant one — and a per-tenant limit is a
support ticket, not an outage.

---

# The answer sheet

Fill one column per implementation. Entries below are from
[`ACCEPTANCE.md`](../alts/unified/ACCEPTANCE.md) and
[`COVERAGE.md`](../alts/unified/COVERAGE.md) where the switch table records
them, and from source where noted; `—` means not recorded here, not
"unanswered".

| | `resonate` | `resonate-pg` | `resonate-on-convex` |
|---|---|---|---|
| **C1** arm policy | `target` ❌ under | external ✅ | `all` ❌ over |
| **C2** callback guard | ❌ | ✅ | ❌ |
| **C3** listener guard | ❌ | ❌ | ❌ |
| **C4** promise liveness | ❌ | ❌ | ✅ |
| **C5** timeout liveness | ❌ | ❌ | ✅ |
| **C6** resume liveness | ❌ | ❌ | ❌ |
| **C7** heartbeat guard | ❌ | ✅ | ✅ |
| **C8** projected responses | — | ❌ | — |
| **C12** debug auth | — | — | ❌ |
| **L1** read discipline | `-m` | `-p` | `-m` |
| **L4** worker layer | yes | no | no |
| **L5** TTL | fixed | fixed | fixed |
| **L7** driver ordering | — | sequenced | — |

Rows C1–C7 and L1/L4/L5 are read off the profile definitions in
`ACCEPTANCE.md`, where a profile is the set of guards switched **off**; C8 and
C12 are marked `—` where no profile or bug record establishes them either way.

A new implementation adds a column. An implementation that cannot fill a row
has found the question it was going to answer by accident.

## Using this

1. Answer Part C from the specification. Do not weigh trade-offs; there are
   none. Every wrong answer here is already a recorded bug in something.
2. Answer Part L on the substrate's terms, and write down which shape your
   conformance harness assumes — L2 in particular changes what a structural
   comparison will say.
3. Answer Part P before writing code, especially P1, P3 and P5. P1 constrains
   everything; P3 is the whole cross-partition surface; P5 is the one whose
   failure mode is silent.
4. Add the column. The catalogue is only worth maintaining if it accumulates.
