# What must an implementation decide?

Every Resonate implementation answers the same questions. `resonate` (Rust,
three SQL backends), `resonate-pg` (Postgres), `resonate-on-convex` (Convex)
and `resonate-on-do` (Cloudflare Durable Objects) each rediscovered them from
scratch, and the ones that were rediscovered badly are recorded under their
bug numbers in the [coverage record](#the-coverage-record) at the end of this
document.

This is the catalogue. It exists so the next implementation starts by
answering a list instead of by shipping a defect someone already shipped.

## The three kinds of question

The distinction matters more than any individual entry, and it is not
obvious from a blank page — it comes out of the switch table in the
[acceptance record](#the-acceptance-record) below, where some constants are
labelled *conformance guard* and others *latitude*:

* **Conformance (C).** Looks like a design choice, is not. There is one
  correct answer; every other answer is a defect that a named property in
  the unified TLA+ model caught. (The model itself, `Unified.tla`, has been
  retired from the tree; the records it produced are preserved in the
  appendix below.) Answer these by reading the specification, not by
  weighing trade-offs.
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

Fill one column per implementation. Entries below are from the
[acceptance](#the-acceptance-record) and [coverage](#the-coverage-record)
records where the switch table records them, and from source where noted;
`—` means not recorded here, not "unanswered".

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

Rows C1–C7 and L1/L4/L5 are read off the profile definitions in the
acceptance record, where a profile is the set of guards switched **off**; C8 and
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

---

# Appendix — the record behind the properties

Part C keeps citing named properties and known bugs. Both come out of one
piece of work: a unified TLA+ model (`Unified.tla`) that folded the three
implementation models into a single module whose divergences from the
specification were boolean switches. The model and its configurations have
been retired from the tree; the two records this catalogue relies on are
preserved here. The **acceptance record** is where the switch table and
every named property come from. The **coverage record** is the honest
accounting of what the model actually caught — and did not.

## The acceptance record

`Unified.tla` claimed to be strictly stronger than the three implementation
models it was derived from. That claim is only worth something if it is
falsifiable, so it was stated as a test:

> **The specification profile must clear every property. Each implementation
> profile must reproduce that implementation's known defects, as violations of
> *named* properties, without any bespoke instrumentation.**

Every implementation's divergence from the specification was a named boolean
`CONSTANT`; a profile was an assignment of those constants. Nothing else
changed between runs — one module, one `Next`, six configurations.

### The switches

| constant | `TRUE` (specification) | `FALSE` (as shipped somewhere) |
|---|---|---|
| `ArmPolicy` | **conformance guard.** `"external"` is correct; both other settings are defects | `"target"` (resonate) **under-arms**; `"all"` (convex) **over-arms** |
| `MaterialiseOnRead` | latitude: a read materialises (`-m`) or projects (`-p`) | pg projects; resonate and convex materialise |
| `WorkerLayer` | model detail: is there a delivery stage before acquire? | only resonate has one |
| `TTLs` | the lease TTLs a worker may present; a task carries its own | `{Ttl}` in every model config |
| `CallbackExternalGuard` | P-04 refuses an internal awaited (422) | accepts any awaited |
| `ListenerExternalGuard` | P-05 refuses an internal awaited (422) | accepts any awaited |
| `PromiseLivenessGuard` | T-02 claim / T-09 halt / T-10 continue gate on the projection | ungated |
| `TimeoutLivenessGuard` | R5 lease expiry / R6 dispatch gate on the projection | ungated |
| `ResumeLivenessGuard` | R4 resume skips an awaiter that is itself logically dead | resumes it anyway |
| `HeartbeatGuard` | T-05 gates on the projection | ungated |
| `SequencedDriver` | — | promise-timeout loop drains before the task-timeout loop |
| `FaultsOn` | message loss + worker crashes enabled | — |

### Results

TLC 1.8.0 (`2026.07.31`), 4 workers. Scope for every run: 2 ids, 1 listener
address, 1 worker, horizon 2, versions ≤ 2, `Retry = Ttl = 1`, faults on.

| profile | switches off | property checked | result |
|---|---|---|---|
| `MC_spec` | *none* | `Safety` (all 21) | ⏳ never completed — see below |
| `MC_server` | arm=`target`, callback, listener, promise, timeout, resume, heartbeat | `ObligationsAreDischargeable` | **violated**, 118 distinct |
| `MC_convex` | arm=`all`, callback, listener, resume | `NoDeadDispatch` | **violated**, 21 510 distinct |
| `MC_pg` | listener, promise, timeout, resume (+`SequencedDriver`) | `NoHaltOnDead` | **violated**, 1 035 distinct |
| `MC_pg_listener` | as above | `ObligationsAreDischargeable` | **violated**, 212 distinct |
| `MC_resume_gap` | **resume only** | `NoDeadDispatch` | **violated**, 26 478 distinct |
| `MC_pg_response` | + `ProjectedResponses` | `ResponsesNeverRegress` | **violated**, 5 110 distinct |
| `MC_pg_projection` | + `ProjectedResponses` | `ResponsesAreProjected` | **violated**, 799 distinct |
| `MC_pg_task_response` | listener, promise, timeout, resume | `TaskResponsesNeverRegress` | **violated**, 5 200 distinct |
| `MC_pg_task_projection` | as above | `TaskResponsesAreProjected` | **violated**, 873 distinct |
| `MC_armpolicy` | spec + `ArmPolicy = "all"` | `ArmingIsExternalOnly` | **violated**, 21 distinct |
| `MC_convex_arming` | convex profile | `ArmingIsExternalOnly` | **violated**, 21 distinct |
| `MC_liveness` | *none* | `TasksConverge` under `FairSpec` | ⏳ never completed — see below |

Distinct-state counts for a VIOLATED property are "states explored before the
violation was found", which depends on BFS scheduling and worker count — they
are not canonical. The depth at which a violation appears is the stable
figure. Counts for a property that HOLDS are exhaustive and canonical.

Every number in the table is from the final module. That matters: trace
validation forced four changes to the model after the first pass (per-task
`ttl`, clearing `ttl` when the lease ends, splitting `TouchPromise` out of
`OnPromiseTimeout`, and the `WorkerLayer` switch), so every earlier figure
was stale and was replaced rather than carried forward.

The specification profile had been clean at each earlier stage — 17
properties (2 469 914 distinct, depth 25) and 19 with the promise channel
(6 779 134 distinct, depth 27), both exhaustive with faults on. The re-run
against the final module had not finished when the model was retired, so
**the 21-property row was never claimed** — the two ⏳ rows above are open,
not passed. The earlier `TasksConverge` result (complete state space,
temporal check over 4 939 828 branches, 16m42s, zero `UNCHANGED` warnings)
should also be read narrowly: within a bounded horizon every promise
eventually times out, so the timeout rule alone discharges the "eventually".
It is evidence that deadlines converge a workflow, not that redelivery works.

Each violation maps to a finding the three model-checking runs had reported
independently:

| violation | the finding it reproduces |
|---|---|
| `MC_server` → `ObligationsAreDischargeable` | resonate BUG-1 — `c8d7c7b` gated `promise_timeouts` on `resonate:target` while leaving registration open, so a suspended task waits on a promise nothing will ever settle |
| `MC_pg_listener` → `ObligationsAreDischargeable` | resonate-pg BUG-1 — `promise_register_listener` has no `external` guard |
| `MC_convex` → `NoDeadDispatch` | resonate-on-convex BUG-1 — `triggerCallbacks` resurrects and dispatches a timed-out workflow |
| `MC_pg` → `NoHaltOnDead` | resonate-pg BUG-4 — `task.halt` returns 200 on a task `task.get` already reports `fulfilled` |
| `MC_resume_gap` → `NoDeadDispatch` | **the divergence all three models contained and none of the three probed** (see below) |
| `MC_pg_response` → `ResponsesNeverRegress` | resonate-pg BUG-2, **response half** — `task.create` serves the raw stored row, so `promise.get` answers `rejected_timedout` and a later `task.create` answers `pending`, for the same promise |

### Arming is external-only — a decision, not a discovery

`ArmPolicy` was carried for a while as possible LATITUDE, on the reasoning
that convex over-arms rather than under-guards and that an observer under the
projection discipline might not be able to tell. **That reading was wrong.**
Arming external-only is the correct behaviour; every other setting is a
defect. So the model pins arming from BOTH sides, because the two failures
are different and neither property catches the other:

| setting | who | failure | caught by |
|---|---|---|---|
| `"external"` | specification, resonate-pg | — | — |
| `"target"` | resonate @ `c8d7c7b` | **under-arms** — an external-but-untargeted promise gets no timeout, so an obligation against it can never be discharged | `ObligationsAreDischargeable` |
| `"all"` | resonate-on-convex | **over-arms** — an internal promise gets a durable timeout it should not have; its deadline is projection-only by design | `ArmingIsExternalOnly` |

Two honest notes on what this proves. Under `ArmPolicy = "external"` the
property is TRUE BY THE DEFINITION of `Armed`, so it cannot fail in the
specification profile and is **not evidence about it** — its whole job is to
fail for the other settings. And since `"target"` is a subset of
`"external"`, it fails only for `"all"`; under-arming needs the other
property. That division of labour is deliberate, and it is why C1 lists two
properties. It also made resonate-on-convex's second defect explicit: its
profile had carried over-arming as an unresolved question, so nothing fired.
It violates `ArmingIsExternalOnly` in 21 states — the shortest
counterexample in the whole suite, because creating one plain promise is
already enough.

### The row that justifies the exercise

`MC_resume_gap` is the specification profile with **one** switch flipped:
`ResumeLivenessGuard = FALSE`. Everything else is spec-conformant. It
violates `NoDeadDispatch` at depth 7.

That switch corresponds to a line that was missing from **all three**
implementation models: server's `SettleChain` computed
`resumed == suspended awaiters \ selfFulfilled`, convex's `TriggerSettlement`
used `Suspenders(st, i)`, and pg's `CascadeTasks` used `SuspAwaiters(i)` —
none consulted the awaiter's *own* deadline, where the specification's
`ResumeTasks` requires `ps[id].timeoutAt > tnow`. Only the convex run had
instrumented for the consequence, so only the convex run reported it. In the
unified model it is one named property, checked in every profile at once.
This is C6, and it is why C6 is the entry to be suspicious of.

### The response channel

Everything else in the model observes STATE, and the three source models all
did — which is exactly why none of them could have found resonate-pg BUG-2's
response half: an observation of `Proj(i)` cannot see a handler serving an
unprojected record. The unified model closed that with a response channel,
`RespondP(i, ps)`, recording per promise what a handler **answered**:

- `resRegress` — a response answered `pending` for a promise some earlier
  response had already answered settled. Settled-promise stability *on the
  wire*: the client-visible statement, which does not presuppose the
  projection discipline.
- `resUnprojected` — a response carried a record that is not the projection
  at the answering instant. The discipline itself; strictly stronger.

`promise.get` is the reference endpoint (always projected). The
counterexample is 6 states: create a promise with `timeoutAt = 1`, claim it,
`Tick` to `now = 1`, `promise.get` answers `rejected_timedout` — then an
ungated `task.create` serves the raw row and answers **`pending`**. Two
endpoints, one promise, one instant, contradictory answers — reproduced from
the model rather than from reading the spec.

The same construction on `ProjTask` gives the task axis: a task reads
`fulfilled` the moment its promise stops being logically pending, so
answering any live state at such an instant is unprojected, and answering it
after some response already said `fulfilled` is a regression. `task.get` is
the reference endpoint. Under the resonate-pg profile both task properties
violate; the shortest counterexample goes through `task.create`
(`task.get` answers `fulfilled`, an ungated `task.create` then answers
`acquired`), and `task.halt` reaches the same violation in BUG-4's exact
shape. Both sites are ungated by the same `PromiseLivenessGuard`.

What the channel did not cover: status codes (a handler's answer is its
record, not its `2xx`/`4xx`), `preload`, search results, and `task.fence`'s
inner response. `task.fulfill` settled and answered via the promise channel
only — conservative: it can miss regressions, never invent them. This
section is C8's backing.

### Two corrections the model forced on itself

Recorded because both were found by running the model, and both invalidated
an earlier result:

1. **A history variable must not appear in any action's `UNCHANGED` list.**
   `obs` was written by `Next`; when it was also listed in each action's
   `UNCHANGED`, `Tick` became *disabled* in exactly those states where
   advancing the clock would newly expose an expiry — the two conjuncts
   contradicted. The first spec-profile run reported 1 004 962 distinct
   states and "no error"; that number was an artifact. After the fix the
   reachable space was larger and the convex/resume-gap violations appeared
   (they had been unreachable). A "no violation" result over a silently
   truncated state space is the worst outcome a model can produce, and
   nothing in TLC's output flags it — a reachability probe
   (`NoDeadSuspender`) is what exposed it.

2. **`SuspendedTaskHasCallback` needed a liveness qualifier.** Stated as
   *every suspended task has a callback*, it fails on the **specification**
   profile at depth 7: when a promise settles it scrubs its callback rows
   while deliberately not resuming an awaiter that is itself past its
   deadline. That awaiter is dead weight owned by the promise-timeout rule,
   which will fulfil it — so it legitimately sits suspended with no
   callback. The property is correct only as *every suspended task **that is
   still logically alive** has a callback*. This is TIMEOUT ALWAYS WINS
   showing up as a constraint on what an invariant is allowed to say.

### What the model did not do

- **Only one axis of the response surface** — promise records on
  `promise.get` and `task.create`; no status codes, no `preload`.
- **No schedules** (S-01..S-04, R7), and no `task.fence` (T-04).
- **Small scope.** 2 ids / horizon 2 was enough for every violation above to
  appear at depth ≤ 7, but the negative result (`MC_spec`) is bounded
  evidence, not a proof. The specification's own theorems are the proof;
  this was a check.
- **Atomic handlers.** Each action was one atomic step — reflecting the
  advisory-lock / transaction discipline of the implementations rather than
  verifying it.

## The coverage record

Does the one model still find every bug we found? Split the question,
because the answer differs by half:

* **Is the property there?** — does the model have a named property that
  catches this defect class? Answered by a mutation experiment: reproduce
  the defect in the specification and see what fires.
* **Is it detected in the implementation?** — would we find it in
  resonate / resonate-pg / resonate-on-convex today? Answered only by
  replaying a recorded trace.

They are not the same, and conflating them would overstate what the model
does.

| # | bug | property? | detected in the implementation? |
|---|---|---|---|
| resonate BUG-1 | suspended task never woken; `promise_timeouts` armed on target only | ✅ `ObligationsAreDischargeable` | ✅ 2 traces refuse |
| resonate BUG-2 | `resonate:delay` ignored by the SQL backends | ❌ **not modelled** — no delay in the model | ❌ |
| convex BUG-1 | timed-out workflow resurrected and dispatched | ✅ `NoDeadDispatch` | ⚠️ its trace refuses EARLIER, for the waiter rule |
| convex BUG-2 | `debug.*` protocol surface unauthenticated | ❌ **out of scope** — not protocol state | ❌ |
| convex (new) | over-arms: every promise gets a durable timeout | ✅ `ArmingIsExternalOnly` | ❌ no trace exercises it |
| pg BUG-1 | listener on an internal promise never notified | ✅ `ObligationsAreDischargeable` | ❌ its listener is on a timer (external) |
| pg BUG-2 | `task.create` leases against a dead promise | ✅ `NoDeadDispatch` | ❌ |
| pg BUG-2 (response half) | `task.create` serves the unprojected row | ✅ `ResponsesAreProjected` | ❌ no harness records responses |
| pg BUG-2b | `task.continue` ungated | ✅ `NoDeadDispatch` | ❌ |
| pg BUG-3 | `resonate:target = ''` dispatched once, never redelivered | ❌ **not modelled** — target is a tag, not a string | ❌ |
| pg BUG-4 | `task.halt` 200 on a task `task.get` calls fulfilled | ✅ `NoHaltOnDead` | ❌ |
| pg BUG-5 | timeout handlers redispatch a dead workflow | ✅ `NoDeadDispatch` | ❌ |
| cross-cutting | the resume gap all three models share | ✅ `NoDeadDispatch` | ⚠️ as convex BUG-1 |
| cross-cutting | waiters on internal promises | ✅ `ObligationsAreDischargeable` | ✅ 4 traces refuse |

### The two answers

**Properties: yes, with three exceptions.** Ten of thirteen model-checkable
defect classes are caught by a named property, each in 3-7 states, and **no
mutation survives**. The three exceptions were never model-detectable in any
version of this work — they came from code analysis, and two of them
(`resonate:delay`, `resonate:target = ''`) are about tag VALUES the model
abstracts away, while the third is about authentication rather than protocol
state.

**Detection: no — and this got worse, not better.** Only two classes are
actually caught in an implementation today, both by trace refusal. The rest
have the property but nothing to point it at, because the recorded traces
are scripted happy paths that never build the required conditions.

### The honest cost of removing the profiles

The old per-implementation configuration (`MC_pg.cfg`) model-checked
resonate-pg's *semantics* exhaustively and found `NoHaltOnDead` across the
whole reachable space, with no trace needed. Nothing does that now: the
unified model had no way to express "resonate-pg's semantics" by design, and
with the model retired, neither does anything else.

That trade was right, because the failure mode it removed is worse — a
profile masks a bug exactly when the switch IS the property, which is how
`CallbackExternalGuard = FALSE` hid the waiter divergence in both convex and
resonate. But it was a trade, not a free win, and the column of ❌ above is
what it cost.

### What would close the gap

In order of value:

1. **Traces that exercise the bug conditions.** The scenarios were written
   to demonstrate features, not to reach dead-promise interleavings. A
   scenario that lets a promise expire and then calls `task.halt` would turn
   pg BUG-4 from "property exists" into "detected".
2. **Harnesses that record responses.** Three of the ✅-property rows are
   response-level and no harness records what an RPC returned, so they can
   never be trace-detected as things stand.
3. **A fencing mutation.** No mutation targeted `AtMostOneValidClaim`, the
   one property nothing else in this family can even state.

### Takeaways

Three things to carry forward from the retired model, since the catalogue is
what remains of it:

1. **Every Part C row except C12 and the two tag-value bugs is backed by a
   property that actually fired somewhere.** The correct answers are not
   judgment calls; each wrong answer above is a reproduced counterexample.
2. **A property existing is not the same as a defect being detectable.**
   The recorded traces are happy paths, so a green trace replay is weak
   evidence of conformance. New scenarios should be written to reach the
   bug conditions in this appendix, not to demonstrate features.
3. **A switch masks a bug exactly when the switch is the property.** That is
   why this catalogue states correct answers instead of offering profiles,
   and why "we model our own semantics" is not a defensible conformance
   position.
