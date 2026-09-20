# Durable promises on an object store

Why a bucket is enough, what has to be true for that to hold, and what happens
at every point a process can stop.

## The property the protocol hands you

Every operation the protocol admits is **single-origin**. An origin is everything
before the first `:` in an id, and:

* `promise.create`, `promise.get`, `promise.settle` name one promise;
* `task.*` name a task, whose id *is* a promise id;
* `promise.register_callback` names an awaited and an awaiter, and the protocol
  refuses the pair unless they share an origin ("Awaiter and awaited must belong
  to the same origin");
* `promise.register_listener` names an awaited and an address, and an address is
  not state;
* `task.create` and `task.fence` carry an action, whose id is in the origin the
  task is in;
* a schedule has no promises of its own — only a template for the ones it fires,
  each of which is its own origin.

So everything an operation has to read, and everything it has to write, lives in
one place. That is what makes **one conditional write of one object** enough to
commit a whole transition, and it is the whole reason this needs no log, no
lease, no lock and no consensus.

The searches and `debug.snap` are the exception, and they are exactly the
operations that read many objects one at a time. They are surveys, not atomic
steps, and nothing in the protocol promised otherwise.

## Three prefixes

| key | holds | written by |
|---|---|---|
| `wf/<enc(origin)>` | every promise and task of one origin, as one document | the commit loop |
| `sched/<enc(id)>` | one schedule | the schedule service |
| `t/<NN>/<20-digit deadline>_<enc(target)>@<token>` | a deadline, as a zero-byte object | whoever armed it |

The document is a header line plus one line per promise and per task, all in a
canonical form: ASCII only, minimal escapes, fixed key order, integers without
exponents, and omission rather than `null` or `[]` for anything empty. Two
encoders given equal state produce identical bytes, which is what lets a writer
ask "did anything change?" by comparing bytes, and a simulation ask "is this the
same state?" by comparing snapshots.

A timer key is a zero-byte object whose *name* is the whole record. The deadline
is zero-padded so lexicographic order is time order, which makes "the nearest
deadlines" a capped ascending listing. `NN` spreads the keys across shards,
because deadlines only ever increase and a monotone key prefix is the one access
pattern object stores are worst at. The token after `@` names the **arm** that
wrote it — see below.

## What a write is

Two preconditions, and no others:

* `If-None-Match: *` — create only. The first writer of an origin wins.
* `If-Match: <etag>` — replace what was read, and only that.

Three failure outcomes, which are not interchangeable:

| the store says | what is known | what to do |
|---|---|---|
| `412` precondition failed | it did not land, and the state has moved | **re-decide**: read again, decide again, never replay |
| `409` conflict | nothing — it could not be ordered against another write | retry the same conditional write |
| no answer | nothing — it may have landed | tell the caller 503 and let it retry, because every operation is idempotent |

Re-deciding rather than replaying is the important one. A decision made against
state that no longer exists is not a decision that can be re-applied: the promise
it was going to create may exist now, the task it was going to acquire may be
held by somebody else. Replaying would produce answers no sequential execution
gives, which is exactly what the linearizability search refutes.

A read is validated with `If-None-Match: <etag>`, so a cached document costs one
round trip and no body. Without that, two servers over one bucket serve stale
reads — the first thing the checker found.

The cache holds the canonical bytes rather than a decoded document, because a
re-decide after a lost race has to start from what the store says. It is bounded
by document count *and* by total weight, since one origin's document grows with
every promise in it and a count alone would leave the memory unbounded; whichever
bound binds first evicts the oldest read. One entry is always kept, however heavy,
because the batch that just committed it is what reads it next.

## The order effects go in

**Arm the deadline → commit the document → disarm the old deadline → send the
messages → answer.**

Every window between two steps has to leave a state something repairs:

| stopped after | what is left | what repairs it |
|---|---|---|
| arming | a timer object nothing points at | it fires into a sweep with nothing due, which collects it |
| committing | the transition is durable, the old deadline is still armed | the old deadline fires into a document that has moved on, and is collected |
| disarming | the transition is durable, the messages did not go | the task's retry deadline, which was committed *before* the message left |
| sending | the caller was told nothing | the caller retries, and every operation is idempotent |

Notice what is *not* in that table: a committed document whose deadline was never
armed. That is the one state nothing repairs — the promise never times out, the
task is never offered again, and every answer about it stays correct forever. So
the arm comes first, and a failed arm fails the request rather than committing
anyway.

## The token on a timer key

A deadline's object is named by the arm that wrote it, and the document records
that name. Two things need it:

* **Disarming.** A writer removes the object its own predecessor wrote, and
  nothing else. Without the token the name is only (target, deadline), and a
  deadline that becomes the nearest one again — a promise timeout after a shorter
  lease goes away, a schedule deleted and recreated in the same minute — gets
  removed by somebody else's disarm.
* **Collecting.** Whatever fires an orphan deletes it by name afterwards. An
  attempt that armed its object and then failed to commit leaves that object
  behind; a retry that loads the same document version computes the same
  deadline, so a token derived from the document's own counter would give it the
  same *name*. The collect then lands on the live object.

Which is why the token is unique to the **arm** rather than to the commit: 63
bits from the server's own stream, one per arm, recorded in the document that
takes the deadline on.

## One actor per origin, and group commit

A request does not commit; it *enqueues* on the actor for its origin. The event
loop drains once per poll, so everything that arrived together rides one
conditional write — and each request still sees the one before it, because they
are decided in order against one document.

What that buys is not only throughput. A burst of operations on one origin is the
common shape of a durable execution — a task acquires, settles a child, suspends
— and each one of those as its own read-modify-write against S3 would contend
with the others for the same object.

## What the state does not contain

No log, no manifest, no write-ahead anything, no lock objects, no lease objects,
no compaction markers, no index. The documents *are* the state and the timer keys
*are* the schedule. Two consequences worth being plain about:

* Nothing has to be replayed to serve a request, so a cold server answers as soon
  as it can read one object.
* There is nothing to compact, so there is no background process whose failure
  is a slow leak.

The cost is in the searches: a search reads every document in the prefix, one
object at a time. That is why it is off by default in the reference blob server,
and why this one answers searches but never promised they are atomic.
