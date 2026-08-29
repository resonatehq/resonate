import «valid».«lean».intervals

/-!  # A trace from a real resonate server, checked against the spec

Captured from `resonatehq/resonate` v0.9.8 (Rust), release build, SQLite
backend, `RESONATE_DEBUG=true` so background loops are paused and
`resonate:debug_time` is honoured — which makes the run deterministic and
lets the observer name the instant of every call.

Eleven external calls over HTTP, every one `200`. Transcribed verbatim
from the recorded responses; nothing here was computed by the
specification.

## The hidden step

The interesting event is #6:

    task.get @2100 -> {"task":{"id":"o1.x","resumes":1,"state":"pending","version":1}}

The task was `suspended` at 1200 and nobody woke it explicitly. resonate
discharges the resume INSIDE `promise.settle`'s transaction, so an
observer sees a task that changed state with no request to explain it.
The specification does not fix that schedule — `deferred` plus a drain internal step
is one admissible schedule, doing it eagerly is another. So the validator
has to supply the missing `callback` itself. That is the whole point of
asking for admissibility rather than for reproduction.

## Two divergences from the specification, found while capturing

Neither is a specification violation — both are resonate REFUSING things
the specification permits, so they never appear in an accepted trace —
but they are worth recording because they mean the two machines do not
accept the same request language:

* `task.suspend` requires awaiter and awaited to carry the same
  `resonate:origin`, else `400 "Awaiter and awaited must belong to the
  same origin"`. The specification's `taskSuspend` has no such guard: it
  requires the awaited to be external and the awaiter to be targeted, and
  nothing about origins.
* `promise.create` requires the id to equal `resonate:origin` or to start
  with `origin ++ "."`, else `400 "Promise ID must be prefixed by
  resonate:origin"`. `01-protocol/validation.lean` has no such rule.

Both are extra 400s on the implementation side. A conformance suite that
only replays accepted traces cannot see them; they showed up because the
first two capture attempts were rejected. -/

namespace TraceCheck.Real

open ServerModel AbstractModel Abstraction TraceCheck

def aTags : Tags := [("resonate:external", "true"), ("resonate:origin", "o1")]
def xTags : Tags := [("resonate:origin", "o1"), ("resonate:target", "poll://any@w1")]

def pa (settled : Option Nat) (st : PromiseState) : PromiseRecord :=
  { id := oid "o1.a", state := st, param := {}, value := {}, tags := aTags, timeoutAt := 900000, createdAt := 1000, settledAt := settled }

def px (settled : Option Nat) (st : PromiseState) : PromiseRecord :=
  { id := oid "o1.x", state := st, param := {}, value := {}, tags := xTags, timeoutAt := 900000, createdAt := 1000, settledAt := settled }

/-- The recorded run. `now` is the `resonate:debug_time` sent with each
    call, so the specification is driven by the same clock the server
    used. -/
def trace : List Observation :=
  [ -- 0
    { now := 1000, req := .promiseCreate { id := oid "o1.a", timeoutAt := 900000, param := {}, tags := aTags }, res := .promiseCreate { status := 200, promise := some (pa none .pending) } }
    -- 1
  , { now := 1000, req := .promiseCreate { id := oid "o1.x", timeoutAt := 900000, param := {}, tags := xTags }, res := .promiseCreate { status := 200, promise := some (px none .pending) } }
    -- 2
  , { now := 1000, req := .taskGet { id := oid "o1.x" }, res := .taskGet { status := 200, task := some { id := oid "o1.x", state := .pending, version := 0, resumes := 0, ttl := none, pid := none } } }
    -- 3
  , { now := 1100, req := .taskAcquire { id := oid "o1.x", version := 0, pid := "p0", ttl := 60000 }, res := .taskAcquire { status := 200, task := some { id := oid "o1.x", state := .acquired, version := 1, resumes := 0, ttl := some 60000, pid := some "p0" }, promise := some (px none .pending), preload := [] } }
    -- 4
  , { now := 1200, req := .taskSuspend { id := oid "o1.x", version := 1, actions := [{ awaited := oid "o1.a", awaiter := oid "o1.x" }] }, res := .taskSuspend { status := 200, preload := [] } }
    -- 5 — the resume happens inside this call, invisibly
  , { now := 2000, req := .promiseSettle { id := oid "o1.a", state := .resolved, value := {} }, res := .promiseSettle { status := 200, promise := some (pa (some 2000) .resolved) } }
    -- 6 — pending again, resumes = 1, with no request to explain it
  , { now := 2100, req := .taskGet { id := oid "o1.x" }, res := .taskGet { status := 200, task := some { id := oid "o1.x", state := .pending, version := 1, resumes := 1, ttl := none, pid := none } } }
    -- 7
  , { now := 2200, req := .taskAcquire { id := oid "o1.x", version := 1, pid := "p1", ttl := 60000 }, res := .taskAcquire { status := 200, task := some { id := oid "o1.x", state := .acquired, version := 2, resumes := 0, ttl := some 60000, pid := some "p1" }, promise := some (px none .pending), preload := [] } }
    -- 8
  , { now := 2300, req := .taskFulfill { id := oid "o1.x", version := 2, action := { id := oid "o1.x", state := .resolved, value := {} } }, res := .taskFulfill { status := 200, promise := some (px (some 2300) .resolved) } }
    -- 9
  , { now := 2400, req := .promiseGet { id := oid "o1.x" }, res := .promiseGet { status := 200, promise := some (px (some 2300) .resolved) } }
    -- 10
  , { now := 2400, req := .promiseGet { id := oid "o1.a" }, res := .promiseGet { status := 200, promise := some (pa (some 2000) .resolved) } }
  ]

/-- **Not** a tamper, as it turns out. Reporting the task still
    `suspended` at event 6 is ADMISSIBLE: the specification leaves the
    drain schedule free, so a server that has not yet discharged the
    deferred resume is conformant too. Kept because it demonstrates the
    schedule freedom on a real trace — the checker accepts both the
    eager server we recorded and a lazy one we did not. -/
def lazyVariant : List Observation :=
  trace.mapIdx fun i o =>
    if i == 6 then
      { o with res := .taskGet { status := 200, task := some { id := oid "o1.x", state := .suspended, version := 1, resumes := 0, ttl := none, pid := none } } }
    else o

/-- A genuine impossibility: `o1.a` was settled at 2000 and nothing can
    un-settle a promise, so reporting it `pending` at 2400 cannot be
    explained by any schedule. -/
def tampered : List Observation :=
  trace.mapIdx fun i o =>
    if i == 10 then
      { o with res := .promiseGet { status := 200, promise := some (pa none .pending) } }
    else o

/-- A second impossibility, on the task channel: version 2 was reached at
    2200, so a later read cannot report version 1. -/
def tampered2 : List Observation :=
  trace.mapIdx fun i o =>
    if i == 9 then
      { o with res := .promiseGet { status := 404 } }
    else o

end TraceCheck.Real

open TraceCheck TraceCheck.Intervals TraceCheck.Real in
def main : IO Unit := do
  IO.eprintln "── a real resonate trace, against the specification ───────────"
  IO.eprintln s!"  events            {trace.length}"
  IO.eprintln s!"  recorded run      {verdictLine (validate trace)}"
  IO.eprintln s!"  lazy variant      {verdictLine (validate lazyVariant)}"
  IO.eprintln s!"  un-settled a=200  {verdictLine (validate tampered)}"
  IO.eprintln s!"  x vanished @2400  {verdictLine (validate tampered2)}"
  IO.eprintln ""
  match validate trace with
  | .admissible w _ _ =>
      IO.eprintln "  witness — the internal steps the server never told us about:"
      for (r, t) in w do
        IO.eprintln s!"    @{t}  {r}"
  | v => IO.eprintln s!"  no witness: {verdictLine v}"
