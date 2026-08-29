import «valid».«lean».intervals

/-!  # How far does it go?

Traces are built by running a script — internal steps included — through
the specification and keeping only what an outside observer sees. The internal steps
are thrown away. The validator has to rediscover a schedule that explains
the external events, which is exactly the job it would have against a
real implementation.

Two axes, because they behave completely differently:

* **length** — more events, one object at a time;
* **fanout** — more obligations armed AT ONCE.

The second is the one that bites, and the reason is structural.
Canonicalisation collapses ORDERINGS: firing two independent internal steps in
either order lands in the same state, so `n!` becomes 1. It does not
collapse SUBSETS: with `n` independent obligations due, "fired none",
"fired a", "fired b", "fired both" are four genuinely different states.
So the closure is `2^n` in the number of SIMULTANEOUSLY ENABLED internal steps,
regardless of how long the trace is. -/

namespace TraceCheck.Experiments

open ServerModel AbstractModel Abstraction Equivalence TraceCheck TraceCheck.Intervals

def ext : Tags := [("resonate:external", "true")]
def tgt : Tags := [("resonate:target", "w1")]

/-- One full workflow on a fresh object pair: create the awaited and the
    targeted promise, acquire, suspend, settle, resume, re-acquire,
    fulfil. The `callback` is the hidden step the validator must recover. -/
def workflow (i : Nat) (t0 t1 : Nat) : List (Step × Nat) :=
  let a := oid s!"a{i}"
  let x := oid s!"x{i}"
  [ (.external (.promiseCreate { id := a, timeoutAt := 100000, param := {}, tags := ext }), t0)
  , (.external (.promiseCreate { id := x, timeoutAt := 100000, param := {}, tags := tgt }), t0)
  , (.external (.taskAcquire   { id := x, version := 0, pid := "p", ttl := 50 }), t0)
  , (.external (.taskSuspend   { id := x, version := 1, actions := [{ awaited := a, awaiter := x }] }), t0)
  , (.external (.promiseSettle { id := a, state := .resolved, value := {} }), t1)
  , (.internal (.callback a x), t1)          -- hidden
  , (.external (.taskAcquire   { id := x, version := 1, pid := "p", ttl := 50 }), t1)
  , (.external (.taskFulfill { id := x, version := 2, action := { id := x, state := .resolved, value := {} } }), t1) ]

/-- `k` workflows back to back — pure length, one object pair live at a
    time, so at most one obligation is ever armed. -/
def lengthScript (k : Nat) : List (Step × Nat) :=
  (List.range k).flatMap fun i => workflow i (10 + i * 10) (15 + i * 10)

/-- `n` external promises that all expire before the final observation,
    so at that instant `n` promise timeouts are enabled at once and
    nothing has discharged them. This is the fanout axis. -/
def fanoutScript (n : Nat) : List (Step × Nat) :=
  (List.range n).map (fun i =>
      (.external (.promiseCreate { id := oid s!"a{i}", timeoutAt := 20 + i, param := {}, tags := ext }), 10))
  ++ [ (.external (.promiseGet { id := oid "a0" }), 500) ]

/-- Fanout with the timeouts actually fired in the hidden part, so the
    validator must find the right SUBSET rather than the empty one. -/
def fanoutFiredScript (n : Nat) : List (Step × Nat) :=
  (List.range n).map (fun i =>
      (.external (.promiseCreate { id := oid s!"a{i}", timeoutAt := 20 + i, param := {}, tags := ext }), 10))
  ++ (List.range n).map (fun i => (.internal (.promiseTimeout (oid s!"a{i}")), 500))
  ++ [ (.external (.promiseGet { id := oid "a0" }), 500) ]

/-- **Regression.** `a` expires on its own; that expiry defers a resume
    for `x`, which wakes it. The first cone keyed relevance on the object
    an internal step NAMES — `touches (promiseTimeout "a") = ["a"]` — so it refused to
    fire the timeout when the observed request read `x`, and reported this
    conforming trace REFUTED. `affects` plus a transitive closure fixed
    it. Kept because nothing else in the suite has an internal step whose consequences
    land on a different object. -/
def crossObjectScript : List (Step × Nat) :=
  [ (.external (.promiseCreate { id := oid "a", timeoutAt := 30, param := {}, tags := ext }), 10)
  , (.external (.promiseCreate { id := oid "x", timeoutAt := 100000, param := {}, tags := tgt }), 10)
  , (.external (.taskAcquire   { id := oid "x", version := 0, pid := "p", ttl := 50 }), 10)
  , (.external (.taskSuspend   { id := oid "x", version := 1, actions := [{ awaited := oid "a", awaiter := oid "x" }] }), 12)
  , (.internal (.promiseTimeout (oid "a")), 40)                            -- hidden
  , (.internal (.callback (oid "a") (oid "x")), 40)      -- hidden
  , (.external (.taskGet { id := oid "x" }), 50) ]

/-! ## Running -/

structure Result where
  label   : String
  events  : Nat
  verdict : Verdict
  ms      : Nat

def run (label : String) (script : List (Step × Nat))
    (coned : Bool := true) (fuel : Nat := 16) (cap : Nat := 200000) : IO Result := do
  let trace := recordFrom script
  let t0 ← IO.monoMsNow
  let v := validateBy false coned trace fuel cap
  -- FORCE it: `let` is call-by-need, so without this the timer measures
  -- the construction of a thunk and the work happens later, in `report`.
  let line := verdictLine v
  if line.length == 0 then IO.eprintln "" else pure ()
  let t1 ← IO.monoMsNow
  return { label, events := trace.length, verdict := v, ms := t1 - t0 }

def report (r : Result) : IO Unit :=
  IO.eprintln s!"{r.label.pushn ' ' (22 - min 22 r.label.length)} events={r.events}  {verdictLine r.verdict}  {r.ms}ms"

/-- A trace the specification cannot explain: take a real one and lie
    about one response. If this came back ADMISSIBLE the machinery would
    be worthless, so it is checked first. -/
def corrupt (trace : List Observation) : List Observation :=
  match trace with
  | [] => []
  | o :: rest => { o with res := .promiseGet { status := 404 } } :: rest

def corruptLast (trace : List Observation) : List Observation :=
  match trace.reverse with
  | [] => []
  | o :: rest => (({ o with res := .taskFulfill { status := 409 } } : Observation) :: rest).reverse

end TraceCheck.Experiments

open TraceCheck TraceCheck.Intervals TraceCheck.Experiments in
def main : IO Unit := do
  IO.eprintln "── discrimination (cone on) ───────────────────────────────────"
  let good := recordFrom (lengthScript 1)
  IO.eprintln s!"honest trace          {verdictLine (validate good)}"
  IO.eprintln s!"first response lied   {verdictLine (validate (corrupt good))}"
  IO.eprintln s!"last response lied    {verdictLine (validate (corruptLast good))}"

  IO.eprintln ""
  IO.eprintln "── the reduction agrees with the unreduced checker ────────────"
  -- honest traces, and corrupted ones. The dangerous direction is a
  -- cone that turns REFUTED into ADMISSIBLE: that would be unsoundness,
  -- not just imprecision.
  let mut allAgree := true
  for n in [1, 2, 3, 4, 5, 6, 7, 8] do
    for (tag, t) in [("cross ", recordFrom crossObjectScript),
                     ("honest", recordFrom (fanoutFiredScript n)),
                     ("lied  ", corrupt (recordFrom (fanoutFiredScript n))),
                     ("workfl", recordFrom (lengthScript (min n 4))),
                     ("wf-lie", corruptLast (recordFrom (lengthScript (min n 4))))] do
      let a := verdictKind (validateBy false true  t 16 200000)
      let b := verdictKind (validateBy false false t 16 200000)
      if a != b then allAgree := false
      IO.eprintln s!"  n={n} {tag}  cone={a}  full={b}  {if a == b then "AGREE" else "*** DIFFER ***"}"
  IO.eprintln s!"  --> {if allAgree then "all agree" else "DISAGREEMENT FOUND"}"

  IO.eprintln ""
  IO.eprintln "── fanout, cone OFF (the old wall) ────────────────────────────"
  for n in [8, 10, 11, 12] do
    report (← run s!"armed={n}" (fanoutScript n) false)

  IO.eprintln ""
  IO.eprintln "── fanout, cone ON ────────────────────────────────────────────"
  for n in [8, 16, 32, 64, 128, 256, 512] do
    report (← run s!"armed={n}" (fanoutScript n) true)

  IO.eprintln ""
  IO.eprintln "── fanout FIRED, cone ON ──────────────────────────────────────"
  for n in [8, 16, 32, 64, 128] do
    report (← run s!"fired={n}" (fanoutFiredScript n) true)

  IO.eprintln ""
  IO.eprintln "── length, cone ON ────────────────────────────────────────────"
  for k in [16, 64, 128, 256] do
    report (← run s!"workflows={k}" (lengthScript k) true)
