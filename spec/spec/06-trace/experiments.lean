import «06-trace».validator

/-!  # How far does it go?

Traces are built by running a script — internal steps included — through
the specification and keeping only what an outside observer sees. The τs
are thrown away. The validator has to rediscover a schedule that explains
the external events, which is exactly the job it would have against a
real implementation.

Two axes, because they behave completely differently:

* **length** — more events, one object at a time;
* **fanout** — more obligations armed AT ONCE.

The second is the one that bites, and the reason is structural.
Canonicalisation collapses ORDERINGS: firing two independent τs in
either order lands in the same state, so `n!` becomes 1. It does not
collapse SUBSETS: with `n` independent obligations due, "fired none",
"fired a", "fired b", "fired both" are four genuinely different states.
So the closure is `2^n` in the number of SIMULTANEOUSLY ENABLED τs,
regardless of how long the trace is. -/

namespace TraceCheck.Experiments

open ServerModel Equivalence TraceCheck

def ext : Tags := [("resonate:external", "true")]
def tgt : Tags := [("resonate:target", "w1")]

/-- One full workflow on a fresh object pair: create the awaited and the
    targeted promise, acquire, suspend, settle, resume, re-acquire,
    fulfil. The `τResume` is the hidden step the validator must recover. -/
def workflow (i : Nat) (t0 t1 : Nat) : List (Request × Nat) :=
  let a := s!"a{i}"
  let x := s!"x{i}"
  [ (.promiseCreate { id := a, timeoutAt := 100000, param := {}, tags := ext }, t0)
  , (.promiseCreate { id := x, timeoutAt := 100000, param := {}, tags := tgt }, t0)
  , (.taskAcquire   { id := x, version := 0, pid := "p", ttl := 50 }, t0)
  , (.taskSuspend   { id := x, version := 1, actions := [{ awaited := a, awaiter := x }] }, t0)
  , (.promiseSettle { id := a, state := .resolved, value := {} }, t1)
  , (.τResume       { awaited := a, awaiter := x }, t1)          -- hidden
  , (.taskAcquire   { id := x, version := 1, pid := "p", ttl := 50 }, t1)
  , (.taskFulfill   { id := x, version := 2,
                      action := { id := x, state := .resolved, value := {} } }, t1) ]

/-- `k` workflows back to back — pure length, one object pair live at a
    time, so at most one obligation is ever armed. -/
def lengthScript (k : Nat) : List (Request × Nat) :=
  (List.range k).flatMap fun i => workflow i (10 + i * 10) (15 + i * 10)

/-- `n` external promises that all expire before the final observation,
    so at that instant `n` promise timeouts are enabled at once and
    nothing has discharged them. This is the fanout axis. -/
def fanoutScript (n : Nat) : List (Request × Nat) :=
  (List.range n).map (fun i =>
      (Request.promiseCreate { id := s!"a{i}", timeoutAt := 20 + i, param := {}, tags := ext }, 10))
  ++ [ (.promiseGet { id := "a0" }, 500) ]

/-- Fanout with the timeouts actually fired in the hidden part, so the
    validator must find the right SUBSET rather than the empty one. -/
def fanoutFiredScript (n : Nat) : List (Request × Nat) :=
  (List.range n).map (fun i =>
      (Request.promiseCreate { id := s!"a{i}", timeoutAt := 20 + i, param := {}, tags := ext }, 10))
  ++ (List.range n).map (fun i => (Request.τPromiseTimeout s!"a{i}", 500))
  ++ [ (.promiseGet { id := "a0" }, 500) ]

/-! ## Running -/

structure Result where
  label   : String
  events  : Nat
  verdict : Verdict
  ms      : Nat

def run (label : String) (script : List (Request × Nat))
    (fuel : Nat := 16) (cap : Nat := 4000) : IO Result := do
  let trace := recordFrom script
  let t0 ← IO.monoMsNow
  let v := validate trace fuel cap
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

open TraceCheck TraceCheck.Experiments in
def main : IO Unit := do
  IO.eprintln "── discrimination ─────────────────────────────────────────────"
  let good := recordFrom (lengthScript 1)
  IO.eprintln s!"honest trace          {verdictLine (validate good)}"
  IO.eprintln s!"first response lied   {verdictLine (validate (corrupt good))}"
  IO.eprintln s!"last response lied    {verdictLine (validate (corruptLast good))}"
  (← IO.getStdout).flush

  IO.eprintln ""
  IO.eprintln "── length (one obligation live at a time) ─────────────────────"
  for k in [4, 8, 16, 32, 48] do
    report (← run s!"workflows={k}" (lengthScript k))

  IO.eprintln ""
  IO.eprintln "── fanout (n obligations armed at once, none fired) ───────────"
  for n in [6, 8, 9, 10, 11, 12] do
    report (← run s!"armed={n}" (fanoutScript n))

  IO.eprintln ""
  IO.eprintln "── fanout (n armed AND fired in the hidden part) ──────────────"
  for n in [6, 8, 9, 10] do
    report (← run s!"fired={n}" (fanoutFiredScript n))
