import «valid».«lean».json
import «valid».«lean».intervals

/-!  # `check <trace.ndjson>`

Load a recorded run and ask the specification whether any schedule of
internal steps explains it. -/

def usage : String :=
  "usage: checktrace [cap] [fuel]   (reads the trace on stdin)\n\n\
   Reads a recorded run as NDJSON — one {kind, now, req, res} object per\n\
   line — and asks the specification whether ANY schedule of internal\n\
   steps explains it.\n\n\
   exit 0  ADMISSIBLE   some execution the spec permits has these events\n\
   exit 1  REFUTED      no execution does; the offending event is printed\n\
   exit 3  INCONCLUSIVE the search hit a bound, or the trace uses schedules\n\
   exit 2  usage/parse error"

open TraceCheck TraceCheck.Json TraceCheck.Intervals in
def main (args : List String) : IO UInt32 := do
  if args.contains "-h" || args.contains "--help" then
    IO.println usage; return 0
  let cap  := (args[0]?.bind (·.toNat?)).getD 20000
  -- Fuel is an argument because generated traces reach deeper internal step chains
  -- than recorded ones: the differential fuzzer routinely needs more than
  -- the 16 that suffices for real traffic, and a DECLINE there would make
  -- the comparison pass vacuously.
  let fuel := (args[1]?.bind (·.toNat?)).getD 16
  let loaded ← loadTraceStdin.toBaseIO
  let trace ← match loaded with
              | .error e => do IO.eprintln s!"checktrace: {e}"; return 2
              | .ok tr   => pure tr
  IO.eprintln s!"loaded {trace.length} events"
  let t0 ← IO.monoMsNow
  let v := validate trace fuel cap
  let line := verdictLine v
  -- force it: `let` is call-by-need, so without this the timer would
  -- measure thunk construction and always report 0ms
  if line.length == 0 then IO.eprintln "" else pure ()
  let t1 ← IO.monoMsNow
  IO.eprintln s!"{line}   {t1 - t0}ms"
  match v with
  | .admissible w _ _ =>
      IO.eprintln s!"witness: {w.length} internal steps the server never reported"
      for (r, t) in w.take 20 do
        IO.eprintln s!"  @{t}  {r}"
      if w.length > 20 then IO.eprintln s!"  … and {w.length - 20} more"
      return 0
  | .refuted i _ =>
      IO.eprintln s!"the specification cannot explain event {i}:"
      match trace[i]? with
      | some o => IO.eprintln s!"  @{o.now}  req {repr o.req}\n  got  {repr o.res}"
      | none   => pure ()
      return 1
  | .inconclusive _ r =>
      IO.eprintln s!"  reason: {r}"
      return 3
