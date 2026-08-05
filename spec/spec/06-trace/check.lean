import «06-trace».json
import «06-trace».intervals

/-!  # `check <trace.ndjson>`

Load a recorded run and ask the specification whether any schedule of
internal steps explains it. -/

open TraceCheck TraceCheck.Json TraceCheck.Intervals in
def main (args : List String) : IO UInt32 := do
  match args with
  | [] => IO.eprintln "usage: check <trace.ndjson> [cap]"; return 2
  | path :: rest =>
    let cap := (rest.head?.bind (·.toNat?)).getD 20000
    let trace ← loadTrace path
    IO.eprintln s!"loaded {trace.length} events from {path}"
    let t0 ← IO.monoMsNow
    let v := validate trace 16 cap
    let line := verdictLine v
    -- force it: `let` is call-by-need, so without this the timer would
    -- measure thunk construction and always report 0ms
    if line.length == 0 then IO.eprintln "" else pure ()
    let t1 ← IO.monoMsNow
    IO.eprintln s!"pinned   {line}   {t1 - t0}ms"
    -- the same question of the INTERVAL checker, whose completeness needs
    -- no `InstantsSuffice`: τs may fire at any critical instant in the gap
    let t2 ← IO.monoMsNow
    let viv := validateIv trace 16 cap
    let liv := verdictLine viv
    if liv.length == 0 then IO.eprintln "" else pure ()
    let t3 ← IO.monoMsNow
    IO.eprintln s!"interval {liv}   {t3 - t2}ms"
    IO.eprintln s!"agree: {verdictKind v == verdictKind viv}"
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
    | .inconclusive _ _ => return 3
