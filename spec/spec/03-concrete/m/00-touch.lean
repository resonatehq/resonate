import «03-concrete».«state»

/-!  # The materialized machine (-m) — touch

The -m machine is the projected machine (-p) under one change of read
discipline: every read of an object MATERIALIZES what -p's projection
would have shown, by firing the anticipated timeout transition at the
moment of observation. `touchPromise` IS -p's `processPromiseTimeout`, run
eagerly; a handler reading through touch therefore sees stored state
that agrees, byte for byte, with -p's projected view.

Handlers keep -p's guards VERBATIM — a compound liveness check
`p.state == .pending ∧ p.timeoutAt > now` is still a true statement of
materialized state — and -p's projected-view expressions where they are
idempotent on materialized state. The line-level diff between the twin
machines is exactly: reads become touches, `project` disappears from
responses, and the extra writes hide inside the touch.  -/

open ServerModel

namespace Materialized

/-- Fire the anticipated promise-timeout transition, then read. The
    materialization body is -p's `processPromiseTimeout`, verbatim. -/
def touchPromise (id : String) (now : Nat) : M (Option PromiseObject) := do
  match ← getPromise id with
  | none =>
      return none
  | some p =>
      if p.state != .pending ∨ p.timeoutAt > now then
        return some p
      else
        let listeners := p.listeners
        let callbacks := p.callbacks
        let p := { p.project p.timeoutAt with callbacks := [], listeners := [] }
        setSettled p
        for address in listeners do
          setMessage address (.unblock p.toRecord)
        for awaiterId in callbacks do
          defer { awaited := p.id, awaiter := awaiterId }
        return some p

/-- Read a task together with its promise, materializing first. The
    task is re-read after the touch: materialization may have
    fulfilled it. -/
def touchTask (id : String) (now : Nat) :
    M (Option (TaskObject × Option PromiseObject)) := do
  match ← getTask id with
  | none => return none
  | some t =>
  match ← touchPromise t.id now with
  | none => return some (t, none)
  | some p =>
  match ← getTask id with
  | none => return some (t, some p)
  | some t => return some (t, some p)

end Materialized
