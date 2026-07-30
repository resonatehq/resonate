import «03-concrete».«m».«00-touch»

open ServerModel

namespace Materialized

def taskFulfill (req : TaskFulfillReq) (now : Nat) : M TaskFulfillRes := do
  if !req.action.state.settable then
    return { status := 400 }
  match ← touchTask req.id now with
  | none =>
      return { status := 404 }
  | some (_, none) =>
      return { status := 409 }
  | some (t, some p) =>
      if t.state != .acquired then
        return { status := 409 }
      if p.state != .pending ∨ p.timeoutAt ≤ now then
        return { status := 409 }
      if t.version != req.version then
        return { status := 409 }
      let listeners := p.listeners
      let callbacks := p.callbacks
      let p := { p with state := req.action.state, value := req.action.value, settledAt := some now, callbacks := [], listeners := [] }
      setPromise p
      delPromiseTimeout p.id
      setTask { t with state := .fulfilled, pid := none, ttl := none, resumes := [] }
      delTaskTimeout t.id
      for address in listeners do
        setMessage address (.unblock p.toRecord)
      for awaiterId in callbacks do
        defer { awaited := p.id, awaiter := awaiterId }
      return { status := 200, promise := some p.toRecord }

end Materialized
