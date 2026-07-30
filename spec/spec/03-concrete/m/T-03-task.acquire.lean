import «03-concrete».«m».«00-touch»

open ServerModel

namespace Materialized

def taskAcquire (req : TaskAcquireReq) (now : Nat) : M TaskAcquireRes := do
  match ← touchTask req.id now with
  | none =>
      return { status := 404 }
  | some (_, none) =>
      return { status := 409 }
  | some (t, some p) =>
      if t.state != .pending then
        return { status := 409 }
      if p.state != .pending ∨ p.timeoutAt ≤ now then
        return { status := 409 }
      if t.version != req.version then
        return { status := 409 }
      let t := { t with state := .acquired, version := t.version + 1, ttl := some req.ttl, pid := some req.pid, resumes := [] }
      setTask t
      delTaskTimeout t.id
      setTaskTimeout t.id 1 (now + req.ttl)
      return { status := 200, task := some t.toRecord, promise := some p.toRecord }

end Materialized
