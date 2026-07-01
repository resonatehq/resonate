import «01-objects».«state»

open ServerModel

def taskAcquire (req : TaskAcquireReq) (now : Nat) : M TaskAcquireRes := do
  match ← getTask req.id with
  | none =>
      return { status := 404 }
  | some t =>
  match ← getPromise t.id with
  | none =>
      return { status := 409 }
  | some p =>
      if t.state != .pending then return { status := 409 }
      if p.state != .pending ∨ p.timeoutAt ≤ now then return { status := 409 }
      if t.version != req.version then return { status := 409 }
      let t := { t with state := .acquired, version := t.version + 1, ttl := some req.ttl, pid := some req.pid, resumes := [] }
      setTask t
      delTaskTimeout t.id
      setTaskTimeout t.id 1 (now + req.ttl)
      return { status := 200, task := some t.toRecord, promise := some p.toRecord }
