import «01-objects».«state»

open ServerModel

def taskGet (req : TaskGetReq) (now : Nat) : M TaskGetRes := do
  match ← getTask req.id with
  | none =>
      return { status := 404 }
  | some t =>
  match ← getPromise t.id with
  | none =>
      return { status := 404 }
  | some p =>
      if p.state == .pending ∧ p.timeoutAt > now then
        return { status := 200, task := some t.toRecord }
      else
        return { status := 200, task := some ({ t with state := .fulfilled, pid := none, ttl := none, resumes := [] }).toRecord }
