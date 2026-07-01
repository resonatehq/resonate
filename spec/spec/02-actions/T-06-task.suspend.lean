import «01-objects».«state»

open ServerModel

def taskSuspend (req : TaskSuspendReq) (now : Nat) : M TaskSuspendRes := do
  match ← getTask req.id with
  | none =>
      return { status := 404 }
  | some t =>
  match ← getPromise t.id with
  | none =>
      return { status := 409 }
  | some tp =>
      if t.state != .acquired then return { status := 409 }
      if tp.state != .pending ∨ tp.timeoutAt ≤ now then return { status := 409 }
      if t.version != req.version then return { status := 409 }
      let mut settled := false
      for action in req.actions do
        match ← getPromise action.awaited with
        | none =>
            return { status := 422 }
        | some pa =>
            if pa.state != .pending ∨ pa.timeoutAt ≤ now then
              settled := true
      if settled then
        setTask { t with resumes := [] }
        return { status := 300 }
      else
        for action in req.actions do
          match ← getPromise action.awaited with
          | some pa =>
              setPromise (pa.addCallback req.id)
          | none =>
              pure ()
        setTask { t with state := .suspended, pid := none, ttl := none, resumes := [] }
        delTaskTimeout t.id
        return { status := 200 }
