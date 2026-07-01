import «01-objects».«state»

open ServerModel

def taskRelease (req : TaskReleaseReq) (now : Nat) : M TaskReleaseRes := do
  let retryTimeout := (← get).config.retryTimeout
  match ← getTask req.id with
  | none =>
      return { status := 404 }
  | some t =>
  match ← getPromise t.id with
  | none =>
      return { status := 409 }
  | some p =>
      if t.state != .acquired then return { status := 409 }
      if p.state != .pending ∨ p.timeoutAt ≤ now then return { status := 409 }
      if t.version != req.version then return { status := 409 }
      let t := { t with state := .pending, pid := none, ttl := none }
      setTask t
      delTaskTimeout t.id
      setTaskTimeout t.id 0 (now + retryTimeout)
      setMessage ((p.tags.get? "resonate:target").getD "") (.execute t.id t.version)
      return { status := 200 }
