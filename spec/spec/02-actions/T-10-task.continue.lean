import «01-objects».«state»

open ServerModel

def taskContinue (req : TaskContinueReq) (now : Nat) : M TaskContinueRes := do
  let retryTimeout := (← get).config.retryTimeout
  match ← getTask req.id with
  | none =>
      return { status := 404 }
  | some t =>
      if t.state != .halted then return { status := 409 }
      match ← getPromise t.id with
      | none =>
          return { status := 404 }
      | some p =>
          let t := { t with state := .pending }
          setTask t
          setTaskTimeout t.id 0 (now + retryTimeout)
          setMessage ((p.tags.get? "resonate:target").getD "") (.execute t.id t.version)
          return { status := 200 }
