import «01-objects».«state»

open ServerModel

def taskFulfill (req : TaskFulfillReq) (now : Nat) : M TaskFulfillRes := do
  if !req.action.state.settable then
    return { status := 400 }
  match ← getTask req.id with
  | none =>
      return { status := 404 }
  | some t =>
  match ← getPromise t.id with
  | none =>
      return { status := 409 }
  | some p =>
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
