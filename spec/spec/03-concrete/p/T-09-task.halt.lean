import «03-concrete».«state»

open ServerModel

/-- Halting exists to take a LIVE task out of circulation. A task whose
    own promise is logically settled has no circulation left — its
    projected state is `.fulfilled` (exactly what `taskGet` reports),
    and halt-on-fulfilled is `409`. Branching on the raw stored task
    here would make the stored-vs-projected divergence observable — the
    one thing the projection discipline forbids. -/
def taskHalt (req : TaskHaltReq) (now : Nat) : H TaskHaltRes := do
  match ← getTask req.id with
  | none =>
      return { status := 404 }
  | some t =>
  match ← getPromise t.id with
  | none =>
      return { status := 404 }
  | some p =>
      if !(p.state == .pending ∧ p.timeoutAt > now) then
        return { status := 409 }
      if t.state == .fulfilled then
        return { status := 409 }
      if t.state == .halted then
        return { status := 200 }
      setTask { t with state := .halted, pid := none, ttl := none }
      delTaskTimeout t.id
      return { status := 200 }
