import «01-objects».«state»

open ServerModel

def taskHalt (req : TaskHaltReq) (_now : Nat) : M TaskHaltRes := do
  match ← getTask req.id with
  | none =>
      return { status := 404 }
  | some t =>
      if t.state == .fulfilled then return { status := 409 }
      if t.state == .halted then return { status := 200 }
      setTask { t with state := .halted, pid := none, ttl := none }
      delTaskTimeout t.id
      return { status := 200 }
