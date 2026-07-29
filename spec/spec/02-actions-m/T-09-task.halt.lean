import «02-actions-m».«00-touch»

open ServerModel

namespace Materialized

def taskHalt (req : TaskHaltReq) (now : Nat) : M TaskHaltRes := do
  match ← touchTask req.id now with
  | none =>
      return { status := 404 }
  | some (_, none) =>
      return { status := 404 }
  | some (t, some p) =>
      if !(p.state == .pending ∧ p.timeoutAt > now) then
        return { status := 409 }
      if t.state == .fulfilled then
        return { status := 409 }
      if t.state == .halted then
        return { status := 200 }
      setTask { t with state := .halted, pid := none, ttl := none }
      delTaskTimeout t.id
      return { status := 200 }

end Materialized
