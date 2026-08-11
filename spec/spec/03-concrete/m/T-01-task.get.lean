import «03-concrete».«m».«00-touch»

open ServerModel

namespace ConcreteModel
namespace M

def taskGet (req : TaskGetReq) (now : Nat) : H TaskGetRes := do
  match ← touchTask req.id now with
  | none =>
      return { status := 404 }
  | some (_, none) =>
      return { status := 404 }
  | some (t, some p) =>
      if p.state == .pending ∧ p.timeoutAt > now then
        return { status := 200, task := some t.toRecord }
      else
        return { status := 200, task := some ({ t with state := .fulfilled, pid := none, ttl := none, resumes := [] }).toRecord }

end M
end ConcreteModel
