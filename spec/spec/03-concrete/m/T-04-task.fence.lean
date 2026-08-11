import «03-concrete».«m».«P-02-promise.create»
import «03-concrete».«m».«P-03-promise.settle»

open ServerModel

namespace ConcreteModel
namespace M

def taskFence (req : TaskFenceReq) (now : Nat) : H TaskFenceRes := do
  if req.action.targetId == req.id then
    return { status := 400 }
  match ← touchTask req.id now with
  | none =>
      return { status := 404 }
  | some (_, none) =>
      return { status := 409 }
  | some (t, some p) =>
      if t.state != .acquired then
        return { status := 409 }
      if p.state != .pending ∨ p.timeoutAt ≤ now then
        return { status := 409 }
      if t.version != req.version then
        return { status := 409 }
      match req.action with
      | .create r =>
          let res ← promiseCreate r now
          return { status := 200, action := some (.create res) }
      | .settle r =>
          let res ← promiseSettle r now
          return { status := 200, action := some (.settle res) }

end M
end ConcreteModel
