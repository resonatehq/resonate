import «03-concrete».«state»

open ServerModel

namespace ConcreteModel
namespace P

def taskContinue (req : TaskContinueReq) (now : Nat) : H TaskContinueRes := do
  let retryTimeout := (← get).config.retryTimeout
  match ← getTask req.id with
  | none =>
      return { status := 404 }
  | some t =>
      if t.state != .halted then
        return { status := 409 }
      match ← getPromise t.id with
      | none =>
          return { status := 404 }
      | some p =>
          if p.state != .pending || p.timeoutAt ≤ now then
            return { status := 409 }
          let t := { t with state := .pending }
          setTask t
          setTaskTimeout t.id 0 (now + retryTimeout)
          setMessage ((p.tags.get? "resonate:target").getD "") (.execute t.id t.version)
          return { status := 200 }

end P
end ConcreteModel
