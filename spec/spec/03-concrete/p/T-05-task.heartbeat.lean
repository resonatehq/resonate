import «03-concrete».«state»

open ServerModel

namespace ConcreteModel
namespace P

def taskHeartbeat (req : TaskHeartbeatReq) (now : Nat) : H TaskHeartbeatRes := do
  for ref in req.tasks do
    match ← getTask ref.id with
    | none =>
        pure ()
    | some t =>
        if t.state == .acquired ∧ t.version == ref.version ∧ t.pid == some req.pid then
          match ← getPromise t.id with
          | some p =>
              if p.state == .pending ∧ p.timeoutAt > now then
                delTaskTimeout t.id
                setTaskTimeout t.id 1 (now + t.ttl.getD 0)
          | none =>
              pure ()
  return { status := 200 }

end P
end ConcreteModel
