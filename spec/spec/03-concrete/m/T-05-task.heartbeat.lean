import «03-concrete».«m».«00-touch»

open ServerModel

namespace ConcreteModel
namespace M

def taskHeartbeat (req : TaskHeartbeatReq) (now : Nat) : H TaskHeartbeatRes := do
  for ref in req.tasks do
    match ← touchTask ref.id now with
    | some (t, some p) =>
        if t.state == .acquired ∧ t.version == ref.version ∧ t.pid == some req.pid then
          if p.state == .pending ∧ p.timeoutAt > now then
            delTaskTimeout t.id
            setTaskTimeout t.id 1 (now + t.ttl.getD 0)
    | _ =>
        pure ()
  return { status := 200 }

end M
end ConcreteModel
