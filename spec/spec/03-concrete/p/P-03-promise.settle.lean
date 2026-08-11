import «03-concrete».«state»

open ServerModel

namespace ConcreteModel
namespace P

def promiseSettle (req : PromiseSettleReq) (now : Nat) : H PromiseSettleRes := do
  if !req.state.settable then
    return { status := 400 }
  match ← getPromise req.id with
  | none =>
      return { status := 404 }
  | some p =>
      if p.state == .pending ∧ p.timeoutAt > now then
        let listeners := p.listeners
        let callbacks := p.callbacks
        let p := { p with state := req.state, value := req.value, settledAt := some now, callbacks := [], listeners := [] }
        setSettled p
        for address in listeners do
          setMessage address (.unblock p.toRecord)
        for awaiterId in callbacks do
          defer { awaited := p.id, awaiter := awaiterId }
        return { status := 200, promise := some p.toRecord }
      else
        return { status := 200, promise := some (p.project now).toRecord }

end P
end ConcreteModel
