import «03-concrete».«m».«00-touch»

open ServerModel

namespace Materialized

def promiseSettle (req : PromiseSettleReq) (now : Nat) : M PromiseSettleRes := do
  if !req.state.settable then
    return { status := 400 }
  match ← touchPromise req.id now with
  | none =>
      return { status := 404 }
  | some p =>
      if p.state == .pending ∧ p.timeoutAt > now then
        let listeners := p.listeners
        let callbacks := p.callbacks
        let p := { p with state := req.state, value := req.value, settledAt := some now, callbacks := [], listeners := [] }
        setPromise p
        delPromiseTimeout p.id
        match ← getTask p.id with
        | some t =>
            setTask { t with state := .fulfilled, pid := none, ttl := none, resumes := [] }
            delTaskTimeout t.id
        | none =>
            pure ()
        for address in listeners do
          setMessage address (.unblock p.toRecord)
        for awaiterId in callbacks do
          defer { awaited := p.id, awaiter := awaiterId }
        return { status := 200, promise := some p.toRecord }
      else
        return { status := 200, promise := some p.toRecord }

end Materialized
