import «02-actions-m».«00-touch»

open ServerModel

namespace Materialized

def promiseRegisterListener (req : PromiseRegisterListenerReq) (now : Nat) : M PromiseRegisterListenerRes := do
  if !addressValid req.address then
    return { status := 400 }
  match ← touchPromise req.awaited now with
  | none =>
      return { status := 404 }
  | some pAwaited =>
      if pAwaited.state == .pending ∧ pAwaited.timeoutAt > now then
        setPromise (pAwaited.addListener req.address)
        return { status := 200, promise := some pAwaited.toRecord }
      else
        return { status := 200, promise := some pAwaited.toRecord }

end Materialized
