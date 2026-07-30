import «03-concrete».«state»

open ServerModel

def promiseRegisterListener (req : PromiseRegisterListenerReq) (now : Nat) : M PromiseRegisterListenerRes := do
  if !addressValid req.address then
    return { status := 400 }
  match ← getPromise req.awaited with
  | none =>
      return { status := 404 }
  | some pAwaited =>
      if pAwaited.state == .pending ∧ pAwaited.timeoutAt > now then
        setPromise (pAwaited.addListener req.address)
        return { status := 200, promise := some pAwaited.toRecord }
      else
        return { status := 200, promise := some (pAwaited.project now).toRecord }
