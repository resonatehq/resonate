import «01-objects».«state»

open ServerModel

def promiseRegisterCallback (req : PromiseRegisterCallbackReq) (now : Nat) : M PromiseRegisterCallbackRes := do
  if req.awaited == req.awaiter then
    return { status := 400 }
  match ← getPromise req.awaited with
  | none =>
      return { status := 404 }
  | some pAwaited =>
  match ← getPromise req.awaiter with
  | none =>
      return { status := 422 }
  | some pAwaiter =>
      if !(pAwaiter.tags.has "resonate:target") then
        return { status := 422 }
      if !pAwaited.external then
        return { status := 422 }
      if pAwaited.state == .pending ∧ pAwaited.timeoutAt > now then
        if pAwaiter.state == .pending ∧ pAwaiter.timeoutAt > now then
          setPromise (pAwaited.addCallback req.awaiter)
        return { status := 200, promise := some pAwaited.toRecord }
      else
        return { status := 200, promise := some (pAwaited.project now).toRecord }
