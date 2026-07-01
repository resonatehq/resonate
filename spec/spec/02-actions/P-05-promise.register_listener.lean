import «01-objects».«state»

open ServerModel

def promiseRegisterListener (req : PromiseRegisterListenerReq) (now : Nat) : M PromiseRegisterListenerRes := do
  match ← getPromise req.awaited with
  | none =>
      return { status := 404 }
  | some pAwaited =>
      if pAwaited.state == .pending then
        if pAwaited.timeoutAt > now then
          setPromise (pAwaited.addListener req.address)
          return { status := 200, promise := some pAwaited.toRecord }
        else
          let projected :=
            if pAwaited.isTimer then
              { pAwaited with state := .resolved, settledAt := some pAwaited.timeoutAt }
            else
              { pAwaited with state := .rejectedTimedout, settledAt := some pAwaited.timeoutAt }
          return { status := 200, promise := some projected.toRecord }
      else
        return { status := 200, promise := some pAwaited.toRecord }
