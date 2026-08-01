import «03-concrete».«state»

open ServerModel

/-- Listeners, like callbacks, attach only to EXTERNAL promises: only
    external promises carry an armed timeout, so only they can
    guarantee the notification is ever sent. An internal awaited is
    `422`, mirroring `promise.register_callback` — without this guard
    the machine would accept an obligation its transition relation
    cannot discharge (an internal promise that dies by deadline is
    settled by projection only; no τ ever emits the `unblock`). -/
def promiseRegisterListener (req : PromiseRegisterListenerReq) (now : Nat) : M PromiseRegisterListenerRes := do
  if !addressValid req.address then
    return { status := 400 }
  match ← getPromise req.awaited with
  | none =>
      return { status := 404 }
  | some pAwaited =>
      if !pAwaited.external then
        return { status := 422 }
      if pAwaited.state == .pending ∧ pAwaited.timeoutAt > now then
        setPromise (pAwaited.addListener req.address)
        return { status := 200, promise := some pAwaited.toRecord }
      else
        return { status := 200, promise := some (pAwaited.project now).toRecord }
