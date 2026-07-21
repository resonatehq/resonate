import «01-objects».«state»

open ServerModel

/-- A deliverable listener address: `http(s)://…`, or `poll://…` carrying an
    `@group` (e.g. `poll://any@default` — a bare `poll://default` names no
    group and could never be routed). -/
def ServerModel.addressValid (a : String) : Bool :=
  a.startsWith "http://" || a.startsWith "https://" ||
  (a.startsWith "poll://" && a.contains '@')

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
