import «04-abstract».«01-state»

/-!  # The coalesced machine — promise handlers

Handlers write objects and return responses; they emit no messages and
record no auxiliary state. Dispatching an `execute`, notifying a
listener, waking an awaiter — all of that is the rules' job
(`05-rules.lean`). In particular:

* `promiseCreate` of a targeted promise creates the task and stops —
  the dispatch rule emits the `execute`. The `resonate:delay` tag is
  consumed at creation: it seeds the task's `retryAt`, so the
  create-side delay machinery of the base spec collapses to one field
  initialization.
* `promiseSettle` writes THE PROMISE ONLY. The task is fulfilled by
  fact T (on the next touch, or by `Rules.taskFulfillment`); awaiters
  and listeners stay on the promise for the batch rules.  -/

namespace AbstractModel

open ServerModel (PromiseState
                  PromiseGetReq PromiseGetRes
                  PromiseCreateReq PromiseCreateRes
                  PromiseSettleReq PromiseSettleRes
                  PromiseRegisterCallbackReq PromiseRegisterCallbackRes
                  PromiseRegisterListenerReq PromiseRegisterListenerRes
                  PromiseSearchReq PromiseSearchRes)

def promiseGet (req : PromiseGetReq) (now : Nat) : M PromiseGetRes := do
  match ← touchPromise req.id now with
  | none =>
      return { status := 404 }
  | some p =>
      return { status := 200, promise := some p.toRecord }

def promiseCreate (req : PromiseCreateReq) (now : Nat) : M PromiseCreateRes := do
  match ← touchPromise req.id now with
  | some p =>
      return { status := 200, promise := some p.toRecord }
  | none =>
      if req.timeoutAt > now then
        let p : PromiseObject :=
          { id := req.id
            state := .pending
            param := req.param
            tags := req.tags
            timeoutAt := req.timeoutAt
            createdAt := now }
        setPromise p
        if p.tags.has "resonate:target" then
          -- The delay tag seeds `retryAt`: the first dispatch is due at
          -- the delay if it is still ahead, immediately otherwise.
          let due :=
            match p.tags.get? "resonate:delay" with
            | some d => max (ServerModel.parseNat d) now
            | none => now
          setTask { id := p.id, state := .pending, version := 0,
                    retryAt := some due }
        return { status := 200, promise := some p.toRecord }
      else
        -- Born past its deadline: fact P holds at birth, so the promise
        -- is written settled and its task (if targeted) fulfilled.
        let state :=
          if req.tags.isTimer then
            PromiseState.resolved
          else
            PromiseState.rejectedTimedout
        let p : PromiseObject :=
          { id := req.id
            state := state
            param := req.param
            tags := req.tags
            timeoutAt := req.timeoutAt
            createdAt := req.timeoutAt
            settledAt := some req.timeoutAt }
        setPromise p
        if p.tags.has "resonate:target" then
          setTask { id := p.id, state := .fulfilled, version := 0 }
        return { status := 200, promise := some p.toRecord }

def promiseSettle (req : PromiseSettleReq) (now : Nat) : M PromiseSettleRes := do
  if !req.state.settable then
    return { status := 400 }
  match ← touchPromise req.id now with
  | none =>
      return { status := 404 }
  | some p =>
      if p.state == .pending then
        let p := { p with state := req.state, value := req.value, settledAt := some now }
        setPromise p
        return { status := 200, promise := some p.toRecord }
      else
        return { status := 200, promise := some p.toRecord }

def promiseRegisterCallback (req : PromiseRegisterCallbackReq) (now : Nat) :
    M PromiseRegisterCallbackRes := do
  if req.awaited == req.awaiter then
    return { status := 400 }
  match ← touchPromise req.awaited now with
  | none =>
      return { status := 404 }
  | some pAwaited =>
  match ← touchPromise req.awaiter now with
  | none =>
      return { status := 422 }
  | some pAwaiter =>
      if !(pAwaiter.tags.has "resonate:target") then
        return { status := 422 }
      if !pAwaited.external then
        return { status := 422 }
      if pAwaited.state == .pending then
        if pAwaiter.state == .pending then
          setPromise (pAwaited.addCallback req.awaiter)
        return { status := 200, promise := some pAwaited.toRecord }
      else
        return { status := 200, promise := some pAwaited.toRecord }

/-- Registration on an already-settled promise returns the record without
    registering, exactly as in the base spec — even though this machine's
    retained-listener drain could naturally serve a late registration,
    admitting one would produce an `unblock` the base machine never
    sends. -/
def promiseRegisterListener (req : PromiseRegisterListenerReq) (now : Nat) :
    M PromiseRegisterListenerRes := do
  if !ServerModel.addressValid req.address then
    return { status := 400 }
  match ← touchPromise req.awaited now with
  | none =>
      return { status := 404 }
  | some pAwaited =>
      if pAwaited.state == .pending then
        setPromise (pAwaited.addListener req.address)
        return { status := 200, promise := some pAwaited.toRecord }
      else
        return { status := 200, promise := some pAwaited.toRecord }

def promiseSearch (_req : PromiseSearchReq) (_now : Nat) : M PromiseSearchRes := do
  return { status := 501 }

end AbstractModel
