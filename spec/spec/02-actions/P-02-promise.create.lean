import «01-objects».«state»

open ServerModel

def promiseCreate (req : PromiseCreateReq) (now : Nat) : M PromiseCreateRes := do
  let retryTimeout := (← get).config.retryTimeout
  match ← getPromise req.id with
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
        if p.external then
          setPromiseTimeout p.id p.timeoutAt
        match p.tags.get? "resonate:target" with
        | none =>
            return { status := 200, promise := some p.toRecord }
        | some target =>
            let t : TaskObject := { id := p.id, state := .pending, version := 0 }
            setTask t
            match p.tags.get? "resonate:delay" with
            | none =>
                setTaskTimeout t.id 0 (now + retryTimeout)
                setMessage target (.execute t.id t.version)
                return { status := 200, promise := some p.toRecord }
            | some delayStr =>
                let delay := delayStr.toNat!
                if delay > now then
                  setTaskTimeout t.id 0 delay
                  return { status := 200, promise := some p.toRecord }
                else
                  setTaskTimeout t.id 0 (now + retryTimeout)
                  setMessage target (.execute t.id t.version)
                  return { status := 200, promise := some p.toRecord }
      else
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
          let t : TaskObject :=
            { id := p.id, state := .fulfilled, version := 0,
              ttl := none, pid := none, resumes := [] }
          setTask t
          return { status := 200, promise := some p.toRecord }
        else
          return { status := 200, promise := some p.toRecord }
  | some p =>
      if p.state == .pending ∧ p.timeoutAt ≤ now then
        let projected :=
          if p.isTimer then
            { p with state := .resolved, settledAt := some p.timeoutAt }
          else
            { p with state := .rejectedTimedout, settledAt := some p.timeoutAt }
        return { status := 200, promise := some projected.toRecord }
      else
        return { status := 200, promise := some p.toRecord }
