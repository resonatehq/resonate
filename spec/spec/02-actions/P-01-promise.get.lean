import «01-objects».«state»

open ServerModel

def promiseGet (req : PromiseGetReq) (now : Nat) : M PromiseGetRes := do
  match ← getPromise req.id with
  | none =>
      return { status := 404 }
  | some p =>
      if p.state == .pending then
        if p.timeoutAt ≤ now then
          let projected :=
            if p.isTimer then
              { p with state := .resolved, settledAt := some p.timeoutAt }
            else
              { p with state := .rejectedTimedout, settledAt := some p.timeoutAt }
          return { status := 200, promise := some projected.toRecord }
        else
          return { status := 200, promise := some p.toRecord }
      else
        return { status := 200, promise := some p.toRecord }
