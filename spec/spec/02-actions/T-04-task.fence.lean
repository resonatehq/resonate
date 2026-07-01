import «02-actions».«P-02-promise.create»
import «02-actions».«P-03-promise.settle»

open ServerModel

def taskFence (req : TaskFenceReq) (now : Nat) : M TaskFenceRes := do
  match ← getTask req.id with
  | none =>
      return { status := 404 }
  | some t =>
  match ← getPromise t.id with
  | none =>
      return { status := 409 }
  | some p =>
      if t.state != .acquired then return { status := 409 }
      if p.state != .pending ∨ p.timeoutAt ≤ now then return { status := 409 }
      if t.version != req.version then return { status := 409 }
      match req.action with
      | .create r =>
          let res ← promiseCreate r now
          return { status := 200, action := some (.create res) }
      | .settle r =>
          let res ← promiseSettle r now
          return { status := 200, action := some (.settle res) }
