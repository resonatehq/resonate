import «03-concrete».«state»

open ServerModel

def promiseGet (req : PromiseGetReq) (now : Nat) : H PromiseGetRes := do
  match ← getPromise req.id with
  | none =>
      return { status := 404 }
  | some p =>
      return { status := 200, promise := some (p.project now).toRecord }
