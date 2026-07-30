import «03-concrete».«m».«00-touch»

open ServerModel

namespace Materialized

def promiseGet (req : PromiseGetReq) (now : Nat) : M PromiseGetRes := do
  match ← touchPromise req.id now with
  | none =>
      return { status := 404 }
  | some p =>
      return { status := 200, promise := some p.toRecord }

end Materialized
