import «03-concrete».«m».«00-touch»

open ServerModel

namespace ConcreteModel
namespace M

def promiseGet (req : PromiseGetReq) (now : Nat) : H PromiseGetRes := do
  match ← touchPromise req.id now with
  | none =>
      return { status := 404 }
  | some p =>
      return { status := 200, promise := some p.toRecord }

end M
end ConcreteModel
