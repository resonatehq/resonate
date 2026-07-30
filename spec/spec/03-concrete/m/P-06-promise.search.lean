import «03-concrete».«m».«00-touch»

open ServerModel

namespace Materialized

def promiseSearch (_req : PromiseSearchReq) (_now : Nat) : M PromiseSearchRes := do
  return { status := 501 }

end Materialized
