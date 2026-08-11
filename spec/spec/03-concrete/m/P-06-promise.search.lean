import «03-concrete».«m».«00-touch»

open ServerModel

namespace Materialized

def promiseSearch (_req : PromiseSearchReq) (_now : Nat) : H PromiseSearchRes := do
  return { status := 501 }

end Materialized
