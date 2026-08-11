import «03-concrete».«m».«00-touch»

open ServerModel

namespace Materialized

def taskSearch (_req : TaskSearchReq) (_now : Nat) : H TaskSearchRes := do
  return { status := 501 }

end Materialized
