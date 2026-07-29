import «02-actions-m».«00-touch»

open ServerModel

namespace Materialized

def taskSearch (_req : TaskSearchReq) (_now : Nat) : M TaskSearchRes := do
  return { status := 501 }

end Materialized
