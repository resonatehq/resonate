import «03-concrete».«state»

open ServerModel

def promiseSearch (_req : PromiseSearchReq) (_now : Nat) : M PromiseSearchRes := do
  return { status := 501 }
