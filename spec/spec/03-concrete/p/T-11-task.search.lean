import «03-concrete».«state»

open ServerModel

def taskSearch (_req : TaskSearchReq) (_now : Nat) : M TaskSearchRes := do
  return { status := 501 }
