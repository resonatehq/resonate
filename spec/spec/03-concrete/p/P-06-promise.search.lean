import «03-concrete».«state»

open ServerModel

namespace ConcreteModel
namespace P

def promiseSearch (_req : PromiseSearchReq) (_now : Nat) : H PromiseSearchRes := do
  return { status := 501 }

end P
end ConcreteModel
