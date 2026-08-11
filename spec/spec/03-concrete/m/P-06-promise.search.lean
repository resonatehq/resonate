import «03-concrete».«m».«00-touch»

open ServerModel

namespace ConcreteModel
namespace M

def promiseSearch (_req : PromiseSearchReq) (_now : Nat) : H PromiseSearchRes := do
  return { status := 501 }

end M
end ConcreteModel
