import «03-concrete».«m».«00-touch»

open ServerModel

namespace ConcreteModel
namespace M

def taskSearch (_req : TaskSearchReq) (_now : Nat) : H TaskSearchRes := do
  return { status := 501 }

end M
end ConcreteModel
