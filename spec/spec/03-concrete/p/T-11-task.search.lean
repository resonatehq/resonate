import «03-concrete».«state»

open ServerModel

namespace ConcreteModel
namespace P

def taskSearch (_req : TaskSearchReq) (_now : Nat) : H TaskSearchRes := do
  return { status := 501 }

end P
end ConcreteModel
