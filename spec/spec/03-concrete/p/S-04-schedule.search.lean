import «03-concrete».«state»

open ServerModel

namespace ConcreteModel
namespace P

def scheduleSearch (_req : ScheduleSearchReq) (_now : Nat) : H ScheduleSearchRes := do
  return { status := 501 }

end P
end ConcreteModel
