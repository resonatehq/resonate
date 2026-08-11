import «03-concrete».«m».«00-touch»

open ServerModel

namespace ConcreteModel
namespace M

def scheduleSearch (_req : ScheduleSearchReq) (_now : Nat) : H ScheduleSearchRes := do
  return { status := 501 }

end M
end ConcreteModel
