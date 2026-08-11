import «03-concrete».«m».«00-touch»

open ServerModel

namespace Materialized

def scheduleSearch (_req : ScheduleSearchReq) (_now : Nat) : H ScheduleSearchRes := do
  return { status := 501 }

end Materialized
