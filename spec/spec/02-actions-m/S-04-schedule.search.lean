import «02-actions-m».«00-touch»

open ServerModel

namespace Materialized

def scheduleSearch (_req : ScheduleSearchReq) (_now : Nat) : M ScheduleSearchRes := do
  return { status := 501 }

end Materialized
