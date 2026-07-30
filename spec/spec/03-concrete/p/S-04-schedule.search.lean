import «03-concrete».«state»

open ServerModel

def scheduleSearch (_req : ScheduleSearchReq) (_now : Nat) : M ScheduleSearchRes := do
  return { status := 501 }
