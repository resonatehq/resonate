import «03-concrete».«m».«00-touch»

open ServerModel

namespace Materialized

def scheduleDelete (req : ScheduleDeleteReq) (_now : Nat) : M ScheduleDeleteRes := do
  match ← getSchedule req.id with
  | none =>
      return { status := 404 }
  | some s =>
      delSchedule s.id
      delScheduleTimeout s.id
      return { status := 200 }

end Materialized
