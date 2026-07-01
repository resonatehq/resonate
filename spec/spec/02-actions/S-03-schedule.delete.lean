import «01-objects».«state»

open ServerModel

def scheduleDelete (req : ScheduleDeleteReq) (_now : Nat) : M ScheduleDeleteRes := do
  match ← getSchedule req.id with
  | none =>
      return { status := 404 }
  | some s =>
      delSchedule s.id
      delScheduleTimeout s.id
      return { status := 200 }
