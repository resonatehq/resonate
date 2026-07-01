import «01-objects».«state»

open ServerModel

def scheduleGet (req : ScheduleGetReq) (_now : Nat) : M ScheduleGetRes := do
  match ← getSchedule req.id with
  | none =>
      return { status := 404 }
  | some s =>
      return { status := 200, schedule := some s }
