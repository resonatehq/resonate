import «03-concrete».«state»

open ServerModel

namespace ConcreteModel
namespace P

def scheduleGet (req : ScheduleGetReq) (_now : Nat) : H ScheduleGetRes := do
  match ← getSchedule req.id with
  | none =>
      return { status := 404 }
  | some s =>
      return { status := 200, schedule := some s }

end P
end ConcreteModel
