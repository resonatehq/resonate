import «03-concrete».«m».«00-touch»

open ServerModel

namespace ConcreteModel
namespace M

def scheduleGet (req : ScheduleGetReq) (_now : Nat) : H ScheduleGetRes := do
  match ← getSchedule req.id with
  | none =>
      return { status := 404 }
  | some s =>
      return { status := 200, schedule := some s }

end M
end ConcreteModel
