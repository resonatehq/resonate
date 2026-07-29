import «01-objects».«state»

open ServerModel

def scheduleCreate (req : ScheduleCreateReq) (now : Nat) : M ScheduleCreateRes := do
  match ← getSchedule req.id with
  | some s =>
      return { status := 200, schedule := some s }
  | none =>
      let s : Schedule :=
        { id := req.id
          cron := req.cron
          promiseId := req.promiseId
          promiseTimeout := req.promiseTimeout
          promiseParam := req.promiseParam
          promiseTags := req.promiseTags
          createdAt := now
          nextRunAt := nextCron req.cron now
          lastRunAt := none }
      setSchedule s
      setScheduleTimeout s.id s.nextRunAt
      return { status := 200, schedule := some s }
