import «03-concrete».«m».«00-touch»

open ServerModel

namespace Materialized

def taskCreate (req : TaskCreateReq) (now : Nat) : M TaskCreateRes := do
  let a := req.action
  if !(a.tags.has "resonate:target") ∨ a.tags.timerTargeted then
    return { status := 400 }
  match ← touchPromise a.id now with
  | none =>
      if a.timeoutAt > now then
        let p : PromiseObject :=
          { id := a.id, state := .pending, param := a.param, tags := a.tags,
            timeoutAt := a.timeoutAt, createdAt := now }
        setPromise p
        setPromiseTimeout p.id p.timeoutAt
        let t : TaskObject :=
          { id := p.id, state := .acquired, version := 1,
            ttl := some req.ttl, pid := some req.pid, resumes := [] }
        setTask t
        setTaskTimeout t.id 1 (now + req.ttl)
        return { status := 200, task := some t.toRecord, promise := some p.toRecord }
      else
        -- targeted, therefore not a timer: the only birth verdict here
        let st := PromiseState.rejectedTimedout
        let p : PromiseObject :=
          { id := a.id, state := st, param := a.param, tags := a.tags,
            timeoutAt := a.timeoutAt, createdAt := a.timeoutAt, settledAt := some a.timeoutAt }
        setPromise p
        let t : TaskObject :=
          { id := p.id, state := .fulfilled, version := 0,
            ttl := none, pid := none, resumes := [] }
        setTask t
        return { status := 200, task := some t.toRecord, promise := some p.toRecord }
  | some p =>
      if !(p.tags.has "resonate:target") then
        return { status := 422 }
      if p.state == .pending ∧ p.timeoutAt > now then
        match ← getTask p.id with
        | some t =>
            if t.state == .fulfilled then
              return { status := 200, task := some t.toRecord, promise := some p.toRecord }
            else if t.state == .pending then
              let t := { t with state := .acquired, version := t.version + 1, ttl := some req.ttl, pid := some req.pid, resumes := [] }
              setTask t
              delTaskTimeout t.id
              setTaskTimeout t.id 1 (now + req.ttl)
              return { status := 200, task := some t.toRecord, promise := some p.toRecord }
            else
              return { status := 409 }
        | none =>
            return { status := 409 }
      else
        match ← getTask p.id with
        | some t =>
            return { status := 200,
                     task := some ({ t with state := .fulfilled, pid := none, ttl := none, resumes := [] }).toRecord,
                     promise := some p.toRecord }
        | none =>
            return { status := 409 }

end Materialized
