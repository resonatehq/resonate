import «03-concrete».«state»

open ServerModel

namespace ConcreteModel
namespace P

def taskCreate (req : TaskCreateReq) (now : Nat) : H TaskCreateRes := do
  let a := req.action
  if !(a.tags.has "resonate:target") ∨ a.tags.timerTargeted then
    return { status := 400 }
  match ← getPromise a.id with
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
      -- Re-acquisition is gated on the PROJECTED promise: a logically
      -- settled promise serves the projected pair — fulfilled task,
      -- settled record — exactly as the expired fresh-create path does.
      -- No lease is ever armed on a logically dead task.
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
                     promise := some (p.project now).toRecord }
        | none =>
            return { status := 409 }

end P
end ConcreteModel
