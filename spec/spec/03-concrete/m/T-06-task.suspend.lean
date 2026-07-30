import «03-concrete».«m».«00-touch»

open ServerModel

namespace Materialized

def taskSuspend (req : TaskSuspendReq) (now : Nat) : M TaskSuspendRes := do
  if req.actions.isEmpty then
    return { status := 400 }
  if req.actions.any (·.awaited == req.id) then
    return { status := 400 }
  let awaitedIds := req.actions.map (·.awaited)
  if awaitedIds.eraseDups.length != awaitedIds.length then
    return { status := 400 }
  match ← touchTask req.id now with
  | none =>
      return { status := 404 }
  | some (_, none) =>
      return { status := 409 }
  | some (t, some tp) =>
      if t.state != .acquired then
        return { status := 409 }
      if tp.state != .pending ∨ tp.timeoutAt ≤ now then
        return { status := 409 }
      if t.version != req.version then
        return { status := 409 }
      let mut settled := false
      for action in req.actions do
        match ← touchPromise action.awaited now with
        | none =>
            return { status := 422 }
        | some pa =>
            if !pa.external then
              return { status := 422 }
            if pa.state != .pending ∨ pa.timeoutAt ≤ now then
              settled := true
      if settled then
        setTask { t with resumes := [] }
        return { status := 300 }
      else
        for action in req.actions do
          match ← touchPromise action.awaited now with
          | some pa =>
              setPromise (pa.addCallback req.id)
          | none =>
              pure ()
        setTask { t with state := .suspended, pid := none, ttl := none, resumes := [] }
        delTaskTimeout t.id
        return { status := 200 }

end Materialized
