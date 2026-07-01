import «01-objects».«state»

open ServerModel

def enqueueResume (awaitedId awaiterId : String) (now : Nat) : M Unit := do
  let some p ← getPromise awaiterId | pure ()
  let some t ← getTask awaiterId | pure ()
  if now < p.timeoutAt then
    match t.state with
    | .suspended =>
        let retryTimeout := (← get).config.retryTimeout
        let t := { t with state := .pending, version := t.version + 1 }
        setTask t
        setTaskTimeout t.id 0 (now + retryTimeout)
        setMessage ((p.tags.get? "resonate:target").getD "") (.execute t.id t.version)
    | .pending | .acquired | .halted =>
        setTask { t with resumes := t.resumes ++ [awaitedId] }
    | .fulfilled =>
        pure ()
