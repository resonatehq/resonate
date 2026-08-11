import «03-concrete».«state»

open ServerModel

namespace ConcreteModel
namespace P

def processResume (req : ResumeReq) (now : Nat) : H ResumeRes := do
  let some t ← getTask req.awaiter    | return { outcome := .absent }
  let some p ← getPromise req.awaiter | return { outcome := .absent }
  if now >= p.timeoutAt then
    return { outcome := .expired }
  match t.state with
  | .suspended =>
      let retryTimeout := (← get).config.retryTimeout
      let t := { t with state := .pending, resumes := [req.awaited] }
      setTask t
      setTaskTimeout t.id 0 (now + retryTimeout)
      let target := (p.tags.get? "resonate:target").getD ""
      if target != "" then
        setMessage target (.execute t.id t.version)
      return { outcome := .resumed }
  | .pending | .acquired | .halted =>
      if t.resumes.contains req.awaited then
        return { outcome := .duplicate }
      else
        setTask { t with resumes := t.resumes ++ [req.awaited] }
        return { outcome := .buffered }
  | .fulfilled =>
      return { outcome := .fulfilled }

def drain (now : Nat) : H Unit := do
  for d in (← get).deferred do
    undefer d
    let _ ← processResume d now

def step {α} (act : H α) (now : Nat) : H α := do
  let res ← act
  drain now
  return res

end P
end ConcreteModel
