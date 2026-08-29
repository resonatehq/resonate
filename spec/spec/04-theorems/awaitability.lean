import «02-abstract».«external»

namespace Abstraction

open ServerModel AbstractModel

/-! # The awaitability chain, evaluated

`otype` is one axis with three values, and the doors ask
`otype.awaitable` rather than naming a constructor. That distinction is
invisible to the type checker: a door written `otype == .external`
compiles and silently refuses every RUNNABLE promise — which is the
ordinary case, a parent awaiting the child a worker runs.

So it is evaluated here rather than reasoned about. Each door, against
each value of the axis. -/

private def targetTags : Tags := [("resonate:target", "poll://any@w")]
private def externalTags : Tags := [("resonate:external", "true")]
private def internalTags : Tags := []

private def idOf (suffix : String) : Ident := { origin := "o", suffix := suffix }

private def promiseWith (tags : Tags) : PromiseObject :=
  { state := .pending, param := {}, tags := tags, timeoutAt := 9000, createdAt := 0 }

private def objectWith (suffix : String) (tags : Tags) (task : Option TaskObject) : Object :=
  { id := idOf suffix, promise := promiseWith tags, task := task }

private def state : ServerState :=
  { objects := [ objectWith "root" targetTags (some { state := .acquired, version := 1 })
               , objectWith "runnable" targetTags (some { state := .pending, version := 0 })
               , objectWith "external" externalTags none
               , objectWith "internal" internalTags none ] }

private def callbackStatus (awaited : String) : Nat :=
  (run true (promiseRegisterCallback
      { awaited := idOf awaited, awaiter := idOf "root" } 100) state).1.status

private def listenerStatus (awaited : String) : Nat :=
  (run true (promiseRegisterListener
      { awaited := idOf awaited, address := "poll://any@w" } 100) state).1.status

theorem otype_of_targeted : Tags.otype targetTags = .runnable := by rfl
theorem otype_of_external : Tags.otype externalTags = .external := by rfl
theorem otype_of_neither : Tags.otype internalTags = .internal := by rfl

theorem callback_admits_runnable : callbackStatus "runnable" = 200 := by rfl
theorem callback_admits_external : callbackStatus "external" = 200 := by rfl
theorem callback_refuses_internal : callbackStatus "internal" = 422 := by rfl

theorem listener_admits_runnable : listenerStatus "runnable" = 200 := by rfl
theorem listener_admits_external : listenerStatus "external" = 200 := by rfl
theorem listener_refuses_internal : listenerStatus "internal" = 422 := by rfl

end Abstraction
