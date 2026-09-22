import «04-theorems».«liveness»

namespace Abstraction

open ServerModel AbstractModel

/-! # The axis, evaluated

Three values, and each answers three questions at once:

  internal   nobody outside its own call graph; nothing runs it; NO armed
             timeout — its deadline is a projection, never a write
  external   anyone may await it; nothing runs it; armed timeout
  runnable   anyone may await it, and a worker is handed the execution;
             armed timeout

The first two columns are the doors. The third is `enabledInternal`: the
machine owes a timeout step only where someone can be waiting for it, so
an internal promise never costs a timer.

`otype` is one axis with three values, and the doors ask
`otype.awaitable` rather than naming a constructor. That distinction is
invisible to the type checker: a door written `otype == .external`
compiles and silently refuses every RUNNABLE promise — which is the
ordinary case, a parent awaiting the child a worker runs.

So it is evaluated here rather than reasoned about. Each door, and the
arming, against each value of the axis. -/

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

private def latePromise (tags : Tags) : PromiseObject :=
  { state := .pending, param := {}, tags := tags, timeoutAt := 50, createdAt := 0 }

private def lateState : ServerState :=
  { objects := [ { id := idOf "runnable", promise := latePromise targetTags,
                   task := some { state := .pending, version := 0 } }
               , { id := idOf "external", promise := latePromise externalTags }
               , { id := idOf "internal", promise := latePromise internalTags } ] }

private def armed (suffix : String) : Bool :=
  enabledInternal (.internal (.promiseTimeout { id := idOf suffix })) 100 lateState

theorem arms_a_runnable_deadline : armed "runnable" = true := by rfl
theorem arms_an_external_deadline : armed "external" = true := by rfl
theorem arms_no_internal_deadline : armed "internal" = false := by rfl

end Abstraction
