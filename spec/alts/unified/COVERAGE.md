# Does the one model still find every bug we found?

Split the question, because the answer differs by half.

* **Is the property there?** — does the model have a named property that
  catches this defect class? Answered by a mutation experiment
  (`MUTATIONS.md`): reproduce the defect in the specification and see what
  fires.
* **Is it detected in the implementation?** — would we find it in
  resonate / resonate-pg / resonate-on-convex today? Answered only by
  replaying a recorded trace (`TRACES.md`).

They are not the same, and conflating them would overstate what this model
does.

| # | bug | property? | detected in the implementation? |
|---|---|---|---|
| resonate BUG-1 | suspended task never woken; `promise_timeouts` armed on target only | ✅ `ObligationsAreDischargeable` | ✅ 2 traces refuse |
| resonate BUG-2 | `resonate:delay` ignored by the SQL backends | ❌ **not modelled** — no delay in the model | ❌ |
| convex BUG-1 | timed-out workflow resurrected and dispatched | ✅ `NoDeadDispatch` | ⚠️ its trace refuses EARLIER, for the waiter rule |
| convex BUG-2 | `debug.*` protocol surface unauthenticated | ❌ **out of scope** — not protocol state | ❌ |
| convex (new) | over-arms: every promise gets a durable timeout | ✅ `ArmingIsExternalOnly` | ❌ no trace exercises it |
| pg BUG-1 | listener on an internal promise never notified | ✅ `ObligationsAreDischargeable` | ❌ its listener is on a timer (external) |
| pg BUG-2 | `task.create` leases against a dead promise | ✅ `NoDeadDispatch` | ❌ |
| pg BUG-2 (response half) | `task.create` serves the unprojected row | ✅ `ResponsesAreProjected` | ❌ no harness records responses |
| pg BUG-2b | `task.continue` ungated | ✅ `NoDeadDispatch` | ❌ |
| pg BUG-3 | `resonate:target = ''` dispatched once, never redelivered | ❌ **not modelled** — target is a tag, not a string | ❌ |
| pg BUG-4 | `task.halt` 200 on a task `task.get` calls fulfilled | ✅ `NoHaltOnDead` | ❌ |
| pg BUG-5 | timeout handlers redispatch a dead workflow | ✅ `NoDeadDispatch` | ❌ |
| cross-cutting | the resume gap all three models share | ✅ `NoDeadDispatch` | ⚠️ as convex BUG-1 |
| cross-cutting | waiters on internal promises | ✅ `ObligationsAreDischargeable` | ✅ 4 traces refuse |

## The two answers

**Properties: yes, with three exceptions.** Ten of thirteen model-checkable
defect classes are caught by a named property, each in 3-7 states, and **no
mutation survives**. The three exceptions were never model-detectable in any
version of this work — they came from code analysis, and two of them
(`resonate:delay`, `resonate:target = ''`) are about tag VALUES the model
abstracts away, while the third is about authentication rather than protocol
state.

**Detection: no — and this got worse, not better.** Only two classes are
actually caught in an implementation today, both by trace refusal. The rest
have the property but nothing to point it at, because the 14 recorded traces
are scripted happy paths that never build the required conditions.

## The honest cost of removing the profiles

The old `MC_pg.cfg` model-checked resonate-pg's *semantics* exhaustively and
found `NoHaltOnDead` across the whole reachable space, with no trace needed.
Nothing does that now: the model has no way to express "resonate-pg's
semantics", by design.

That trade was right, because the failure mode it removed is worse — a
profile masks a bug exactly when the switch IS the property, which is how
`CallbackExternalGuard = FALSE` hid the waiter divergence in both convex and
resonate. But it is a trade, not a free win, and the row of ❌ in the last
column is what it cost.

## What would close the gap

In order of value:

1. **Traces that exercise the bug conditions.** The scenarios were written to
   demonstrate features, not to reach dead-promise interleavings. A scenario
   that lets a promise expire and then calls `task.halt` would turn pg BUG-4
   from "property exists" into "detected".
2. **Harnesses that record responses.** Three of the ✅-property rows are
   response-level and no harness records what an RPC returned, so they can
   never be trace-detected as things stand.
3. **A fencing mutation.** No mutation here targets `AtMostOneValidClaim`,
   the one property nothing else in this family can even state.
