package model

import "context"

// Internal steps, ported from spec/02-abstract/internal-steps.lean.
//
// The machine's entire internal life is five guarded steps here, fired by
// the environment in any order, at any pace, any number of times. Every
// step is total: if its guard does not hold it is a no-op, so a stale or
// spurious firing is harmless. That totality is what lets the closure
// below fire steps speculatively without checking anything first.
//
// R2 is absent because it is absent from the spec: fact T is fact P seen
// through the task, and `SetSettled` makes both true in one write, so a
// separate fulfillment step would be the identity on every reachable
// state.
//
// R7 (processSchedule) is absent for a different reason: `nextCron` and
// `occurrences` are `opaque` in the Lean with no value, so there is
// nothing to port. Traces that mention schedules are rejected by the
// loader.

// R1 processPromiseTimeout — materialize fact P.
func (s *ServerState) ProcessPromiseTimeout(id string, now uint64) {
	s.readPromise(Materialized, id, now)
}

// R3 processListener — deliver a chosen listener of a settled promise its unblock.
func (s *ServerState) ProcessListener(id, address string, now uint64) {
	p := s.readPromise(Materialized, id, now)
	if p == nil || p.State == Pending || !contains(p.Listeners, address) {
		return
	}
	q := p.clone()
	q.Listeners = remove(q.Listeners, address)
	s.SetPromise(q)
	s.SetMessage(Message{Address: address, Kind: "unblock", Promise: p.ID})
}

// resumeOne wakes one awaiter. The touch materializes the awaiter's OWN
// deadline first, so TIMEOUT ALWAYS WINS falls out with no explicit guard:
// an awaiter past its own deadline reads `.fulfilled` and is dropped.
func (s *ServerState) resumeOne(awaited, awaiter string, now uint64) {
	t, p := s.readTask(Materialized, awaiter, now)
	if t == nil || p == nil {
		return
	}
	switch t.State {
	case TaskSuspended:
		u := t.clone()
		u.State = TaskPending
		u.Resumes = []string{awaited}
		u.RetryAt = u64p(now)
		s.SetTask(u)
	case TaskPending, TaskAcquired, TaskHalted:
		if !contains(t.Resumes, awaited) {
			u := t.clone()
			u.Resumes = append(u.Resumes, awaited)
			s.SetTask(u)
		}
	case TaskFulfilled:
	}
}

// R4 processCallback — wake a chosen awaiter of a settled promise.
//
// THIS IS THE NONDETERMINISM. The abstract machine has no deferred queue:
// `promiseSettle` writes the promise and nothing else, and the awaiter
// stays recorded on it until this rule fires. Nothing an observer can see
// says whether it has fired yet, so the model must carry both answers.
func (s *ServerState) ProcessCallback(id, awaiter string, now uint64) {
	p := s.readPromise(Materialized, id, now)
	if p == nil || p.State == Pending || !contains(p.Callbacks, awaiter) {
		return
	}
	q := p.clone()
	q.Callbacks = remove(q.Callbacks, awaiter)
	s.SetPromise(q)
	s.resumeOne(p.ID, awaiter, now)
}

// R5 processLeaseTimeout — an acquired task past its lease returns to pending.
// The TASK is read raw (expiry is a choice, not a fact, and must never be
// forced by observation) but the DECISION consults the promise through the
// view: no rule creates new work for a logically dead task.
func (s *ServerState) ProcessLeaseTimeout(id string, now uint64) {
	t := s.GetTask(id)
	if t == nil || t.ExpiresAt == nil {
		return
	}
	if t.State != TaskAcquired || *t.ExpiresAt > now {
		return
	}
	p := s.readPromise(Projected, t.ID, now)
	if p == nil || p.State != Pending {
		return
	}
	u := t.clone()
	u.State = TaskPending
	u.PID, u.TTL, u.ExpiresAt = nil, nil, nil
	u.RetryAt = u64p(now)
	s.SetTask(u)
}

// R6 processRetryTimeout — emit the execute for a pending task whose dispatch is due,
// re-arming `retryAt` at a chosen instant. Repeatable: the outbox's keyed
// upsert makes re-emission idempotent, so any `next` is sound.
func (s *ServerState) ProcessRetryTimeout(id string, next, now uint64) {
	t := s.GetTask(id)
	if t == nil || t.RetryAt == nil {
		return
	}
	if t.State != TaskPending || *t.RetryAt > now {
		return
	}
	p := s.readPromise(Projected, t.ID, now)
	if p == nil || p.State != Pending {
		return
	}
	u := t.clone()
	u.RetryAt = u64p(next)
	s.SetTask(u)
	addr, _ := p.Tags.Get("resonate:target")
	s.SetMessage(Message{Address: addr, Kind: "execute", TaskID: t.ID, Version: t.Version})
}

// ------------------------------------------------------------- the closure

// rule is one firing, named so a witness can be printed.
type rule struct {
	name string
	fire func(*ServerState)
}

// enabledRules is every rule firing that could CHANGE the state at `now`.
//
// Rules are total, so firing a disabled one is harmless — but generating
// only the ones whose guard holds is what keeps the closure finite and
// small.
//
// R6's `next` is the scheduler's cadence choice, so the specification
// admits every value of it; ONE representative is fired here, `next =
// now`. That is sound rather than convenient: `next` is written to
// `retryAt`, `TaskRecord` is `{id, state, version, resumes, ttl, pid}`
// (`TaskObject.toRecord` in spec/02-abstract/state.lean) and carries no
// `retryAt`, and no
// handler returns the outbox — so the states a different `next` would
// reach differ only in fields no response can report. Firing `next = now`
// also keeps R6 enabled at every later instant, which is the permissive
// choice: it never removes a firing a larger `next` would have allowed.
func enabledRules(s *ServerState, now uint64) []rule {
	var rs []rule
	for _, p := range s.Promises {
		p := p
		if p.State == Pending && p.TimeoutAt <= now {
			rs = append(rs, rule{"R1 processPromiseTimeout " + p.ID,
				func(t *ServerState) { t.ProcessPromiseTimeout(p.ID, now) }})
			continue // its callbacks cannot drain until it is settled
		}
		if p.State != Pending {
			for _, a := range p.Callbacks {
				a := a
				rs = append(rs, rule{"R4 processCallback " + p.ID + " -> " + a,
					func(t *ServerState) { t.ProcessCallback(p.ID, a, now) }})
			}
			for _, l := range p.Listeners {
				l := l
				rs = append(rs, rule{"R3 processListener " + p.ID + " -> " + l,
					func(t *ServerState) { t.ProcessListener(p.ID, l, now) }})
			}
		}
	}
	for _, t := range s.Tasks {
		t := t
		// No new work for the dead — and nothing to fulfil by rule: a
		// settled promise fulfils its task in the same write, and a
		// fact-lagged one is R1's to settle, task included.
		if p := s.GetPromise(t.ID); p == nil || p.Project(now).State != Pending {
			continue
		}
		if t.State == TaskAcquired && t.ExpiresAt != nil && *t.ExpiresAt <= now {
			rs = append(rs, rule{"R5 processLeaseTimeout " + t.ID,
				func(u *ServerState) { u.ProcessLeaseTimeout(t.ID, now) }})
		}
		if t.State == TaskPending && t.RetryAt != nil && *t.RetryAt <= now {
			rs = append(rs, rule{"R6 processRetryTimeout " + t.ID,
				func(u *ServerState) { u.ProcessRetryTimeout(t.ID, now, now) }})
		}
	}
	return rs
}

// ------------------------------------------------------- cone of influence
//
// The closure explores every subset of the enabled rules, which is 2^n and
// is the only thing that actually limits this checker — a 78-event
// single-origin workload trace (the canonical W3 tree) does not finish.
// Most of those subsets are irrelevant: a lease expiry on task `a5` cannot
// change the response to `promiseGet a0`.
//
// So, ported from valid/lean/validator.lean (`touches`/`affects`/
// `relevantTaus`): at each observed event, fire only the rules whose
// affected objects the event's request also reaches, transitively. The
// rest are NOT discarded — enabledness lives in the state, not in the
// closure, so a deferred rule stays armed and is explored at the first
// later event that names its objects. Independent steps commute
// (partial-order reduction), so firing them later is a canonical choice
// rather than a lost one.
//
// Soundness is for the RESPONSE channel, which is all this checker
// compares. Two rules of the abstract machine touch ONLY state no
// response projects and so carry an empty affected set, dropping out of
// the cone entirely:
//
//   R3 notify   removes a listener and writes the outbox — listener lists
//               and outbox appear in no response record, and a listener
//               feeds no rule but R3 itself;
//   R6 dispatch re-arms `retryAt` and writes the outbox — `TaskRecord`
//               carries no retryAt, and retryAt feeds no rule but R6.
//
// R6 was the one rule that discharges no obligation (the closure's
// documented fanout source), so excluding it is most of the win. R1's
// affected set must include the promise's CALLBACKS, not just its own id:
// settling is what arms R4 for each awaiter, and R4 is not yet enabled
// when the cone is computed — the same chain that made the Lean cone
// unsound the first time (its `affects` doc tells the story).

// touches is what a request reads — the seed of the cone. Mirrors the
// Lean `touches` exactly; searches read no single object and seed nothing.
func touches(o Op) []string {
	switch o.Kind {
	case "promise.register_callback":
		return []string{o.ID, o.Awaiter}
	case "task.suspend":
		return append([]string{o.ID}, o.Awaited...)
	case "task.create":
		if o.Action != nil {
			return []string{o.Action.ID}
		}
		return nil
	case "task.fence":
		if o.Action != nil {
			return []string{o.ID, o.Action.ID}
		}
		return []string{o.ID}
	case "task.heartbeat":
		ids := make([]string, 0, len(o.Refs))
		for _, r := range o.Refs {
			ids = append(ids, r.ID)
		}
		return ids
	case "promise.search", "task.search", "schedule.search":
		return nil
	default:
		return []string{o.ID}
	}
}

// firing is an enabled rule together with what it can affect on the
// response channel. An empty affects means no response can ever tell
// whether it fired.
type firing struct {
	rule
	affects []string
}

// enabledFirings is enabledRules with each rule's affected set attached.
func enabledFirings(s *ServerState, now uint64) []firing {
	var fs []firing
	for _, p := range s.Promises {
		p := p
		if p.State == Pending && p.TimeoutAt <= now {
			fs = append(fs, firing{rule{"R1 processPromiseTimeout " + p.ID,
				func(t *ServerState) { t.ProcessPromiseTimeout(p.ID, now) }},
				append(append([]string{}, p.Callbacks...), p.ID)})
			continue
		}
		if p.State != Pending {
			for _, a := range p.Callbacks {
				a := a
				fs = append(fs, firing{rule{"R4 processCallback " + p.ID + " -> " + a,
					func(t *ServerState) { t.ProcessCallback(p.ID, a, now) }},
					[]string{p.ID, a}})
			}
			for _, l := range p.Listeners {
				l := l
				fs = append(fs, firing{rule{"R3 processListener " + p.ID + " -> " + l,
					func(t *ServerState) { t.ProcessListener(p.ID, l, now) }},
					nil})
			}
		}
	}
	for _, t := range s.Tasks {
		t := t
		if p := s.GetPromise(t.ID); p == nil || p.Project(now).State != Pending {
			continue
		}
		if t.State == TaskAcquired && t.ExpiresAt != nil && *t.ExpiresAt <= now {
			fs = append(fs, firing{rule{"R5 processLeaseTimeout " + t.ID,
				func(u *ServerState) { u.ProcessLeaseTimeout(t.ID, now) }},
				[]string{t.ID}})
		}
		if t.State == TaskPending && t.RetryAt != nil && *t.RetryAt <= now {
			fs = append(fs, firing{rule{"R6 processRetryTimeout " + t.ID,
				func(u *ServerState) { u.ProcessRetryTimeout(t.ID, now, now) }},
				nil})
		}
	}
	return fs
}

// relevantRules is the cone: the enabled rules whose affected sets meet the
// transitive closure of what `op` touches. Mirrors the Lean `relevantTaus`
// — grow the wanted set through the enabled rules' affected sets to a
// fixpoint (fuelled), then filter.
func relevantRules(op Op) func(*ServerState, uint64) []rule {
	return func(s *ServerState, now uint64) []rule {
		enabled := enabledFirings(s, now)
		want := map[string]bool{}
		for _, id := range touches(op) {
			want[id] = true
		}
		for i := 0; i < 32; i++ {
			grew := false
			for _, f := range enabled {
				hit := false
				for _, id := range f.affects {
					if want[id] {
						hit = true
						break
					}
				}
				if !hit {
					continue
				}
				for _, id := range f.affects {
					if !want[id] {
						want[id] = true
						grew = true
					}
				}
			}
			if !grew {
				break
			}
		}
		var rs []rule
		for _, f := range enabled {
			for _, id := range f.affects {
				if want[id] {
					rs = append(rs, f.rule)
					break
				}
			}
		}
		return rs
	}
}

// candidate is a state, the rule firings that reached it, and its
// canonical key CACHED.
//
// The key is what every dedup compares, and computing it walks every
// promise, task and outbox entry to build a string. Recomputing it per
// comparison makes dedup quadratic in string work rather than in integer
// work. The Lean side has always cached it — `Cand.key` in
// valid/lean/validator.lean is filled once by `mkCand` — and this port dropped
// that when it was written.
type candidate struct {
	state   *ServerState
	witness []string
	key     string
}

func newCand(s *ServerState, w []string) candidate {
	return candidate{state: s, witness: w, key: s.Key()}
}

// Cone toggles the cone-of-influence reduction. On by default; `-cone=false`
// on the CLIs runs the full closure, and the fuzzer checks the two agree.
var Cone = true

// pickFor is the closure's rule selector for one observed op: the cone
// when enabled, every enabled rule otherwise.
func pickFor(op Op) func(*ServerState, uint64) []rule {
	if Cone {
		return relevantRules(op)
	}
	return enabledRules
}

// closure is every state reachable from `cs` by firing rules `pick` allows
// at `now`. This is the subset construction: since no observer can tell
// which rules have fired, the model carries all of them.
//
// Terminates because every rule that changes the state discharges an
// obligation — settles a pending promise, drains a callback or listener,
// fulfils a task, expires a lease — and R6 is the one that does not, so it
// is capped by dedup on the canonical key rather than by a measure.
func closure(ctx context.Context, cs []candidate, now uint64, fuel int, pick func(*ServerState, uint64) []rule) ([]candidate, bool) {
	seen := map[string]bool{}
	out := make([]candidate, 0, len(cs))
	frontier := make([]candidate, 0, len(cs))
	for _, c := range cs {
		if !seen[c.key] {
			seen[c.key] = true
			out = append(out, c)
			frontier = append(frontier, c)
		}
	}
	for i := 0; i < fuel; i++ {
		// The deadline is checked HERE, not only by porcupine between
		// steps. porcupine can only interrupt between calls into the
		// model, so without this a single expensive closure runs to
		// completion however long that takes — which is how a 180s budget
		// became a 6m25s run.
		if ctx != nil && ctx.Err() != nil {
			return out, false
		}
		var next []candidate
		for _, c := range frontier {
			for _, r := range pick(c.state, now) {
				t := c.state.clone()
				r.fire(t)
				k := t.Key()
				if seen[k] {
					continue
				}
				seen[k] = true
				w := append(append([]string(nil), c.witness...), r.name)
				cand := candidate{state: t, witness: w, key: k}
				out = append(out, cand)
				next = append(next, cand)
			}
		}
		if len(next) == 0 {
			return out, true // saturated
		}
		frontier = next
	}
	return out, false // fuel exhausted; the caller must not conclude anything
}
