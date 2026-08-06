package model

// Internal rules, ported from spec/02-abstract/rules.lean.
//
// The machine's entire internal life is seven guarded rules, fired by the
// environment in any order, at any pace, any number of times. Every rule
// is total: if its guard does not hold it is a no-op, so a stale or
// spurious firing is harmless. That totality is what lets the closure
// below fire rules speculatively without checking anything first.
//
// R7 (scheduleFire) is absent: `nextCron` and `occurrences` are `opaque`
// in the Lean with no value, so there is nothing to port. Traces that
// mention schedules are rejected by the loader.

// R1 promiseTimeout — materialize fact P.
func (s *ServerState) RulePromiseTimeout(id string, now uint64) {
	s.readPromise(Materialized, id, now)
}

// R2 taskFulfillment — materialize fact T.
func (s *ServerState) RuleTaskFulfillment(id string, now uint64) {
	s.readTask(Materialized, id, now)
}

// R3 notify — deliver a chosen listener of a settled promise its unblock.
func (s *ServerState) RuleNotify(id, address string, now uint64) {
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

// R4 resume — wake a chosen awaiter of a settled promise.
//
// THIS IS THE NONDETERMINISM. The abstract machine has no deferred queue:
// `promiseSettle` writes the promise and nothing else, and the awaiter
// stays recorded on it until this rule fires. Nothing an observer can see
// says whether it has fired yet, so the model must carry both answers.
func (s *ServerState) RuleResume(id, awaiter string, now uint64) {
	p := s.readPromise(Materialized, id, now)
	if p == nil || p.State == Pending || !contains(p.Callbacks, awaiter) {
		return
	}
	q := p.clone()
	q.Callbacks = remove(q.Callbacks, awaiter)
	s.SetPromise(q)
	s.resumeOne(p.ID, awaiter, now)
}

// R5 leaseExpiry — an acquired task past its lease returns to pending.
// The TASK is read raw (expiry is a choice, not a fact, and must never be
// forced by observation) but the DECISION consults the promise through the
// view: no rule creates new work for a logically dead task.
func (s *ServerState) RuleLeaseExpiry(id string, now uint64) {
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

// R6 dispatch — emit the execute for a pending task whose dispatch is due,
// re-arming `retryAt` at a chosen instant. Repeatable: the outbox's keyed
// upsert makes re-emission idempotent, so any `next` is sound.
func (s *ServerState) RuleDispatch(id string, next, now uint64) {
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
// small. R6's `next` parameter is the scheduler's cadence choice; the two
// representatives below are the only ones that matter, because `next`
// affects nothing until a later instant compares against it, and no
// response projects `retryAt`.
func enabledRules(s *ServerState, now uint64) []rule {
	var rs []rule
	for _, p := range s.Promises {
		p := p
		if p.State == Pending && p.TimeoutAt <= now {
			rs = append(rs, rule{"R1 promiseTimeout " + p.ID,
				func(t *ServerState) { t.RulePromiseTimeout(p.ID, now) }})
			continue // its callbacks cannot drain until it is settled
		}
		if p.State != Pending {
			for _, a := range p.Callbacks {
				a := a
				rs = append(rs, rule{"R4 resume " + p.ID + " -> " + a,
					func(t *ServerState) { t.RuleResume(p.ID, a, now) }})
			}
			for _, l := range p.Listeners {
				l := l
				rs = append(rs, rule{"R3 notify " + p.ID + " -> " + l,
					func(t *ServerState) { t.RuleNotify(p.ID, l, now) }})
			}
		}
	}
	for _, t := range s.Tasks {
		t := t
		if t.State != TaskFulfilled {
			if p := s.GetPromise(t.ID); p != nil && p.Project(now).State != Pending {
				rs = append(rs, rule{"R2 taskFulfillment " + t.ID,
					func(u *ServerState) { u.RuleTaskFulfillment(t.ID, now) }})
				continue
			}
		}
		if t.State == TaskAcquired && t.ExpiresAt != nil && *t.ExpiresAt <= now {
			rs = append(rs, rule{"R5 leaseExpiry " + t.ID,
				func(u *ServerState) { u.RuleLeaseExpiry(t.ID, now) }})
		}
		if t.State == TaskPending && t.RetryAt != nil && *t.RetryAt <= now {
			rs = append(rs, rule{"R6 dispatch " + t.ID,
				func(u *ServerState) { u.RuleDispatch(t.ID, now, now) }})
		}
	}
	return rs
}

// candidate is a state plus the rule firings that reached it.
type candidate struct {
	state   *ServerState
	witness []string
}

// closure is every state reachable from `cs` by firing enabled rules at
// `now`. This is the subset construction: since no observer can tell which
// rules have fired, the model carries all of them.
//
// Terminates because every rule that changes the state discharges an
// obligation — settles a pending promise, drains a callback or listener,
// fulfils a task, expires a lease — and R6 is the one that does not, so it
// is capped by dedup on the canonical key rather than by a measure.
func closure(cs []candidate, now uint64, fuel int) ([]candidate, bool) {
	seen := map[string]bool{}
	out := make([]candidate, 0, len(cs))
	frontier := make([]candidate, 0, len(cs))
	for _, c := range cs {
		k := c.state.Key()
		if !seen[k] {
			seen[k] = true
			out = append(out, c)
			frontier = append(frontier, c)
		}
	}
	for i := 0; i < fuel; i++ {
		var next []candidate
		for _, c := range frontier {
			for _, r := range enabledRules(c.state, now) {
				t := c.state.clone()
				r.fire(t)
				k := t.Key()
				if seen[k] {
					continue
				}
				seen[k] = true
				w := append(append([]string(nil), c.witness...), r.name)
				cand := candidate{t, w}
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
