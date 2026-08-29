package model

import "context"

func (s *ServerState) ProcessPromiseTimeout(id string, now uint64) {
	s.readObject(Materialized, id, now)
}

func (s *ServerState) ProcessListener(id, address string, now uint64) {
	o := s.readObject(Materialized, id, now)
	if o == nil || o.Promise.State == Pending || !contains(o.Promise.Listeners, address) {
		return
	}
	q := o.Promise.clone()
	q.Listeners = remove(q.Listeners, address)
	s.SetPromise(o.ID, q)
	s.SetMessage(Message{Address: address, Kind: "unblock", Promise: o.ID})
}

func (s *ServerState) resumeOne(awaited, awaiter string, now uint64) {
	o := s.readTaskObject(Materialized, awaiter, now)
	if o == nil || o.Task == nil {
		return
	}
	t := o.Task
	switch t.State {
	case TaskSuspended:
		u := t.clone()
		u.State = TaskPending
		u.Resumes = []string{awaited}
		u.RetryTimeoutAt = u64p(now)
		s.SetTask(o.ID, u)
	case TaskPending, TaskAcquired, TaskHalted:
		if !contains(t.Resumes, awaited) {
			u := t.clone()
			u.Resumes = append(u.Resumes, awaited)
			s.SetTask(o.ID, u)
		}
	case TaskFulfilled:
	}
}

func (s *ServerState) ProcessCallback(id, awaiter string, now uint64) {
	o := s.readObject(Materialized, id, now)
	if o == nil || o.Promise.State == Pending || !contains(o.Promise.Callbacks, awaiter) {
		return
	}
	q := o.Promise.clone()
	q.Callbacks = remove(q.Callbacks, awaiter)
	s.SetPromise(o.ID, q)
	s.resumeOne(o.ID, awaiter, now)
}

func (s *ServerState) ProcessLeaseTimeout(id string, now uint64) {
	o := s.readTaskObject(Projected, id, now)
	if o == nil || o.Task == nil {
		return
	}
	t := o.Task
	if t.LeaseTimeoutAt == nil || t.State != TaskAcquired || *t.LeaseTimeoutAt > now {
		return
	}
	if o.Promise.State != Pending {
		return
	}
	u := t.clone()
	u.State = TaskPending
	u.PID, u.TTL, u.LeaseTimeoutAt = nil, nil, nil
	u.RetryTimeoutAt = u64p(now)
	s.SetTask(o.ID, u)
}

func (s *ServerState) ProcessRetryTimeout(id string, next, now uint64) {
	o := s.readTaskObject(Projected, id, now)
	if o == nil || o.Task == nil {
		return
	}
	t := o.Task
	if t.RetryTimeoutAt == nil || t.State != TaskPending || *t.RetryTimeoutAt > now {
		return
	}
	if o.Promise.State != Pending {
		return
	}
	u := t.clone()
	u.RetryTimeoutAt = u64p(next)
	s.SetTask(o.ID, u)
	addr, _ := o.Promise.Tags.Get("resonate:target")
	s.SetMessage(Message{Address: addr, Kind: "execute", TaskID: o.ID, Version: t.Version})
}

type internalStep struct {
	name string
	fire func(*ServerState)
}

func enabledInternalSteps(s *ServerState, now uint64) []internalStep {
	var rs []internalStep
	for _, o := range s.Objects {
		o := o
		p := o.Promise
		if p.State == Pending && p.TimeoutAt <= now {
			rs = append(rs, internalStep{"promiseTimeout " + o.ID,
				func(t *ServerState) { t.ProcessPromiseTimeout(o.ID, now) }})
			continue
		}
		if p.State != Pending {
			for _, a := range p.Callbacks {
				a := a
				rs = append(rs, internalStep{"callback " + o.ID + " → " + a,
					func(t *ServerState) { t.ProcessCallback(o.ID, a, now) }})
			}
			for _, l := range p.Listeners {
				l := l
				rs = append(rs, internalStep{"listener " + o.ID + " → " + l,
					func(t *ServerState) { t.ProcessListener(o.ID, l, now) }})
			}
		}
	}
	for _, o := range s.Objects {
		o := o
		t := o.Task

		if t == nil || o.Promise.Project(now).State != Pending {
			continue
		}
		if t.State == TaskAcquired && t.LeaseTimeoutAt != nil && *t.LeaseTimeoutAt <= now {
			rs = append(rs, internalStep{"taskLeaseTimeout " + o.ID,
				func(u *ServerState) { u.ProcessLeaseTimeout(o.ID, now) }})
		}
		if t.State == TaskPending && t.RetryTimeoutAt != nil && *t.RetryTimeoutAt <= now {
			rs = append(rs, internalStep{"taskRetryTimeout " + o.ID,
				func(u *ServerState) { u.ProcessRetryTimeout(o.ID, now, now) }})
		}
	}
	return rs
}

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

type firing struct {
	internalStep
	affects []string
}

func enabledFirings(s *ServerState, now uint64) []firing {
	var fs []firing
	for _, o := range s.Objects {
		o := o
		p := o.Promise
		if p.State == Pending && p.TimeoutAt <= now {
			fs = append(fs, firing{internalStep{"promiseTimeout " + o.ID,
				func(t *ServerState) { t.ProcessPromiseTimeout(o.ID, now) }},
				append(append([]string{}, p.Callbacks...), o.ID)})
			continue
		}
		if p.State != Pending {
			for _, a := range p.Callbacks {
				a := a
				fs = append(fs, firing{internalStep{"callback " + o.ID + " → " + a,
					func(t *ServerState) { t.ProcessCallback(o.ID, a, now) }},
					[]string{o.ID, a}})
			}
			for _, l := range p.Listeners {
				l := l
				fs = append(fs, firing{internalStep{"listener " + o.ID + " → " + l,
					func(t *ServerState) { t.ProcessListener(o.ID, l, now) }},
					nil})
			}
		}
	}
	for _, o := range s.Objects {
		o := o
		t := o.Task
		if t == nil || o.Promise.Project(now).State != Pending {
			continue
		}
		if t.State == TaskAcquired && t.LeaseTimeoutAt != nil && *t.LeaseTimeoutAt <= now {
			fs = append(fs, firing{internalStep{"taskLeaseTimeout " + o.ID,
				func(u *ServerState) { u.ProcessLeaseTimeout(o.ID, now) }},
				[]string{o.ID}})
		}
		if t.State == TaskPending && t.RetryTimeoutAt != nil && *t.RetryTimeoutAt <= now {
			fs = append(fs, firing{internalStep{"taskRetryTimeout " + o.ID,
				func(u *ServerState) { u.ProcessRetryTimeout(o.ID, now, now) }},
				nil})
		}
	}
	return fs
}

func relevantInternalSteps(op Op) func(*ServerState, uint64) []internalStep {
	return func(s *ServerState, now uint64) []internalStep {
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
		var rs []internalStep
		for _, f := range enabled {
			for _, id := range f.affects {
				if want[id] {
					rs = append(rs, f.internalStep)
					break
				}
			}
		}
		return rs
	}
}

type candidate struct {
	state   *ServerState
	witness []string
	key     string
}

func newCand(s *ServerState, w []string) candidate {
	return candidate{state: s, witness: w, key: s.Key()}
}

var Cone = true

func pickFor(op Op) func(*ServerState, uint64) []internalStep {
	if Cone {
		return relevantInternalSteps(op)
	}
	return enabledInternalSteps
}

func closure(ctx context.Context, cs []candidate, now uint64, fuel int, pick func(*ServerState, uint64) []internalStep) ([]candidate, bool) {
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
			return out, true
		}
		frontier = next
	}
	return out, false
}
