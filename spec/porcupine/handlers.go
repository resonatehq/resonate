package model

// Protocol handlers, ported from spec/02-abstract/{p,m}.lean.
//
// ONE copy, taking a `Discipline`, because the two Lean files are the same
// code modulo viewPromise/viewTask vs touchPromise/touchTask — checked by
// diff before writing this. Guards are transcribed in the Lean's order,
// because order is observable: validation errors (400) outrank existence
// (404), which outranks state and version (409).
//
// Each handler mutates the receiver. The caller clones first, so the
// function porcupine sees is pure.

// Response is what a handler returns. Only `Status` and the fields a
// recorded trace actually carries are modelled — the checker compares
// responses, so anything the wire format does not report cannot be
// compared and would be invented detail.
type Response struct {
	Status int
	// PromiseState is set when the response carries a promise record.
	PromiseState *PromiseState
	// TaskState and TaskVersion are set when it carries a task record.
	TaskState   *TaskState
	TaskVersion *uint64
	// SettledAt distinguishes a promise settled at its own deadline from
	// one settled by a client at `now` — the field that caught the
	// mixed-clock capture in the Lean checker.
	SettledAt *uint64
}

// ------------------------------------------------------------------ promises

func (s *ServerState) PromiseGet(d Discipline, id string, now uint64) Response {
	p := s.readPromise(d, id, now)
	if p == nil {
		return Response{Status: 404}
	}
	return Response{Status: 200, PromiseState: &p.State, SettledAt: p.SettledAt}
}

type PromiseCreateReq struct {
	ID        string
	TimeoutAt uint64
	Tags      Tags
}

func (s *ServerState) PromiseCreate(d Discipline, req PromiseCreateReq, now uint64) Response {
	if p := s.readPromise(d, req.ID, now); p != nil {
		return Response{Status: 200, PromiseState: &p.State, SettledAt: p.SettledAt}
	}
	if req.TimeoutAt > now {
		p := &Promise{ID: req.ID, State: Pending, Tags: req.Tags,
			TimeoutAt: req.TimeoutAt, CreatedAt: now}
		s.SetPromise(p)
		if p.Tags.Has("resonate:target") {
			// The delay tag seeds `retryAt`: the first dispatch is due at
			// the delay if it is still ahead, immediately otherwise.
			due := now
			if v, ok := p.Tags.Get("resonate:delay"); ok {
				if n := parseNat(v); n > due {
					due = n
				}
			}
			s.SetTask(&Task{ID: p.ID, State: TaskPending, Version: 0, RetryAt: u64p(due)})
		}
		return Response{Status: 200, PromiseState: &p.State, SettledAt: p.SettledAt}
	}
	// Born past its deadline: fact P holds at birth, so the promise is
	// written settled and its task (if targeted) fulfilled.
	st := RejectedTimedout
	if req.Tags.IsTimer() {
		st = Resolved
	}
	p := &Promise{ID: req.ID, State: st, Tags: req.Tags, TimeoutAt: req.TimeoutAt,
		CreatedAt: req.TimeoutAt, SettledAt: u64p(req.TimeoutAt)}
	s.SetPromise(p)
	if p.Tags.Has("resonate:target") {
		s.SetTask(&Task{ID: p.ID, State: TaskFulfilled, Version: 0})
	}
	return Response{Status: 200, PromiseState: &p.State, SettledAt: p.SettledAt}
}

func (s *ServerState) PromiseSettle(d Discipline, id string, st PromiseState, now uint64) Response {
	if !st.Settable() {
		return Response{Status: 400}
	}
	p := s.readPromise(d, id, now)
	if p == nil {
		return Response{Status: 404}
	}
	if p.State == Pending {
		q := p.clone()
		q.State = st
		q.SettledAt = u64p(now)
		// The promise ONLY. The task is fulfilled by fact T; the awaiters
		// and listeners stay on the promise for the batch rules. This is
		// the line that makes the model nondeterministic.
		s.SetPromise(q)
		return Response{Status: 200, PromiseState: &q.State, SettledAt: q.SettledAt}
	}
	return Response{Status: 200, PromiseState: &p.State, SettledAt: p.SettledAt}
}

func (s *ServerState) PromiseRegisterCallback(d Discipline, awaited, awaiter string, now uint64) Response {
	if awaited == awaiter {
		return Response{Status: 400}
	}
	pa := s.readPromise(d, awaited, now)
	if pa == nil {
		return Response{Status: 404}
	}
	pw := s.readPromise(d, awaiter, now)
	if pw == nil {
		return Response{Status: 422}
	}
	if !pw.Tags.Has("resonate:target") {
		return Response{Status: 422}
	}
	if !pa.External() {
		return Response{Status: 422}
	}
	if pa.State == Pending && pw.State == Pending {
		s.SetPromise(pa.AddCallback(awaiter))
	}
	return Response{Status: 200, PromiseState: &pa.State, SettledAt: pa.SettledAt}
}

// ---------------------------------------------------------------------- tasks

func (s *ServerState) TaskGet(d Discipline, id string, now uint64) Response {
	t, p := s.readTask(d, id, now)
	if t == nil || p == nil {
		return Response{Status: 404}
	}
	v := t.Version
	return Response{Status: 200, TaskState: &t.State, TaskVersion: &v}
}

func (s *ServerState) TaskAcquire(d Discipline, id string, version uint64, pid string, ttl uint64, now uint64) Response {
	t, p := s.readTask(d, id, now)
	if t == nil {
		return Response{Status: 404}
	}
	if p == nil {
		return Response{Status: 409}
	}
	if t.State != TaskPending || p.State != Pending || t.Version != version {
		return Response{Status: 409}
	}
	u := t.clone()
	u.State = TaskAcquired
	u.Version = t.Version + 1
	u.TTL, u.PID = u64p(ttl), strp(pid)
	u.ExpiresAt = u64p(now + ttl)
	u.RetryAt, u.Resumes = nil, nil
	s.SetTask(u)
	v := u.Version
	// `taskAcquire` returns BOTH records in the Lean —
	// `{ status, task := some t.toRecord, promise := some p.toRecord }` —
	// and the capture carries both, so both are compared.
	return Response{Status: 200, TaskState: &u.State, TaskVersion: &v,
		PromiseState: &p.State, SettledAt: p.SettledAt}
}

// TaskSuspend parks an acquired task on awaited promises. `300` if any
// awaited promise is already settled — the client must re-check rather
// than sleep.
func (s *ServerState) TaskSuspend(d Discipline, id string, version uint64, awaited []string, now uint64) Response {
	if len(awaited) == 0 {
		return Response{Status: 400}
	}
	seen := map[string]bool{}
	for _, a := range awaited {
		if a == id || seen[a] {
			return Response{Status: 400}
		}
		seen[a] = true
	}
	t, tp := s.readTask(d, id, now)
	if t == nil {
		return Response{Status: 404}
	}
	if tp == nil {
		return Response{Status: 409}
	}
	if t.State != TaskAcquired || tp.State != Pending || t.Version != version {
		return Response{Status: 409}
	}
	// Pass 1: stop at the first undischargeable waiter.
	anySettled := false
	for _, a := range awaited {
		pa := s.readPromise(d, a, now)
		if pa == nil || !pa.External() {
			return Response{Status: 422}
		}
		if pa.State != Pending {
			anySettled = true
		}
	}
	if anySettled {
		u := t.clone()
		u.Resumes = nil
		s.SetTask(u)
		return Response{Status: 300}
	}
	// Pass 2: park the awaiter on every awaited promise.
	for _, a := range awaited {
		if pa := s.readPromise(d, a, now); pa != nil {
			s.SetPromise(pa.AddCallback(id))
		}
	}
	u := t.clone()
	u.State = TaskSuspended
	u.PID, u.TTL, u.ExpiresAt, u.RetryAt = nil, nil, nil, nil
	u.Resumes = nil
	s.SetTask(u)
	return Response{Status: 200}
}

func (s *ServerState) TaskFulfill(d Discipline, id string, version uint64, st PromiseState, now uint64) Response {
	if !st.Settable() {
		return Response{Status: 400}
	}
	t, p := s.readTask(d, id, now)
	if t == nil {
		return Response{Status: 404}
	}
	if p == nil {
		return Response{Status: 409}
	}
	if t.State != TaskAcquired || p.State != Pending || t.Version != version {
		return Response{Status: 409}
	}
	q := p.clone()
	q.State = st
	q.SettledAt = u64p(now)
	// The promise only — the task is fulfilled by fact T on the next
	// touch, or by R2. Observably indistinguishable, since every task read
	// that could report it goes through the view.
	s.SetPromise(q)
	return Response{Status: 200, PromiseState: &q.State, SettledAt: q.SettledAt}
}

func (s *ServerState) TaskRelease(d Discipline, id string, version uint64, now uint64) Response {
	t, p := s.readTask(d, id, now)
	if t == nil {
		return Response{Status: 404}
	}
	if p == nil {
		return Response{Status: 409}
	}
	if t.State != TaskAcquired || p.State != Pending || t.Version != version {
		return Response{Status: 409}
	}
	u := t.clone()
	u.State = TaskPending
	u.PID, u.TTL, u.ExpiresAt = nil, nil, nil
	u.RetryAt = u64p(now)
	s.SetTask(u)
	return Response{Status: 200}
}

func (s *ServerState) TaskHeartbeat(d Discipline, pid string, now uint64) Response {
	for _, t := range append([]*Task(nil), s.Tasks...) {
		u, p := s.readTask(d, t.ID, now)
		if u == nil || p == nil {
			continue
		}
		if u.State == TaskAcquired && u.PID != nil && *u.PID == pid && p.State == Pending {
			v := u.clone()
			ttl := uint64(0)
			if u.TTL != nil {
				ttl = *u.TTL
			}
			v.ExpiresAt = u64p(now + ttl)
			s.SetTask(v)
		}
	}
	return Response{Status: 200}
}

func parseNat(s string) uint64 {
	var n uint64
	for _, c := range s {
		n = n*10 + uint64(c-'0')
	}
	return n
}
