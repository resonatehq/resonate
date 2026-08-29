package model

import (
	"encoding/json"
	"strings"
)

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
// Response carries the RECORDS a handler returns, not just a status.
//
// The Lean checker decodes full `PromiseRecord`/`TaskRecord` values and
// compares them, so a Go response that carried only a status and a state
// would be checking strictly less than its counterpart — and a
// differential test between the two would pass on differences neither
// could see. Records it is.
type Response struct {
	Status  int
	Promise *PromiseRecord
	Task    *TaskRecord
	// Inner is the nested response a `task.fence` carries. A fence that
	// succeeds around a create that 404s is 200 outside and 404 inside,
	// and both are compared.
	Inner *InnerResponse
}

func promiseRes(status int, p *PromiseRecord) Response { return Response{Status: status, Promise: p} }

// ------------------------------------------------------------------ promises

func (s *ServerState) PromiseGet(d Discipline, id string, now uint64) Response {
	o := s.readObject(d, id, now)
	if o == nil {
		return Response{Status: 404}
	}
	return Response{Status: 200, Promise: o.Promise.Record(o.ID)}
}

type PromiseCreateReq struct {
	ID        string
	TimeoutAt uint64
	Tags      Tags
	Param     json.RawMessage
}

func (s *ServerState) PromiseCreate(d Discipline, req PromiseCreateReq, now uint64) Response {
	if req.Tags.TimerTargeted() {
		return Response{Status: 400}
	}
	if o := s.readObject(d, req.ID, now); o != nil {
		return Response{Status: 200, Promise: o.Promise.Record(o.ID)}
	}
	if req.TimeoutAt > now {
		p := &Promise{State: Pending, Tags: req.Tags,
			TimeoutAt: req.TimeoutAt, CreatedAt: now, Param: req.Param}
		s.SetPromise(req.ID, p)
		if p.Tags.Has("resonate:target") {
			// The delay tag seeds `retryTimeoutAt`: the first dispatch is due at
			// the delay if it is still ahead, immediately otherwise.
			due := now
			if v, ok := p.Tags.Get("resonate:delay"); ok {
				if n := parseNat(v); n > due {
					due = n
				}
			}
			s.SetTask(req.ID, &Task{State: TaskPending, Version: 0, RetryTimeoutAt: u64p(due)})
		}
		return Response{Status: 200, Promise: p.Record(req.ID)}
	}
	// Born past its deadline: fact P holds at birth, so the promise is
	// written settled and its task (if targeted) fulfilled.
	st := RejectedTimedout
	if req.Tags.IsTimer() {
		st = Resolved
	}
	p := &Promise{State: st, Tags: req.Tags, TimeoutAt: req.TimeoutAt,
		CreatedAt: req.TimeoutAt, SettledAt: u64p(req.TimeoutAt), Param: req.Param}
	s.SetPromise(req.ID, p)
	if p.Tags.Has("resonate:target") {
		s.SetTask(req.ID, &Task{State: TaskFulfilled, Version: 0})
	}
	return Response{Status: 200, Promise: p.Record(req.ID)}
}

func (s *ServerState) PromiseSettle(d Discipline, id string, st PromiseState, val json.RawMessage, now uint64) Response {
	if !st.Settable() {
		return Response{Status: 400}
	}
	o := s.readObject(d, id, now)
	if o == nil {
		return Response{Status: 404}
	}
	if o.Promise.State == Pending {
		q := o.Promise.clone()
		q.State = st
		q.SettledAt = u64p(now)
		q.Value = val
		// The promise and its task pair, coupled — and the task is the one
		// in the row we already read, not one looked up again. The awaiters
		// and listeners stay on the promise for the batch internal steps —
		// that is what makes the model nondeterministic.
		s.SetSettled(o, q)
		return Response{Status: 200, Promise: q.Record(o.ID)}
	}
	return Response{Status: 200, Promise: o.Promise.Record(o.ID)}
}

func (s *ServerState) PromiseRegisterCallback(d Discipline, awaited, awaiter string, now uint64) Response {
	if awaited == awaiter {
		return Response{Status: 400}
	}
	oa := s.readObject(d, awaited, now)
	if oa == nil {
		return Response{Status: 404}
	}
	ow := s.readObject(d, awaiter, now)
	if ow == nil {
		return Response{Status: 422}
	}
	if !ow.Promise.Tags.Has("resonate:target") {
		return Response{Status: 422}
	}
	if !oa.Promise.External() {
		return Response{Status: 422}
	}
	if oa.Promise.State == Pending && ow.Promise.State == Pending {
		s.SetPromise(oa.ID, oa.Promise.AddCallback(awaiter))
	}
	return Response{Status: 200, Promise: oa.Promise.Record(oa.ID)}
}

// ---------------------------------------------------------------------- tasks

func (s *ServerState) TaskGet(d Discipline, id string, now uint64) Response {
	o := s.readTaskObject(d, id, now)
	if o == nil || o.Task == nil {
		return Response{Status: 404}
	}
	return Response{Status: 200, Task: o.Task.Record(o.ID)}
}

func (s *ServerState) TaskAcquire(d Discipline, id string, version uint64, pid string, ttl uint64, now uint64) Response {
	o := s.readTaskObject(d, id, now)
	if o == nil || o.Task == nil {
		return Response{Status: 404}
	}
	t := o.Task
	if t.State != TaskPending || o.Promise.State != Pending || t.Version != version {
		return Response{Status: 409}
	}
	u := t.clone()
	u.State = TaskAcquired
	u.Version = t.Version + 1
	u.TTL, u.PID = u64p(ttl), strp(pid)
	u.LeaseTimeoutAt = u64p(now + ttl)
	u.RetryTimeoutAt, u.Resumes = nil, nil
	s.SetTask(o.ID, u)
	// `taskAcquire` returns BOTH records in the Lean —
	// `{ status, task := some (t.toRecord o.id), promise := some (p.toRecord o.id) }` —
	// and the capture carries both, so both are compared.
	return Response{Status: 200, Task: u.Record(o.ID), Promise: o.Promise.Record(o.ID)}
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
	o := s.readTaskObject(d, id, now)
	if o == nil || o.Task == nil {
		return Response{Status: 404}
	}
	t := o.Task
	if t.State != TaskAcquired || o.Promise.State != Pending || t.Version != version {
		return Response{Status: 409}
	}
	// Pass 1: stop at the first undischargeable waiter.
	anySettled := false
	for _, a := range awaited {
		oa := s.readObject(d, a, now)
		if oa == nil || !oa.Promise.External() {
			return Response{Status: 422}
		}
		if oa.Promise.State != Pending {
			anySettled = true
		}
	}
	if anySettled {
		u := t.clone()
		u.Resumes = nil
		s.SetTask(o.ID, u)
		return Response{Status: 300}
	}
	// Pass 2: park the awaiter on every awaited promise.
	for _, a := range awaited {
		if oa := s.readObject(d, a, now); oa != nil {
			s.SetPromise(oa.ID, oa.Promise.AddCallback(id))
		}
	}
	u := t.clone()
	u.State = TaskSuspended
	u.PID, u.TTL, u.LeaseTimeoutAt, u.RetryTimeoutAt = nil, nil, nil, nil
	u.Resumes = nil
	s.SetTask(o.ID, u)
	return Response{Status: 200}
}

func (s *ServerState) TaskFulfill(d Discipline, id string, version uint64, st PromiseState, val json.RawMessage, now uint64) Response {
	if !st.Settable() {
		return Response{Status: 400}
	}
	o := s.readTaskObject(d, id, now)
	if o == nil || o.Task == nil {
		return Response{Status: 404}
	}
	if o.Task.State != TaskAcquired || o.Promise.State != Pending ||
		o.Task.Version != version {
		return Response{Status: 409}
	}
	q := o.Promise.clone()
	q.State = st
	q.SettledAt = u64p(now)
	q.Value = val
	// The promise and its own task, coupled: this settle fulfils the task
	// it was issued against, in the same step — and they are one row.
	s.SetSettled(o, q)
	return Response{Status: 200, Promise: q.Record(o.ID)}
}

func (s *ServerState) TaskRelease(d Discipline, id string, version uint64, now uint64) Response {
	o := s.readTaskObject(d, id, now)
	if o == nil || o.Task == nil {
		return Response{Status: 404}
	}
	t := o.Task
	if t.State != TaskAcquired || o.Promise.State != Pending || t.Version != version {
		return Response{Status: 409}
	}
	u := t.clone()
	u.State = TaskPending
	u.PID, u.TTL, u.LeaseTimeoutAt = nil, nil, nil
	u.RetryTimeoutAt = u64p(now)
	s.SetTask(o.ID, u)
	return Response{Status: 200}
}

// TaskRef is one (id, version) a heartbeat names.
type TaskRef struct {
	ID      string
	Version uint64
}

// TaskHeartbeat is T-05, from `taskHeartbeat` in
// spec/02-abstract/external-steps-p.lean.
//
// It extends the lease of the REFERENCED tasks only, and only when the
// version matches — `heartbeatOne` checks
// `t.version == ref.version ∧ t.pid == some pid`.
//
// An earlier version here swept every acquired task belonging to the pid
// and ignored both the request's task list and the version. That is
// invisible on the response channel, which always answers 200, but it
// writes `leaseTimeoutAt` on tasks the request never named — and `leaseTimeoutAt`
// is what R5 `leaseExpiry` guards on. A checker built on it would accept
// runs the specification forbids.
func (s *ServerState) TaskHeartbeat(d Discipline, pid string, refs []TaskRef, now uint64) Response {
	for _, ref := range refs {
		o := s.readTaskObject(d, ref.ID, now)
		if o == nil || o.Task == nil {
			continue
		}
		t := o.Task
		if t.State == TaskAcquired && t.Version == ref.Version &&
			t.PID != nil && *t.PID == pid && o.Promise.State == Pending {
			u := t.clone()
			ttl := uint64(0)
			if t.TTL != nil {
				ttl = *t.TTL
			}
			u.LeaseTimeoutAt = u64p(now + ttl)
			s.SetTask(o.ID, u)
		}
	}
	return Response{Status: 200}
}

// The three search handlers are `501` in the specification — P-06, T-11
// and S-04 are all "not yet specified". Modelled so that a trace
// containing one is CHECKED rather than rejected by the loader: a 501 is
// a response like any other, and a server answering something else to a
// search is a divergence worth catching.
func (s *ServerState) Search() Response { return Response{Status: 501} }

func parseNat(s string) uint64 {
	var n uint64
	for _, c := range s {
		n = n*10 + uint64(c-'0')
	}
	return n
}

// PromiseRegisterListener subscribes an address for an `unblock` when the
// promise settles. External promises only — a waiter may attach only where
// an armed timeout guarantees discharge.
//
// Ported for the fuzzer: nothing in the recorded captures registers a
// listener, so R3 `notify` was unreachable and therefore untested.
func (s *ServerState) PromiseRegisterListener(d Discipline, awaited, address string, now uint64) Response {
	if !addressValid(address) {
		return Response{Status: 400}
	}
	oa := s.readObject(d, awaited, now)
	if oa == nil {
		return Response{Status: 404}
	}
	if !oa.Promise.External() {
		return Response{Status: 422}
	}
	if oa.Promise.State == Pending {
		s.SetPromise(oa.ID, oa.Promise.AddListener(address))
	}
	return Response{Status: 200, Promise: oa.Promise.Record(oa.ID)}
}

// addressValid is `ServerModel.addressValid`.
func addressValid(a string) bool {
	return strings.HasPrefix(a, "http://") || strings.HasPrefix(a, "https://") ||
		(strings.HasPrefix(a, "poll://") && strings.ContainsRune(a, '@'))
}

func (s *ServerState) TaskHalt(d Discipline, id string, now uint64) Response {
	o := s.readTaskObject(d, id, now)
	if o == nil || o.Task == nil {
		return Response{Status: 404}
	}
	t := o.Task
	if t.State == TaskFulfilled {
		return Response{Status: 409}
	}
	if t.State == TaskHalted {
		return Response{Status: 200}
	}
	u := t.clone()
	u.State = TaskHalted
	u.PID, u.TTL, u.LeaseTimeoutAt, u.RetryTimeoutAt = nil, nil, nil, nil
	s.SetTask(o.ID, u)
	return Response{Status: 200}
}

func (s *ServerState) TaskContinue(d Discipline, id string, now uint64) Response {
	o := s.readTaskObject(d, id, now)
	if o == nil || o.Task == nil {
		return Response{Status: 404}
	}
	t := o.Task
	if t.State != TaskHalted {
		return Response{Status: 409}
	}
	if o.Promise.State != Pending {
		return Response{Status: 409}
	}
	u := t.clone()
	u.State = TaskPending
	u.RetryTimeoutAt = u64p(now)
	s.SetTask(o.ID, u)
	return Response{Status: 200}
}

// FenceAction is the create-or-settle a fence carries.
type FenceAction struct {
	Kind      string // "promise.create" | "promise.settle"
	ID        string
	TimeoutAt uint64
	Tags      Tags
	State     PromiseState
	Param     json.RawMessage
	Value     json.RawMessage
}

// TargetID is `TaskFenceAction.targetId` — the promise the action
// operates on, whichever variant it is.
func (a FenceAction) TargetID() string { return a.ID }

// InnerResponse is the nested `{kind, head, data}` a fence returns.
type InnerResponse struct {
	Kind    string
	Status  int
	Promise *PromiseRecord
}

// TaskFence is T-04, transcribed from `taskFence` in
// spec/02-abstract/external-steps-p.lean, guard
// for guard.
//
// The validation guard is FIRST — an action operating on the fencing task
// itself is 400, ahead of existence and version — which is the same
// precedence discipline `precedence_test.go` pins for every other handler.
//
// `preload` is NOT modelled. `TaskFenceRes.preload` defaults to `[]` and
// no handler in the specification ever sets it, while the server populates
// it. That is a genuine gap in the specification, not an oversight here;
// comparing it would refute every fence for a channel the spec does not
// describe.
func (s *ServerState) TaskFence(d Discipline, id string, version uint64, act FenceAction, now uint64) (Response, *InnerResponse) {
	if act.TargetID() == id {
		return Response{Status: 400}, nil
	}
	o := s.readTaskObject(d, id, now)
	if o == nil || o.Task == nil {
		return Response{Status: 404}, nil
	}
	if o.Task.State != TaskAcquired || o.Promise.State != Pending ||
		o.Task.Version != version {
		return Response{Status: 409}, nil
	}
	switch act.Kind {
	case "promise.create":
		r := s.PromiseCreate(d, PromiseCreateReq{act.ID, act.TimeoutAt, act.Tags, act.Param}, now)
		return Response{Status: 200}, &InnerResponse{act.Kind, r.Status, r.Promise}
	case "promise.settle":
		r := s.PromiseSettle(d, act.ID, act.State, act.Value, now)
		return Response{Status: 200}, &InnerResponse{act.Kind, r.Status, r.Promise}
	}
	return Response{Status: 400}, nil
}

// TaskCreate is T-02, transcribed from `taskCreate` in
// spec/02-abstract/external-steps-p.lean.
//
// Two shapes in one handler: create a promise with an immediately-acquired
// task, or re-acquire an existing pending one. The SDK issues it for every
// root workflow, so without it a recorded trace has no promise to explain
// anything against — which is exactly how the first end-to-end run refuted
// at event 0.
//
// The validation guard is first, as everywhere: an action without
// `resonate:target` is 400 before existence is consulted.
func (s *ServerState) TaskCreate(d Discipline, pid string, ttl uint64, act PromiseCreateReq, now uint64) Response {
	if !act.Tags.Has("resonate:target") || act.Tags.TimerTargeted() {
		return Response{Status: 400}
	}
	o := s.readObject(d, act.ID, now)
	if o == nil {
		if act.TimeoutAt > now {
			np := &Promise{State: Pending, Tags: act.Tags,
				TimeoutAt: act.TimeoutAt, CreatedAt: now, Param: act.Param}
			s.SetPromise(act.ID, np)
			nt := &Task{State: TaskAcquired, Version: 1,
				TTL: u64p(ttl), PID: strp(pid), LeaseTimeoutAt: u64p(now + ttl)}
			s.SetTask(act.ID, nt)
			return Response{Status: 200, Task: nt.Record(act.ID), Promise: np.Record(act.ID)}
		}
		// Born past its deadline: fact P holds at birth. Targeted,
		// therefore not a timer, so this is the only verdict here.
		np := &Promise{State: RejectedTimedout, Tags: act.Tags, TimeoutAt: act.TimeoutAt,
			CreatedAt: act.TimeoutAt, SettledAt: u64p(act.TimeoutAt), Param: act.Param}
		s.SetPromise(act.ID, np)
		nt := &Task{State: TaskFulfilled, Version: 0}
		s.SetTask(act.ID, nt)
		return Response{Status: 200, Task: nt.Record(act.ID), Promise: np.Record(act.ID)}
	}
	if !o.Promise.Tags.Has("resonate:target") {
		return Response{Status: 422}
	}
	t := o.Task
	if t == nil {
		return Response{Status: 409}
	}
	switch t.State {
	case TaskFulfilled:
		return Response{Status: 200, Task: t.Record(o.ID), Promise: o.Promise.Record(o.ID)}
	case TaskPending:
		u := t.clone()
		u.State = TaskAcquired
		u.Version = t.Version + 1
		u.TTL, u.PID = u64p(ttl), strp(pid)
		u.LeaseTimeoutAt = u64p(now + ttl)
		u.RetryTimeoutAt, u.Resumes = nil, nil
		s.SetTask(o.ID, u)
		return Response{Status: 200, Task: u.Record(o.ID), Promise: o.Promise.Record(o.ID)}
	default:
		return Response{Status: 409}
	}
}
