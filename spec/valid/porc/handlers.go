package model

import (
	"encoding/json"
)

type Response struct {
	Status  int
	Promise *PromiseRecord
	Task    *TaskRecord

	Inner *InnerResponse
}

func promiseRes(status int, p *PromiseRecord) Response { return Response{Status: status, Promise: p} }

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
	if req.Tags.TimerTargeted() || !originTagOK(req.ID, req.Tags) {
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

		s.SetSettled(o, q)
		return Response{Status: 200, Promise: q.Record(o.ID)}
	}
	return Response{Status: 200, Promise: o.Promise.Record(o.ID)}
}

func (s *ServerState) PromiseRegisterCallback(d Discipline, awaited, awaiter string, now uint64) Response {
	if awaited == awaiter || !sameOrigin(awaited, awaiter) {
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
	if !oa.Promise.Awaitable() {
		return Response{Status: 422}
	}
	if oa.Promise.State == Pending && ow.Promise.State == Pending {
		s.SetPromise(oa.ID, oa.Promise.AddCallback(awaiter))
	}
	return Response{Status: 200, Promise: oa.Promise.Record(oa.ID)}
}

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

	return Response{Status: 200, Task: u.Record(o.ID), Promise: o.Promise.Record(o.ID)}
}

func (s *ServerState) TaskSuspend(d Discipline, id string, version uint64, awaited []string, now uint64) Response {
	if len(awaited) == 0 {
		return Response{Status: 400}
	}
	seen := map[string]bool{}
	for _, a := range awaited {
		if a == id || seen[a] || !sameOrigin(a, id) {
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

	anySettled := false
	for _, a := range awaited {
		oa := s.readObject(d, a, now)
		if oa == nil || !oa.Promise.Awaitable() {
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

type TaskRef struct {
	ID      string
	Version uint64
}

func (s *ServerState) TaskHeartbeat(d Discipline, pid string, refs []TaskRef, now uint64) Response {
	for _, ref := range refs {
		if !sameOrigin(ref.ID, refs[0].ID) {
			return Response{Status: 400}
		}
	}
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

func (s *ServerState) Search() Response { return Response{Status: 501} }

func parseNat(s string) uint64 {
	var n uint64
	for _, c := range s {
		n = n*10 + uint64(c-'0')
	}
	return n
}

func (s *ServerState) PromiseRegisterListener(d Discipline, awaited, address string, now uint64) Response {
	oa := s.readObject(d, awaited, now)
	if oa == nil {
		return Response{Status: 404}
	}
	if !oa.Promise.Awaitable() {
		return Response{Status: 422}
	}
	if oa.Promise.State == Pending {
		s.SetPromise(oa.ID, oa.Promise.AddListener(address))
	}
	return Response{Status: 200, Promise: oa.Promise.Record(oa.ID)}
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

type FenceAction struct {
	Kind      string
	ID        string
	TimeoutAt uint64
	Tags      Tags
	State     PromiseState
	Param     json.RawMessage
	Value     json.RawMessage
}

func (a FenceAction) TargetID() string { return a.ID }

type InnerResponse struct {
	Kind    string
	Status  int
	Promise *PromiseRecord
}

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

func (s *ServerState) TaskCreate(d Discipline, pid string, ttl uint64, act PromiseCreateReq, now uint64) Response {
	if !act.Tags.Has("resonate:target") || act.Tags.TimerTargeted() || !originTagOK(act.ID, act.Tags) {
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
