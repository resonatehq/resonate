package model

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"
)

type PromiseState int

const (
	Pending PromiseState = iota
	Resolved
	Rejected
	RejectedCanceled
	RejectedTimedout
)

var promiseStateName = map[PromiseState]string{
	Pending: "PENDING", Resolved: "RESOLVED", Rejected: "REJECTED",
	RejectedCanceled: "REJECTED_CANCELED", RejectedTimedout: "REJECTED_TIMEDOUT",
}

func (s PromiseState) String() string { return promiseStateName[s] }

func (s PromiseState) Settable() bool {
	return s == Resolved || s == Rejected || s == RejectedCanceled
}

type TaskState int

const (
	TaskPending TaskState = iota
	TaskAcquired
	TaskSuspended
	TaskHalted
	TaskFulfilled
)

var taskStateName = map[TaskState]string{
	TaskPending: "INIT", TaskAcquired: "CLAIMED", TaskSuspended: "SUSPENDED",
	TaskHalted: "HALTED", TaskFulfilled: "COMPLETED",
}

func (s TaskState) String() string { return taskStateName[s] }

type Tags map[string]string

func (t Tags) Get(k string) (string, bool) { v, ok := t[k]; return v, ok }
func (t Tags) Has(k string) bool           { _, ok := t[k]; return ok }
func (t Tags) IsTimer() bool               { return t["resonate:timer"] == "true" }

func (t Tags) TimerTargeted() bool { return t.IsTimer() && t.Has("resonate:target") }

func (t Tags) key() string {
	ks := make([]string, 0, len(t))
	for k := range t {
		ks = append(ks, k)
	}
	sort.Strings(ks)
	var b strings.Builder
	for _, k := range ks {
		fmt.Fprintf(&b, "%s=%s;", k, t[k])
	}
	return b.String()
}

type Promise struct {
	State     PromiseState
	Tags      Tags
	TimeoutAt uint64
	CreatedAt uint64
	SettledAt *uint64
	Callbacks []string
	Listeners []string

	Param json.RawMessage
	Value json.RawMessage
}

func (p *Promise) clone() *Promise {
	q := *p
	q.Callbacks = append([]string(nil), p.Callbacks...)
	q.Listeners = append([]string(nil), p.Listeners...)
	return &q
}

func (p *Promise) IsTimer() bool { return p.Tags.IsTimer() }

func (p *Promise) External() bool {
	return p.Tags["resonate:external"] == "true" || p.Tags.Has("resonate:target") || p.IsTimer()
}

func (p *Promise) AddCallback(awaiter string) *Promise {
	if contains(p.Callbacks, awaiter) {
		return p
	}
	q := p.clone()
	q.Callbacks = append(q.Callbacks, awaiter)
	return q
}

func (p *Promise) AddListener(addr string) *Promise {
	if contains(p.Listeners, addr) {
		return p
	}
	q := p.clone()
	q.Listeners = append(q.Listeners, addr)
	return q
}

func (p *Promise) Project(now uint64) *Promise {
	if p.State == Pending && p.TimeoutAt <= now {
		q := p.clone()
		at := p.TimeoutAt
		q.SettledAt = &at
		if p.IsTimer() {
			q.State = Resolved
		} else {
			q.State = RejectedTimedout
		}
		return q
	}
	return p
}

type Task struct {
	State          TaskState
	Version        uint64
	TTL            *uint64
	PID            *string
	LeaseTimeoutAt *uint64
	RetryTimeoutAt *uint64
	Resumes        []string
}

func (t *Task) clone() *Task {
	u := *t
	u.Resumes = append([]string(nil), t.Resumes...)
	return &u
}

func (t *Task) Fulfill() *Task {
	u := t.clone()
	u.State = TaskFulfilled
	u.PID, u.TTL, u.LeaseTimeoutAt, u.RetryTimeoutAt = nil, nil, nil, nil
	u.Resumes = nil
	return u
}

func (t *Task) View(p *Promise) *Task {
	if p.State != Pending && t.State != TaskFulfilled {
		return t.Fulfill()
	}
	return t
}

type PromiseRecord struct {
	ID string
	Promise
}

type TaskRecord struct {
	ID string
	Task
}

func (p *Promise) Record(id string) *PromiseRecord {
	if p == nil {
		return nil
	}
	return &PromiseRecord{ID: id, Promise: *p}
}

func (t *Task) Record(id string) *TaskRecord {
	if t == nil {
		return nil
	}
	return &TaskRecord{ID: id, Task: *t}
}

type Message struct {
	Address string
	Kind    string
	TaskID  string
	Version uint64
	Promise string
}

func (m Message) Key() string {
	if m.Kind == "execute" {
		return m.TaskID
	}
	return fmt.Sprintf("%s:notify:%s", m.Promise, m.Address)
}

type Object struct {
	ID      string
	Promise *Promise
	Task    *Task
}

type ServerState struct {
	Objects []*Object
	Outbox  []Message
}

func (s *ServerState) clone() *ServerState {
	t := &ServerState{
		Objects: make([]*Object, len(s.Objects)),
		Outbox:  append([]Message(nil), s.Outbox...),
	}
	copy(t.Objects, s.Objects)
	return t
}

func (s *ServerState) GetObject(id string) *Object {
	for _, o := range s.Objects {
		if o.ID == id {
			return o
		}
	}
	return nil
}

func (s *ServerState) GetPromise(id string) *Promise {
	if o := s.GetObject(id); o != nil {
		return o.Promise
	}
	return nil
}

func (s *ServerState) SetSettled(o *Object, p *Promise) {
	s.SetPromise(o.ID, p)
	if p.State == Pending {
		return
	}
	if o.Task != nil && o.Task.State != TaskFulfilled {
		s.SetTask(o.ID, o.Task.Fulfill())
	}
}

func (s *ServerState) SetPromise(id string, p *Promise) {
	for i, o := range s.Objects {
		if o.ID == id {
			s.Objects[i] = &Object{ID: id, Promise: p, Task: o.Task}
			return
		}
	}
	s.Objects = append(s.Objects, &Object{ID: id, Promise: p})
}

func (s *ServerState) SetTask(id string, t *Task) {
	for i, o := range s.Objects {
		if o.ID == id {
			s.Objects[i] = &Object{ID: id, Promise: o.Promise, Task: t}
			return
		}
	}
}

func (s *ServerState) GetTask(id string) *Task {
	if o := s.GetObject(id); o != nil {
		return o.Task
	}
	return nil
}

func (s *ServerState) SetMessage(m Message) {
	k := m.Key()
	for i, n := range s.Outbox {
		if n.Key() == k {
			s.Outbox[i] = m
			return
		}
	}
	s.Outbox = append(s.Outbox, m)
}

type Discipline int

const (
	Projected Discipline = iota

	Materialized
)

func (d Discipline) String() string {
	if d == Projected {
		return "-p (projected)"
	}
	return "-m (materialized)"
}

func (o *Object) Project(now uint64) *Object {
	p := o.Promise.Project(now)
	var t *Task
	if o.Task != nil {
		t = o.Task.View(p)
	}
	return &Object{ID: o.ID, Promise: p, Task: t}
}

func (s *ServerState) readObject(d Discipline, id string, now uint64) *Object {
	o := s.GetObject(id)
	if o == nil {
		return nil
	}
	u := o.Project(now)
	if d == Materialized {
		if u.Promise.State != o.Promise.State {
			s.SetPromise(id, u.Promise)
		}
		if o.Task != nil && u.Task != nil && u.Task.State != o.Task.State {
			s.SetTask(id, u.Task)
		}
	}
	return u
}

func (s *ServerState) readTaskObject(d Discipline, id string, now uint64) *Object {
	o := s.GetObject(id)
	if o == nil || o.Task == nil {
		return nil
	}
	return s.readObject(d, id, now)
}

func (s *ServerState) Key() string {
	var b strings.Builder
	os_ := append([]*Object(nil), s.Objects...)
	sort.Slice(os_, func(i, j int) bool { return os_[i].ID < os_[j].ID })
	for _, o := range os_ {
		p := o.Promise
		cb := append([]string(nil), p.Callbacks...)
		ls := append([]string(nil), p.Listeners...)
		sort.Strings(cb)
		sort.Strings(ls)

		fmt.Fprintf(&b, "P|%s|%d|%d|%d|%s|%s|%s|%s|%s|%s\n",
			o.ID, p.State, p.TimeoutAt, p.CreatedAt, u64s(p.SettledAt),
			strings.Join(cb, ","), strings.Join(ls, ","), p.Tags.key(),
			canonValue(p.Param), canonValue(p.Value))
	}
	for _, o := range os_ {
		t := o.Task
		if t == nil {
			continue
		}
		rs := append([]string(nil), t.Resumes...)
		sort.Strings(rs)
		fmt.Fprintf(&b, "T|%s|%d|%d|%s|%s|%s|%s|%s\n",
			o.ID, t.State, t.Version, u64s(t.TTL), strs(t.PID),
			u64s(t.LeaseTimeoutAt), u64s(t.RetryTimeoutAt), strings.Join(rs, ","))
	}
	ob := make([]string, 0, len(s.Outbox))
	for _, m := range s.Outbox {
		ob = append(ob, fmt.Sprintf("O|%s|%s|%s|%d|%s", m.Address, m.Kind, m.TaskID, m.Version, m.Promise))
	}
	sort.Strings(ob)
	b.WriteString(strings.Join(ob, "\n"))
	return b.String()
}

func contains(xs []string, x string) bool {
	for _, y := range xs {
		if y == x {
			return true
		}
	}
	return false
}

func remove(xs []string, x string) []string {
	out := make([]string, 0, len(xs))
	for _, y := range xs {
		if y != x {
			out = append(out, y)
		}
	}
	return out
}

func u64p(v uint64) *uint64 { return &v }
func strp(v string) *string { return &v }

func u64s(p *uint64) string {
	if p == nil {
		return "-"
	}
	return fmt.Sprint(*p)
}

func strs(p *string) string {
	if p == nil {
		return "-"
	}
	return *p
}
