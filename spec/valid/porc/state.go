// Package model is a Go port of the Resonate abstract machine
// (spec/02-abstract), wired up as a nondeterministic model for the
// porcupine linearizability checker.
//
// The port is deliberately literal. Every function here corresponds to a
// definition in the Lean, named the same, and the guards are transcribed
// in the same order. Where Lean returns `M X` (a state monad) Go takes a
// *ServerState receiver and mutates it — the caller clones first, so the
// model function porcupine sees is still pure.
//
// ONE DIFFERENCE FROM THE LEAN, ON PURPOSE. `spec/02-abstract/state.lean`
// carries ONE row — an `Object` holding a promise and, if it has one, the
// task executing it. This file still carries two slices keyed by the same
// id. The two agree: `cmd/fuzz -lean` runs generated traces and mutants
// past both and requires the same verdict, which is what stops the port
// from drifting. Fusing the row here is a follow-up, not a correction.
//
// Two things the Lean says that this file leans on:
//
//   - The two read disciplines are the SAME CODE modulo `viewObject` vs
//     `touchObject` — in the Lean they are one body and a `mat` bit. So
//     there is one set of handlers here, taking a `Discipline`, rather
//     than two copies.
//
//   - The abstract machine has NO deferred queue. A settled promise keeps
//     its callbacks and listeners until the batch internal steps drain them. That
//     is what makes the model nondeterministic: whether a resume has been
//     drained yet is not determined by the requests, so `Step` has to
//     return every state consistent with some drain schedule.
package model

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"
)

// ---------------------------------------------------------------- wire types

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

// Settable is `PromiseState.settable`: the terminal states a client may
// settle into. `rejectedTimedout` is server-owned — only the timeout path
// writes it — so a client can never forge one.
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

// Tags is `abbrev Tags := List (String × String)`, kept sorted so that
// state equality is structural.
type Tags map[string]string

func (t Tags) Get(k string) (string, bool) { v, ok := t[k]; return v, ok }
func (t Tags) Has(k string) bool           { _, ok := t[k]; return ok }
func (t Tags) IsTimer() bool               { return t["resonate:timer"] == "true" }

// TimerTargeted is the malformed combination: `resonate:timer` says nothing
// executes this promise, `resonate:target` says a worker owns its execution
// and earns it a task. A promise carrying both would be handed a task no
// worker should run. Refused at every creation path — see `Tags.timerTargeted`
// in spec/01-protocol/validation.lean.
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

// ---------------------------------------------------------------- objects

// Promise is `AbstractModel.PromiseObject`. Note `Callbacks` and
// `Listeners`: in this machine they SURVIVE settlement and are drained by
// internal steps R4/R3 later. That is the whole source of nondeterminism.
type Promise struct {
	ID        string
	State     PromiseState
	Tags      Tags
	TimeoutAt uint64
	CreatedAt uint64
	SettledAt *uint64
	Callbacks []string
	Listeners []string
	// Param and Value are carried as raw JSON and never interpreted. The
	// model does not reason about payloads, but the LEAN checker compares
	// full `PromiseRecord`s including them — so dropping them here made
	// re-emitting a recorded trace lossy, and the Lean side refuted a file
	// it had just accepted. Found by running the fuzzer over a real SDK
	// capture.
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

// External is `PromiseObject.external`.
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

// Project is fact P — `PromiseObject.project`. Stamped AT THE DEADLINE,
// not at `now`, so the record a late read produces is byte-identical to
// the one an on-time internal step would have written. Awaiters and listeners are
// untouched: settlement records the fact, the drain internal steps discharge the
// obligations.
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

// Task is `AbstractModel.TaskObject`. `ExpiresAt` and `RetryAt` are the
// deadlines the concrete machine keeps in `taskTimeouts`; here they live
// on the object, so R5 and R6 guard on the task alone.
type Task struct {
	ID        string
	State     TaskState
	Version   uint64
	TTL       *uint64
	PID       *string
	ExpiresAt *uint64
	RetryAt   *uint64
	Resumes   []string
}

func (t *Task) clone() *Task {
	u := *t
	u.Resumes = append([]string(nil), t.Resumes...)
	return &u
}

// Fulfill is fact T's written form — `TaskObject.fulfill`.
func (t *Task) Fulfill() *Task {
	u := t.clone()
	u.State = TaskFulfilled
	u.PID, u.TTL, u.ExpiresAt, u.RetryAt = nil, nil, nil, nil
	u.Resumes = nil
	return u
}

// View is fact T as a pure function — `TaskObject.view`.
func (t *Task) View(p *Promise) *Task {
	if p.State != Pending && t.State != TaskFulfilled {
		return t.Fulfill()
	}
	return t
}

// Message is the outbox payload.
type Message struct {
	Address string
	Kind    string // "execute" | "unblock"
	TaskID  string
	Version uint64
	Promise string
}

// Key is `OutboxEntry.key` — the outbox is a KEYED upsert, which is what
// makes repeated dispatch idempotent.
func (m Message) Key() string {
	if m.Kind == "execute" {
		return m.TaskID
	}
	return fmt.Sprintf("%s:notify:%s", m.Promise, m.Address)
}

// ---------------------------------------------------------------- the state

// ServerState is `AbstractModel.ServerState`: four components. No timeout
// sets, no deferred queue, no config.
//
// Schedules are omitted deliberately, not forgotten. `nextCron` and
// `occurrences` are `opaque` in the Lean with no value, so there is
// nothing to port — see valid/lean/schedules.lean. A trace mentioning
// schedules is rejected by the loader rather than silently checked
// against an invented calendar.
type ServerState struct {
	Promises []*Promise
	Tasks    []*Task
	Outbox   []Message
}

func (s *ServerState) clone() *ServerState {
	t := &ServerState{
		Promises: make([]*Promise, len(s.Promises)),
		Tasks:    make([]*Task, len(s.Tasks)),
		Outbox:   append([]Message(nil), s.Outbox...),
	}
	copy(t.Promises, s.Promises)
	copy(t.Tasks, s.Tasks)
	return t
}

func (s *ServerState) GetPromise(id string) *Promise {
	for _, p := range s.Promises {
		if p.ID == id {
			return p
		}
	}
	return nil
}

// SetSettled is the coupled write — `setSettled` in
// spec/02-abstract/state.lean. Storing a settled promise stores its
// co-keyed task fulfilled, in the same step, so that
//
//	stored promise settled  <=>  stored co-keyed task fulfilled
//
// holds of every state this model reaches. Birth is the other writer of
// a settled promise and cannot come through here — there is no stored
// task to couple to — so the create paths write the fulfilled task
// themselves.
func (s *ServerState) SetSettled(p *Promise) {
	s.SetPromise(p)
	if p.State == Pending {
		return
	}
	if t := s.GetTask(p.ID); t != nil && t.State != TaskFulfilled {
		s.SetTask(t.Fulfill())
	}
}

func (s *ServerState) SetPromise(p *Promise) {
	for i, q := range s.Promises {
		if q.ID == p.ID {
			s.Promises[i] = p
			return
		}
	}
	s.Promises = append(s.Promises, p)
}

func (s *ServerState) GetTask(id string) *Task {
	for _, t := range s.Tasks {
		if t.ID == id {
			return t
		}
	}
	return nil
}

func (s *ServerState) SetTask(t *Task) {
	for i, u := range s.Tasks {
		if u.ID == t.ID {
			s.Tasks[i] = t
			return
		}
	}
	s.Tasks = append(s.Tasks, t)
}

// SetMessage is the keyed upsert.
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

// ------------------------------------------------------- the read Discipline

// Discipline is the ONLY difference between
// spec/02-abstract/external-steps-p.lean and
// m.lean. Verified by diffing the two files with view/touch normalised:
// the code is otherwise identical, so porting them as one parameterised
// handler set is faithful rather than a shortcut.
type Discipline int

const (
	// Projected serves the view and writes no fact (p.lean).
	Projected Discipline = iota
	// Materialized persists the view on every read (m.lean).
	Materialized
)

func (d Discipline) String() string {
	if d == Projected {
		return "-p (projected)"
	}
	return "-m (materialized)"
}

// readPromise is `viewPromise` or `touchPromise`.
func (s *ServerState) readPromise(d Discipline, id string, now uint64) *Promise {
	p := s.GetPromise(id)
	if p == nil {
		return nil
	}
	q := p.Project(now)
	if d == Materialized && q.State != p.State {
		s.SetSettled(q)
	}
	return q
}

// readTask is `viewTask` or `touchTask`. Returns (task, promise); a nil
// promise is the `some (t, none)` case, which handlers treat as 404/409.
func (s *ServerState) readTask(d Discipline, id string, now uint64) (*Task, *Promise) {
	t := s.GetTask(id)
	if t == nil {
		return nil, nil
	}
	p := s.readPromise(d, t.ID, now)
	if p == nil {
		return t, nil
	}
	u := t.View(p)
	if d == Materialized && u != t {
		s.SetTask(u)
	}
	return u, p
}

// ---------------------------------------------------------------- canonical

// Key is a total order-insensitive encoding of the state, used for
// porcupine's Equal and Hash. The components are keyed collections, so
// sorting by key is sound — the same argument as `canon` in
// valid/lean/validator.lean.
func (s *ServerState) Key() string {
	var b strings.Builder
	ps := append([]*Promise(nil), s.Promises...)
	sort.Slice(ps, func(i, j int) bool { return ps[i].ID < ps[j].ID })
	for _, p := range ps {
		cb := append([]string(nil), p.Callbacks...)
		ls := append([]string(nil), p.Listeners...)
		sort.Strings(cb)
		sort.Strings(ls)
		// EVERY field, not the observable ones.
		//
		// This is the dedup key, and dedup DISCARDS: two candidates with
		// the same key collapse to whichever arrived first. So a field
		// left out of the key is a field along which distinct states are
		// silently merged, and if it is observable — `createdAt` is, it
		// is in `PromiseRecord` and `matches` compares it — the survivor
		// may be the one that fails to match a later event while the one
		// that would have matched was thrown away. That is a false
		// REFUTE: the checker rejecting a run the specification permits.
		//
		// valid/lean/validator.lean's `canon` sorts and compares the WHOLE
		// state for exactly this reason. Matching it here costs a longer
		// string and removes the obligation to keep proving which fields
		// happen to be unobservable today.
		fmt.Fprintf(&b, "P|%s|%d|%d|%d|%s|%s|%s|%s|%s|%s\n",
			p.ID, p.State, p.TimeoutAt, p.CreatedAt, u64s(p.SettledAt),
			strings.Join(cb, ","), strings.Join(ls, ","), p.Tags.key(),
			canonValue(p.Param), canonValue(p.Value))
	}
	ts := append([]*Task(nil), s.Tasks...)
	sort.Slice(ts, func(i, j int) bool { return ts[i].ID < ts[j].ID })
	for _, t := range ts {
		rs := append([]string(nil), t.Resumes...)
		sort.Strings(rs)
		fmt.Fprintf(&b, "T|%s|%d|%d|%s|%s|%s|%s|%s\n",
			t.ID, t.State, t.Version, u64s(t.TTL), strs(t.PID),
			u64s(t.ExpiresAt), u64s(t.RetryAt), strings.Join(rs, ","))
	}
	ob := make([]string, 0, len(s.Outbox))
	for _, m := range s.Outbox {
		ob = append(ob, fmt.Sprintf("O|%s|%s|%s|%d|%s", m.Address, m.Kind, m.TaskID, m.Version, m.Promise))
	}
	sort.Strings(ob)
	b.WriteString(strings.Join(ob, "\n"))
	return b.String()
}

// ---------------------------------------------------------------- helpers

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
