package model

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"strings"
)

// Trace generation, for differential fuzzing.
//
// The three recorded captures exercise a narrow slice: 7 handler kinds,
// status 200 only, and exactly ONE of the six internal steps. Everything else in
// this package is untested by them. So: generate traces that reach the
// rest, and require the Lean checker in `valid/` to agree on every one.
//
// ## Why generated traces are valid by construction
//
// A script is a list of (external request | internal internal step, instant). Run
// it through the model, keep only the external steps, and the result is a
// trace SOME execution explains — namely the script. So a checker that
// refutes it is wrong, and no oracle is needed to know the right answer.
// That is the same trick `recordFrom` plays in valid/lean/validator.lean.
//
// Mutants give the other direction. Corrupt one recorded response and
// both checkers should refute; if they disagree about WHETHER, one of
// them is wrong.
//
// ## What is deliberately NOT generated
//
// Schedules — `occurrences` is opaque, so neither checker judges them.
// Values and params are always `{}`: the models do not track payloads, so
// generating them would produce differences neither checker can see.

// Gen builds one random script over a single origin.
//
// One origin per trace keeps every awaits-edge inside it, which is what
// `CheckPartitionable` requires; the fuzzer emits many traces rather than
// one wide one.
type Gen struct {
	r      *rand.Rand
	origin string
	now    uint64
	script []step
	// Jumpy allows large clock jumps. They are what make R1/R5
	// reachable — but a jump longer than the retry cadence lets an internal step arm a
	// deadline mid-gap, which the Lean checker declines to reason about
	// (`noNewInGapDeadline`). So jumpy corpora reach more code and compare
	// less: about 60% of them come back DECLINE, and a decline agrees with
	// anything. Both settings are worth running, for opposite reasons.
	Jumpy bool
}

type step struct {
	op       *Op    // external, or nil for an internal step
	internal string // internal-step name when op == nil
	arg      string // internal-step target
	arg2     string // internal step's second parameter (awaiter / listener address)
	now      uint64
}

func NewGen(seed int64, origin string) *Gen {
	return &Gen{r: rand.New(rand.NewSource(seed)), origin: origin, now: 1000, Jumpy: true}
}

func (g *Gen) id(suffix string) string { return g.origin + ":" + suffix }

// tick advances the clock. Sometimes by a lot, so deadlines are crossed
// and R1/R5 become enabled — a generator that never lets time pass
// cannot reach the timeout internal steps at all.
func (g *Gen) tick() uint64 {
	switch g.r.Intn(10) {
	case 0:
		if g.Jumpy {
			g.now += uint64(g.r.Intn(40000)) // jump past deadlines
		} else {
			g.now += uint64(g.r.Intn(400))
		}
	case 1:
		// no advance: two events at the same instant
	default:
		g.now += uint64(1 + g.r.Intn(50))
	}
	return g.now
}

var settleStates = []PromiseState{Resolved, Rejected, RejectedCanceled,
	Pending /* 400: not settable */, RejectedTimedout /* 400: server-owned */}

// Script generates `n` steps: a BACKBONE of well-formed workflows with
// random noise and internal-step firings interleaved.
//
// Purely random requests do not work. The first version of this was
// uniform over the alphabet and produced 50 x 404 and zero state-changing
// internal-step firings in five traces — nothing existed for an internal step to act on, so
// the corpus could not reach the code it was written to test. Structure
// first, perturbation on top.
//
// The noise still carries the error paths the captures never reach:
// wrong versions (409), missing ids (404), unsettable states and
// duplicate awaited lists (400), internal awaited promises (422).
func (g *Gen) Script(n int) []step {
	ext := Tags{"resonate:external": "true", "resonate:origin": g.origin}
	tgt := Tags{"resonate:target": "poll://any@w1", "resonate:origin": g.origin}
	timer := Tags{"resonate:timer": "true", "resonate:external": "true", "resonate:origin": g.origin}

	var plan []Op
	var ids []string
	wf := 0

	// refill lays down one complete workflow: two promises, acquire,
	// suspend, settle, re-acquire, fulfil, reads. Deadlines are drawn so
	// that some workflows outlive the trace and others expire inside it —
	// the second kind is what makes R1 and R5 reachable.
	refill := func(now uint64) {
		a := fmt.Sprintf("%s.a%d", g.origin, wf)
		x := fmt.Sprintf("%s.x%d", g.origin, wf)
		wf++
		ids = append(ids, a, x)
		long := now + 30000
		short := now + uint64(50+g.r.Intn(400)) // expires mid-trace
		aTo, xTo := long, long
		switch g.r.Intn(4) {
		case 0:
			aTo = short
		case 1:
			xTo = short
		case 2:
			aTo, xTo = short, short
		}
		aTags := ext
		if g.r.Intn(5) == 0 {
			aTags = timer // resolves rather than times out
		}
		ttl := uint64(100 + g.r.Intn(600)) // short leases make R5 reachable
		plan = append(plan,
			// task.create is how the SDK starts every root workflow, and
			// task.fence how it does every fenced create/settle. Both were
			// absent from this corpus while they were absent from the
			// checkers; generating them is what keeps the fuzzer honest
			// about the traffic that actually exists.
			Op{Kind: "task.create", ID: x, PID: "p0", TTL: 500,
				Action: &FenceAction{Kind: "promise.create", ID: x, TimeoutAt: now + 30000, Tags: tgt}},
			Op{Kind: "task.fence", ID: x, Version: 1,
				Action: &FenceAction{Kind: "promise.create", ID: a, TimeoutAt: now + 30000, Tags: ext}},
			Op{Kind: "task.heartbeat", PID: "p0", Refs: []TaskRef{{ID: x, Version: 1}}},
			Op{Kind: "task.fence", ID: x, Version: 1,
				Action: &FenceAction{Kind: "promise.settle", ID: a, State: Resolved}},
			Op{Kind: "promise.create", ID: a, TimeoutAt: aTo, Tags: aTags},
			Op{Kind: "promise.create", ID: x, TimeoutAt: xTo, Tags: tgt},
			Op{Kind: "task.get", ID: x},
			Op{Kind: "promise.register_listener", ID: a, PID: "poll://any@w1"},
			Op{Kind: "task.acquire", ID: x, Version: 0, PID: "p0", TTL: ttl},
			Op{Kind: "task.suspend", ID: x, Version: 1, Awaited: []string{a}},
			Op{Kind: "promise.settle", ID: a, State: Resolved},
			Op{Kind: "task.get", ID: x},
			Op{Kind: "task.acquire", ID: x, Version: 1, PID: "p1", TTL: ttl},
			Op{Kind: "task.fulfill", ID: x, Version: 2, State: Resolved},
			Op{Kind: "promise.get", ID: x},
			Op{Kind: "promise.get", ID: a},
		)
	}

	pick := func() string {
		if len(ids) == 0 {
			return g.id("nope")
		}
		if g.r.Intn(8) == 0 {
			return g.id("nope") // a 404
		}
		return ids[g.r.Intn(len(ids))]
	}

	for i := 0; i < n; i++ {
		now := g.tick()
		if len(plan) == 0 {
			refill(now)
		}
		switch {
		case g.r.Intn(4) == 0:
			g.script = append(g.script, g.randomInternalStep(ids, now))
			continue
		case g.r.Intn(5) == 0:
			// noise: a deliberately ill-formed or ill-timed request
			g.script = append(g.script, step{op: g.noise(pick(), now), now: now})
			continue
		}
		op := plan[0]
		plan = plan[1:]
		op.Now = now
		if op.Tags == nil {
			op.Tags = Tags{}
		}
		g.script = append(g.script, step{op: &op, now: now})
	}
	return g.script
}

// noise is one request drawn to land somewhere the structured plan does
// not go: an error path, or a branch that is legal but rare.
//
// The last three cases exist because the plan cannot reach them. The
// structured workflow never pairs `resonate:timer` with
// `resonate:target` — the combination is malformed, so nothing that
// models real traffic would emit it — and never creates a timer whose
// deadline has already passed, because its deadlines are drawn ahead of
// the clock. Both are branches of the machine, so the fuzzer has to
// reach them deliberately or not at all.
func (g *Gen) noise(id string, now uint64) *Op {
	timerTgt := Tags{"resonate:timer": "true", "resonate:target": "poll://any@w1",
		"resonate:origin": g.origin}
	var op Op
	switch g.r.Intn(13) {
	case 0:
		op = Op{Kind: "promise.settle", ID: id, State: Pending} // 400: not settable
	case 1:
		op = Op{Kind: "promise.settle", ID: id, State: RejectedTimedout} // 400: server-owned
	case 2:
		op = Op{Kind: "task.acquire", ID: id, Version: uint64(g.r.Intn(4)), PID: "px", TTL: 200}
	case 3:
		op = Op{Kind: "task.suspend", ID: id, Version: uint64(g.r.Intn(3)), Awaited: []string{id}} // 400: self
	case 4:
		op = Op{Kind: "task.suspend", ID: id, Version: 1, Awaited: []string{}} // 400: empty
	case 5:
		op = Op{Kind: "task.fulfill", ID: id, Version: uint64(g.r.Intn(4)), State: Resolved}
	case 6:
		op = Op{Kind: "task.release", ID: id, Version: uint64(g.r.Intn(4))}
	case 7:
		op = Op{Kind: "promise.register_listener", ID: id, PID: "nonsense"} // 400: bad address
	case 8:
		op = Op{Kind: "task.halt", ID: id}
	case 9:
		// 400: a timer is never targeted
		op = Op{Kind: "promise.create", ID: fmt.Sprintf("%s.tt%d", g.origin, now), TimeoutAt: now + 1000, Tags: timerTgt}
	case 10:
		// the same internal step at the other door
		tid := fmt.Sprintf("%s.tx%d", g.origin, now)
		op = Op{Kind: "task.create", ID: tid, PID: "p0", TTL: 500,
			Action: &FenceAction{Kind: "promise.create", ID: tid,
				TimeoutAt: now + 1000, Tags: timerTgt}}
	case 11:
		// fact P at birth, timer verdict: born `resolved`, not
		// `rejectedTimedout`, and untargeted so it carries no task
		op = Op{Kind: "promise.create", ID: fmt.Sprintf("%s.tm%d", g.origin, now),
			TimeoutAt: now - uint64(1+g.r.Intn(50)),
			Tags: Tags{"resonate:timer": "true", "resonate:external": "true",
				"resonate:origin": g.origin}}
	default:
		op = Op{Kind: "task.continue", ID: id}
	}
	op.Now = now
	if op.Tags == nil {
		op.Tags = Tags{}
	}
	return &op
}

func (g *Gen) randomInternalStep(ids []string, now uint64) step {
	id := ids[g.r.Intn(len(ids))]
	other := ids[g.r.Intn(len(ids))]
	switch g.r.Intn(5) {
	case 0:
		return step{internal: "promiseTimeout", arg: id, now: now}
	case 1:
		return step{internal: "listener", arg: id, arg2: "poll://any@w1", now: now}
	case 2:
		return step{internal: "callback", arg: id, arg2: other, now: now}
	case 3:
		return step{internal: "taskLeaseTimeout", arg: id, now: now}
	default:
		return step{internal: "taskRetryTimeout", arg: id, now: now}
	}
}

// Run executes a script and returns the external events only — the internal steps are
// discarded, which is the point: the checkers must rediscover a schedule
// that explains what is left.
//
// `fired` counts internal-step firings that actually changed the state, so the
// fuzzer can report whether a corpus reached the internal steps at all rather than
// merely mentioning them.
// Snapshot is a state a replay passed through, with the instant it was at.
type Snapshot struct {
	S   *ServerState
	Now uint64
}

// States replays a script and returns the state BEFORE each step, so a
// property about enabled firings can be checked at every state the run
// actually reached rather than only at hand-built fixtures.
func States(d Discipline, script []step) []Snapshot {
	s := &ServerState{}
	out := make([]Snapshot, 0, len(script))
	for _, st := range script {
		now := st.now
		if st.op != nil {
			now = st.op.Now
		}
		out = append(out, Snapshot{S: s.clone(), Now: now})
		if st.op != nil {
			st.op.apply(s, d)
			continue
		}
		switch st.internal {
		case "promiseTimeout":
			s.ProcessPromiseTimeout(st.arg, st.now)
		case "listener":
			s.ProcessListener(st.arg, st.arg2, st.now)
		case "callback":
			s.ProcessCallback(st.arg, st.arg2, st.now)
		case "taskLeaseTimeout":
			s.ProcessLeaseTimeout(st.arg, st.now)
		case "taskRetryTimeout":
			s.ProcessRetryTimeout(st.arg, st.now, st.now)
		}
	}
	return out
}

func Run(d Discipline, script []step) (ops []Op, resps []Response, fired map[string]int) {
	s := &ServerState{}
	fired = map[string]int{}
	for _, st := range script {
		if st.op != nil {
			ops = append(ops, *st.op)
			resps = append(resps, st.op.apply(s, d))
			continue
		}
		before := s.Key()
		switch st.internal {
		case "promiseTimeout":
			s.ProcessPromiseTimeout(st.arg, st.now)
		case "listener":
			s.ProcessListener(st.arg, st.arg2, st.now)
		case "callback":
			s.ProcessCallback(st.arg, st.arg2, st.now)
		case "taskLeaseTimeout":
			s.ProcessLeaseTimeout(st.arg, st.now)
		case "taskRetryTimeout":
			s.ProcessRetryTimeout(st.arg, st.now, st.now)
		}
		if s.Key() != before {
			fired[st.internal]++
		}
	}
	return ops, resps, fired
}

// ---------------------------------------------------------------- emitting

var promiseWire = map[PromiseState]string{
	Pending: "pending", Resolved: "resolved", Rejected: "rejected",
	RejectedCanceled: "rejected_canceled", RejectedTimedout: "rejected_timedout",
}

// The Lean decoder's task names, from valid/lean/json.lean's `taskState`.
var taskWire = map[TaskState]string{
	TaskPending: "pending", TaskAcquired: "acquired", TaskSuspended: "suspended",
	TaskHalted: "halted", TaskFulfilled: "fulfilled",
}

// Emit writes the trace as NDJSON in resonate's wire envelope — the same
// trace shape (`valid/README.md`), so the Lean checker reads it unmodified.
//
// Records are emitted in FULL, because `valid/lean/json.lean` decodes full
// `PromiseRecord`/`TaskRecord` values and compares them. Emitting only a
// status and a state would make the Lean side compare less than it can,
// and a differential test that hides differences is worse than none.
func Emit(ops []Op, resps []Response) string {
	var b strings.Builder
	for i, o := range ops {
		fmt.Fprintf(&b, `{"kind":%q,"now":%d,"req":%s,"res":{"kind":%q,"head":{"status":%d},"data":%s}}`+"\n",
			o.Kind, o.Now, emitReq(o), o.Kind, resps[i].Status, emitData(resps[i]))
	}
	return b.String()
}

func emitReq(o Op) string {
	var f []string
	add := func(s string, a ...interface{}) { f = append(f, fmt.Sprintf(s, a...)) }
	add(`"id":%q`, o.ID)
	switch o.Kind {
	case "promise.create":
		add(`"timeoutAt":%d`, o.TimeoutAt)
		add(`"param":%s`, rawOr(o.Param))
		add(`"tags":%s`, emitTags(o.Tags))
	case "promise.settle":
		add(`"state":%q`, promiseWire[o.State])
		add(`"value":%s`, rawOr(o.Value))
	case "task.create":
		f = []string{fmt.Sprintf(`"pid":%q,"ttl":%d,"action":{"kind":"promise.create","head":{},"data":%s}`,
			o.PID, o.TTL, emitPromiseCreate(o.Action))}
	case "task.fence":
		var inner string
		if o.Action != nil && o.Action.Kind == "promise.settle" {
			inner = fmt.Sprintf(`{"id":%q,"state":%q,"value":%s}`, o.Action.ID,
				promiseWire[o.Action.State], rawOr(o.Action.Value))
		} else {
			inner = emitPromiseCreate(o.Action)
		}
		f = append(f, fmt.Sprintf(`"version":%d`, o.Version))
		f = append(f, fmt.Sprintf(`"action":{"kind":%q,"head":{},"data":%s}`, o.Action.Kind, inner))
	case "task.heartbeat":
		var ts []string
		for _, r := range o.Refs {
			ts = append(ts, fmt.Sprintf(`{"id":%q,"version":%d}`, r.ID, r.Version))
		}
		f = []string{fmt.Sprintf(`"pid":%q,"tasks":[%s]`, o.PID, strings.Join(ts, ","))}
	case "promise.register_callback":
		f = []string{fmt.Sprintf(`"awaited":%q,"awaiter":%q`, o.ID, o.Awaiter)}
	case "promise.register_listener":
		f = []string{fmt.Sprintf(`"awaited":%q,"address":%q`, o.ID, o.PID)}
	case "task.acquire":
		add(`"version":%d`, o.Version)
		add(`"pid":%q`, o.PID)
		add(`"ttl":%d`, o.TTL)
	case "task.suspend":
		add(`"version":%d`, o.Version)
		var as []string
		for _, a := range o.Awaited {
			as = append(as, fmt.Sprintf(
				`{"kind":"promise.register_callback","head":{},"data":{"awaited":%q,"awaiter":%q}}`, a, o.ID))
		}
		add(`"actions":[%s]`, strings.Join(as, ","))
	case "task.fulfill":
		add(`"version":%d`, o.Version)
		add(`"action":{"kind":"promise.settle","head":{},"data":{"id":%q,"state":%q,"value":%s}}`,
			o.ID, promiseWire[o.State], rawOr(o.Value))
	case "task.release":
		add(`"version":%d`, o.Version)
	}
	return "{" + strings.Join(f, ",") + "}"
}

// rawOr preserves a payload verbatim, or emits `{}` when the trace was
// generated rather than recorded.
func rawOr(r json.RawMessage) string {
	if len(r) == 0 {
		return "{}"
	}
	return string(r)
}

func emitPromiseCreate(a *FenceAction) string {
	if a == nil {
		return "{}"
	}
	return fmt.Sprintf(`{"id":%q,"timeoutAt":%d,"param":%s,"tags":%s}`,
		a.ID, a.TimeoutAt, rawOr(a.Param), emitTags(a.Tags))
}

func emitTags(t Tags) string {
	if len(t) == 0 {
		return "{}"
	}
	var f []string
	for _, k := range sortedKeys(t) {
		f = append(f, fmt.Sprintf("%q:%q", k, t[k]))
	}
	return "{" + strings.Join(f, ",") + "}"
}

func emitData(r Response) string {
	var f []string
	if r.Promise != nil {
		p := r.Promise
		settled := ""
		if p.SettledAt != nil {
			settled = fmt.Sprintf(`,"settledAt":%d`, *p.SettledAt)
		}
		f = append(f, fmt.Sprintf(
			`"promise":{"id":%q,"state":%q,"param":%s,"value":%s,"tags":%s,"timeoutAt":%d,"createdAt":%d%s}`,
			p.ID, promiseWire[p.State], rawOr(p.Param), rawOr(p.Value),
			emitTags(p.Tags), p.TimeoutAt, p.CreatedAt, settled))
	}
	if r.Inner != nil {
		in := r.Inner
		var pd string
		if in.Promise != nil {
			p := in.Promise
			settled := ""
			if p.SettledAt != nil {
				settled = fmt.Sprintf(`,"settledAt":%d`, *p.SettledAt)
			}
			pd = fmt.Sprintf(`{"promise":{"id":%q,"state":%q,"param":%s,"value":%s,"tags":%s,"timeoutAt":%d,"createdAt":%d%s}}`,
				p.ID, promiseWire[p.State], rawOr(p.Param), rawOr(p.Value),
				emitTags(p.Tags), p.TimeoutAt, p.CreatedAt, settled)
		} else {
			pd = `"Promise not found"`
		}
		f = append(f, fmt.Sprintf(`"action":{"kind":%q,"head":{"status":%d},"data":%s}`,
			in.Kind, in.Status, pd))
	}
	if r.Task != nil {
		t := r.Task
		extra := ""
		if t.TTL != nil {
			extra += fmt.Sprintf(`,"ttl":%d`, *t.TTL)
		}
		if t.PID != nil {
			extra += fmt.Sprintf(`,"pid":%q`, *t.PID)
		}
		f = append(f, fmt.Sprintf(`"task":{"id":%q,"state":%q,"version":%d,"resumes":%d%s}`,
			t.ID, taskWire[t.State], t.Version, len(t.Resumes), extra))
	}
	return "{" + strings.Join(f, ",") + "}"
}

func sortedKeys(t Tags) []string {
	ks := make([]string, 0, len(t))
	for k := range t {
		ks = append(ks, k)
	}
	for i := 1; i < len(ks); i++ {
		for j := i; j > 0 && ks[j] < ks[j-1]; j-- {
			ks[j], ks[j-1] = ks[j-1], ks[j]
		}
	}
	return ks
}
