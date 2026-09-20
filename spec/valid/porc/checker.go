package model

import (
	"context"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"sort"
	"strings"

	"github.com/anishathalye/porcupine"
)

type Op struct {
	Kind string
	Now  uint64

	ID        string
	TimeoutAt uint64
	Tags      Tags
	Version   uint64
	PID       string
	TTL       uint64
	State     PromiseState
	Awaited   []string
	Awaiter   string
	Action    *FenceAction
	Refs      []TaskRef

	Param json.RawMessage
	Value json.RawMessage
}

func (o Op) String() string {
	return fmt.Sprintf("%s %s @%d", o.Kind, o.ID, o.Now)
}

func (o Op) apply(s *ServerState, d Discipline) Response {
	switch o.Kind {
	case "promise.get":
		return s.PromiseGet(d, o.ID, o.Now)
	case "promise.create":
		return s.PromiseCreate(d, PromiseCreateReq{o.ID, o.TimeoutAt, o.Tags, o.Param}, o.Now)
	case "promise.settle":
		return s.PromiseSettle(d, o.ID, o.State, o.Value, o.Now)
	case "promise.register_callback":
		return s.PromiseRegisterCallback(d, o.ID, o.Awaiter, o.Now)
	case "task.get":
		return s.TaskGet(d, o.ID, o.Now)
	case "task.acquire":
		return s.TaskAcquire(d, o.ID, o.Version, o.PID, o.TTL, o.Now)
	case "task.suspend":
		return s.TaskSuspend(d, o.ID, o.Version, o.Awaited, o.Now)
	case "task.fulfill":
		return s.TaskFulfill(d, o.ID, o.Version, o.State, o.Value, o.Now)
	case "task.release":
		return s.TaskRelease(d, o.ID, o.Version, o.Now)
	case "task.heartbeat":
		return s.TaskHeartbeat(d, o.PID, o.Refs, o.Now)
	case "promise.search", "task.search", "schedule.search":
		return s.Search()
	case "promise.register_listener":
		return s.PromiseRegisterListener(d, o.ID, o.PID, o.Now)
	case "task.halt":
		return s.TaskHalt(d, o.ID, o.Now)
	case "task.continue":
		return s.TaskContinue(d, o.ID, o.Now)
	case "task.create":
		if o.Action == nil {
			return Response{Status: -1}
		}
		return s.TaskCreate(d, o.PID, o.TTL,
			PromiseCreateReq{o.Action.ID, o.Action.TimeoutAt, o.Action.Tags, o.Action.Param}, o.Now)
	case "task.fence":
		if o.Action == nil {
			return Response{Status: -1}
		}
		r, inner := s.TaskFence(d, o.ID, o.Version, *o.Action, o.Now)
		r.Inner = inner
		return r
	default:
		return Response{Status: -1}
	}
}

func PendingOp(r Response) bool {
	return r.Status == 500
}

func matches(got, want Response) bool {
	if got.Status != want.Status {
		return false
	}
	if want.Promise != nil && !samePromiseRecord(got.Promise, want.Promise) {
		return false
	}
	if want.Inner != nil {
		g := got.Inner
		if g == nil || g.Kind != want.Inner.Kind || g.Status != want.Inner.Status {
			return false
		}
		if want.Inner.Promise != nil && !samePromiseRecord(g.Promise, want.Inner.Promise) {
			return false
		}
	}
	if want.Task != nil {
		t := got.Task
		if t == nil || t.ID != want.Task.ID || t.State != want.Task.State ||
			t.Version != want.Task.Version || len(t.Resumes) != len(want.Task.Resumes) ||
			!eqU64(t.TTL, want.Task.TTL) || !eqStr(t.PID, want.Task.PID) {
			return false
		}
	}
	return true
}

func samePromiseRecord(got, want *PromiseRecord) bool {
	if got == nil {
		return false
	}
	return got.ID == want.ID && got.State == want.State &&
		got.TimeoutAt == want.TimeoutAt && got.CreatedAt == want.CreatedAt &&
		eqU64(got.SettledAt, want.SettledAt) &&
		got.Tags.key() == want.Tags.key() &&
		sameJSON(got.Param, want.Param) && sameJSON(got.Value, want.Value)
}

func sameJSON(a, b []byte) bool {
	return canonValue(a) == canonValue(b)
}

func canonValue(raw []byte) string {
	if len(raw) == 0 {
		return "|"
	}
	var v struct {
		Headers map[string]string `json:"headers"`
		Data    *string           `json:"data"`
	}
	if err := json.Unmarshal(raw, &v); err != nil {

		return "raw:" + string(raw)
	}
	ks := make([]string, 0, len(v.Headers))
	for k := range v.Headers {
		ks = append(ks, k)
	}
	sort.Strings(ks)
	var b strings.Builder
	for _, k := range ks {
		fmt.Fprintf(&b, "%s=%s;", k, v.Headers[k])
	}
	b.WriteString("|")
	if v.Data != nil {
		b.WriteString(*v.Data)
	}
	return b.String()
}

func appendNote(w []string, note string) []string {
	out := make([]string, len(w), len(w)+1)
	copy(out, w)
	return append(out, note)
}

func eqU64(a, b *uint64) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return *a == *b
}

func eqStr(a, b *string) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return *a == *b
}

type modelState struct {
	state   *ServerState
	witness []string

	key string
}

func newModelState(s *ServerState, w []string) modelState {
	return modelState{state: s, witness: w, key: s.Key()}
}

var Fuel = 24

type Saturation struct{ ok bool }

func (s *Saturation) OK() bool { return s.ok }

func NondeterministicModel(d Discipline, sat *Saturation, partition bool) porcupine.NondeterministicModel {
	sat.ok = true
	m := porcupine.NondeterministicModel{
		Init: func() []interface{} {
			return []interface{}{newModelState(&ServerState{}, nil)}
		},

		StepContext: func(ctx context.Context, st interface{}, input interface{}, output interface{}) []interface{} {
			ms := st.(modelState)
			op := input.(Op)
			want := output.(Response)

			cands, ok := closure(ctx, []candidate{{state: ms.state, witness: ms.witness, key: ms.key}}, op.Now, Fuel, pickFor(op))
			if !ok {
				sat.ok = false
			}
			var out []interface{}
			seen := map[string]bool{}
			keep := func(s *ServerState, w []string, k string) {
				if seen[k] {
					return
				}
				seen[k] = true
				out = append(out, modelState{state: s, witness: w, key: k})
			}
			for _, c := range cands {
				if ctx.Err() != nil {
					return out
				}
				if PendingOp(want) {

					keep(c.state, appendNote(c.witness, fmt.Sprintf("pending %v: not applied", op)), c.key)
					next := c.state.clone()
					op.apply(next, d)
					keep(next, appendNote(c.witness, fmt.Sprintf("pending %v: applied, response unobserved", op)), next.Key())
					continue
				}
				next := c.state.clone()
				got := op.apply(next, d)
				if !matches(got, want) {
					continue
				}
				keep(next, c.witness, next.Key())
			}
			return out
		},
		Equal: func(a, b interface{}) bool {
			return a.(modelState).key == b.(modelState).key
		},

		Hash: func(st interface{}) uint64 {
			h := fnv.New64a()
			_, _ = h.Write([]byte(st.(modelState).key))
			return h.Sum64()
		},
		DescribeOperation: func(input, output interface{}) string {
			return fmt.Sprintf("%v -> %d", input.(Op), output.(Response).Status)
		},
		DescribeState: func(st interface{}) string {
			return strings.ReplaceAll(st.(modelState).state.Key(), "\n", " | ")
		},
	}
	if partition {
		m.Partition = partitionOps
		m.PartitionEvent = partitionEvents
	}
	return m
}

func Replay(d Discipline, ops []Op, resps []Response) (witness []string, failedAt int, ok bool) {
	cur := []candidate{newCand(&ServerState{}, nil)}
	for i, op := range ops {
		cands, sat := closure(context.Background(), cur, op.Now, Fuel, pickFor(op))
		if !sat {
			return nil, i, false
		}
		var next []candidate
		seen := map[string]bool{}
		keep := func(s *ServerState, w []string, k string) {
			if seen[k] {
				return
			}
			seen[k] = true
			next = append(next, candidate{state: s, witness: w, key: k})
		}
		for _, c := range cands {
			if PendingOp(resps[i]) {

				keep(c.state, appendNote(c.witness, fmt.Sprintf("pending %v: not applied", op)), c.key)
				t := c.state.clone()
				op.apply(t, d)
				keep(t, appendNote(c.witness, fmt.Sprintf("pending %v: applied, response unobserved", op)), t.Key())
				continue
			}
			t := c.state.clone()
			if !matches(op.apply(t, d), resps[i]) {
				continue
			}
			keep(t, c.witness, t.Key())
		}
		if len(next) == 0 {
			return nil, i, false
		}
		cur = next
	}
	return cur[0].witness, -1, true
}
