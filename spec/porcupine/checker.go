package model

import (
	"fmt"
	"strings"

	"github.com/anishathalye/porcupine"
)

// The porcupine wiring.
//
// `porcupine.NondeterministicModel` takes a Step returning a SET of next
// states and converts it to a deterministic Model by a power-set
// construction. That is the same subset construction the Lean checker
// does by hand in valid/validator.lean — which is the point of building
// this: an independent implementation, in a different language, driven by
// a checker nobody here wrote.

// Op is one recorded external call.
type Op struct {
	Kind string
	Now  uint64
	// request fields, by kind
	ID        string
	TimeoutAt uint64
	Tags      Tags
	Version   uint64
	PID       string
	TTL       uint64
	State     PromiseState
	Awaited   []string
	Awaiter   string
}

func (o Op) String() string {
	return fmt.Sprintf("%s %s @%d", o.Kind, o.ID, o.Now)
}

// apply runs the handler for this op. It is the deterministic half: all
// nondeterminism is in the rule closure that runs before it.
func (o Op) apply(s *ServerState, d Discipline) Response {
	switch o.Kind {
	case "promise.get":
		return s.PromiseGet(d, o.ID, o.Now)
	case "promise.create":
		return s.PromiseCreate(d, PromiseCreateReq{o.ID, o.TimeoutAt, o.Tags}, o.Now)
	case "promise.settle":
		return s.PromiseSettle(d, o.ID, o.State, o.Now)
	case "promise.register_callback":
		return s.PromiseRegisterCallback(d, o.ID, o.Awaiter, o.Now)
	case "task.get":
		return s.TaskGet(d, o.ID, o.Now)
	case "task.acquire":
		return s.TaskAcquire(d, o.ID, o.Version, o.PID, o.TTL, o.Now)
	case "task.suspend":
		return s.TaskSuspend(d, o.ID, o.Version, o.Awaited, o.Now)
	case "task.fulfill":
		return s.TaskFulfill(d, o.ID, o.Version, o.State, o.Now)
	case "task.release":
		return s.TaskRelease(d, o.ID, o.Version, o.Now)
	case "task.heartbeat":
		return s.TaskHeartbeat(d, o.PID, o.Now)
	default:
		return Response{Status: -1}
	}
}

// matches compares a computed response against the recorded one.
//
// Only what the wire format actually reports is compared. A field the
// capture does not carry is not evidence, and demanding agreement on it
// would refute traces for a difference nobody could have observed.
func matches(got, want Response) bool {
	if got.Status != want.Status {
		return false
	}
	if want.PromiseState != nil && (got.PromiseState == nil || *got.PromiseState != *want.PromiseState) {
		return false
	}
	if want.TaskState != nil && (got.TaskState == nil || *got.TaskState != *want.TaskState) {
		return false
	}
	if want.TaskVersion != nil && (got.TaskVersion == nil || *got.TaskVersion != *want.TaskVersion) {
		return false
	}
	if want.SettledAt != nil && (got.SettledAt == nil || *got.SettledAt != *want.SettledAt) {
		return false
	}
	return true
}

// modelState is what porcupine carries between steps. The witness rides
// along so an accepted history can report the hidden rule firings — the
// same certificate idea as the Lean checker's schedule.
type modelState struct {
	state   *ServerState
	witness []string
}

// Fuel bounds the rule closure per step. Exhausting it means the model
// stopped looking, which is NOT the same as finding nothing — see
// Saturated below.
var Fuel = 24

// Saturated reports whether every closure ran to a fixpoint. A `false`
// linearizability verdict is only meaningful when this is true; otherwise
// the correct reading is "inconclusive", exactly as in the Lean checker's
// three-valued verdict.
type Saturation struct{ ok bool }

func (s *Saturation) OK() bool { return s.ok }

// NondeterministicModel builds the porcupine model for one read
// Discipline. `sat` is written through as the check runs.
func NondeterministicModel(d Discipline, sat *Saturation, partition bool) porcupine.NondeterministicModel {
	sat.ok = true
	m := porcupine.NondeterministicModel{
		Init: func() []interface{} {
			return []interface{}{modelState{&ServerState{}, nil}}
		},
		Step: func(st interface{}, input interface{}, output interface{}) []interface{} {
			ms := st.(modelState)
			op := input.(Op)
			want := output.(Response)

			// Fire every schedule of internal rules that could have run
			// before this call, then keep the states whose response
			// matches what the server actually said. That filter is the
			// whole pruning power of the construction.
			cands, ok := closure([]candidate{{ms.state, ms.witness}}, op.Now, Fuel)
			if !ok {
				sat.ok = false
			}
			var out []interface{}
			seen := map[string]bool{}
			for _, c := range cands {
				next := c.state.clone()
				got := op.apply(next, d)
				if !matches(got, want) {
					continue
				}
				k := next.Key()
				if seen[k] {
					continue
				}
				seen[k] = true
				out = append(out, modelState{next, c.witness})
			}
			return out
		},
		Equal: func(a, b interface{}) bool {
			return a.(modelState).state.Key() == b.(modelState).state.Key()
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

// Replay walks the trace sequentially, carrying the candidate set. It
// returns the rule firings that explain the run, and — when nothing does —
// the index of the first event no schedule can account for.
//
// One forward pass. An earlier version answered "where did it fail?" by
// re-checking every prefix, which is quadratic and took longer on a 2200
// event trace than the linearizability check itself.
//
// porcupine tells you WHETHER a history linearizes; this says WITH WHAT,
// or WHERE it stops, which is what makes a verdict reviewable rather than
// merely coloured.
func Replay(d Discipline, ops []Op, resps []Response) (witness []string, failedAt int, ok bool) {
	cur := []candidate{{&ServerState{}, nil}}
	for i, op := range ops {
		cands, sat := closure(cur, op.Now, Fuel)
		if !sat {
			return nil, i, false
		}
		var next []candidate
		seen := map[string]bool{}
		for _, c := range cands {
			t := c.state.clone()
			if !matches(op.apply(t, d), resps[i]) {
				continue
			}
			k := t.Key()
			if seen[k] {
				continue
			}
			seen[k] = true
			next = append(next, candidate{t, c.witness})
		}
		if len(next) == 0 {
			return nil, i, false
		}
		cur = next
	}
	return cur[0].witness, -1, true
}
