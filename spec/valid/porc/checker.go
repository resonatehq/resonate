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

// The porcupine wiring.
//
// `porcupine.NondeterministicModel` takes a Step returning a SET of next
// states and converts it to a deterministic Model by a power-set
// construction. That is the same subset construction the Lean checker
// does by hand in valid/lean/validator.lean — which is the point of building
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
	Action    *FenceAction // task.fence, task.create
	Refs      []TaskRef    // task.heartbeat
	// Param and Value are the request's payloads, carried verbatim and
	// never interpreted. The Lean checker compares full records, so a
	// re-emitted trace that dropped them refuted a file it had accepted.
	Param json.RawMessage
	Value json.RawMessage
}

func (o Op) String() string {
	return fmt.Sprintf("%s %s @%d", o.Kind, o.ID, o.Now)
}

// apply runs the handler for this op. It is the deterministic half: all
// nondeterminism is in the internal step closure that runs before it.
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

// Pending reports whether a recorded response is a PENDING op: the server
// answered 500, the wire's "no verdict". A faithful implementation over a
// fallible store answers 500 exactly when it cannot determine whether the
// request applied (the write may have landed with its acknowledgment lost),
// so the machine — which has no 500 transition — constrains such an op only
// existentially: EITHER the state is unchanged (the request did not go
// through) OR the request applied once, with a response nobody observed.
// The checker carries both branches; the surrounding observations are what
// collapse them.
//
// The cost is stated where the verdict is printed: a pending op is
// unconstrained evidence, so a run's LINEARIZABLE is only as strong as its
// pending count is low — an implementation answering 500 to everything
// would pass vacuously.
func PendingOp(r Response) bool {
	return r.Status == 500
}

// matches compares a computed response against the recorded one.
//
// A record the capture does not carry at all is not evidence and is not
// demanded — but a record it DOES carry is compared in full, field for
// field, exactly as valid/lean/json.lean compares its decoded records. The
// two checkers must look at the same evidence before their verdicts mean
// anything together.
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

// samePromiseRecord compares the WHOLE `PromiseRecord`.
//
// `PromiseObject.toRecord` in `spec/02-abstract/state.lean` is
//
//	{ id, state, param, value, tags, timeoutAt, createdAt, settledAt }
//
// and valid/lean/json.lean decodes every one of those and compares them. This
// used to compare five of the eight, skipping `param`, `value` and
// `tags` — so a server that returned the wrong VALUE on a settle was
// caught by the Lean checker and waved through here. Two checkers that
// disagree about how much to check are not two checks.
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

// sameJSON compares two payloads as the specification's `Value`, not as
// bytes.
//
//	structure Value where
//	  headers : Tags          := []
//	  data    : Option String := none
//
// (spec/01-protocol/types.lean:5) — TWO fields, and nothing else in the
// JSON is part of the value. valid/lean/json.lean's `valueOf` reads exactly
// those two and ignores the rest, so this must too, or the checkers
// disagree over payload shape rather than over the protocol.
//
// It is also why byte comparison is wrong: resonate emits a bare `{}`
// where no value is set, and `{}`, `null` and an absent field are all
// `Value.default`. Comparing bytes refuted every `task.create` in the
// SDK captures on that alone.
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
		// Not the value shape at all — compare verbatim rather than
		// declaring an agreement that cannot be justified.
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

// appendNote copies before appending, so sibling branches never share a
// backing array.
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

// modelState is what porcupine carries between steps. The witness rides
// along so an accepted history can report the hidden internal-step firings — the
// same certificate idea as the Lean checker's schedule.
type modelState struct {
	state   *ServerState
	witness []string
	// key is the canonical form, computed ONCE. `Equal` and `Hash` are
	// called O(n^2) times by porcupine's `merge` when it deduplicates a
	// power-set state, so recomputing it there is the difference between
	// comparing integers and rebuilding a string per comparison.
	key string
}

func newModelState(s *ServerState, w []string) modelState {
	return modelState{state: s, witness: w, key: s.Key()}
}

// Fuel bounds the internal step closure per step. Exhausting it means the model
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
			return []interface{}{newModelState(&ServerState{}, nil)}
		},
		// StepContext, not Step. porcupine only tests its deadline BETWEEN
		// calls into the model, so a step that runs a deep internal step closure
		// cannot be interrupted unless the context reaches it. Without
		// this the timeout is advisory: a 180s budget on a 160-event
		// concurrent history ran for 6m25s.
		StepContext: func(ctx context.Context, st interface{}, input interface{}, output interface{}) []interface{} {
			ms := st.(modelState)
			op := input.(Op)
			want := output.(Response)

			// Fire every schedule of internal internal steps that could have run
			// before this call, then keep the states whose response
			// matches what the server actually said. That filter is the
			// whole pruning power of the construction.
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
					// A pending op constrains nothing by itself: it either
					// did not go through (the state is untouched) or applied
					// once with a response nobody observed. Both branches
					// survive; later observations decide between them. The
					// witness slices are copied — two appends on one shared
					// backing array would silently overwrite each other.
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
		// Hash lets `merge` compare integers and fall back to `Equal` only
		// on collisions. porcupine documents it as "reduces the number of
		// Equal comparisons"; with a string key and a quadratic merge that
		// is the difference between linear and quadratic string work.
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

// Replay walks the trace sequentially, carrying the candidate set. It
// returns the internal-step firings that explain the run, and — when nothing does —
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
				// Same existential as the porcupine step: not applied, or
				// applied with the response unobserved.
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
