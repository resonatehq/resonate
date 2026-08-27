package model

import (
	"os"
	"strings"
	"testing"
	"time"

	"github.com/anishathalye/porcupine"
)

const traceDir = "../traces/"

func load(t *testing.T, name string) ([]Op, []Response) {
	t.Helper()
	f, err := os.Open(traceDir + name)
	if err != nil {
		t.Skipf("trace %s not present: %v", name, err)
	}
	defer f.Close()
	ops, resps, err := LoadTrace(f)
	if err != nil {
		t.Fatalf("load %s: %v", name, err)
	}
	return ops, resps
}

func history(ops []Op, resps []Response, concurrent int) []porcupine.Operation {
	h := make([]porcupine.Operation, len(ops))
	for i := range ops {
		call := int64(i) * 2
		ret := call + 1
		if concurrent > 1 {
			ret = call + int64(2*concurrent)
		}
		h[i] = porcupine.Operation{ClientId: i % concurrent, Input: ops[i],
			Call: call, Output: resps[i], Return: ret}
	}
	return h
}

func check(t *testing.T, d Discipline, ops []Op, resps []Response, concurrent int, partition bool) porcupine.CheckResult {
	t.Helper()
	var sat Saturation
	nm := NondeterministicModel(d, &sat, partition)
	r, _ := porcupine.CheckOperationsVerbose(nm.ToModel(), history(ops, resps, concurrent), 120*time.Second)
	if !sat.OK() {
		t.Fatalf("%v: an internal-step closure hit the fuel bound; no verdict is earned", d)
	}
	return r
}

var disciplines = []Discipline{Projected, Materialized}

// A clean capture must linearize against BOTH read disciplines.
//
// That they agree is not a coincidence to be observed but a THEOREM:
// `responseLockstepAbstract` in spec/04-theorems/proof.lean is an
// unconditional mechanized proof, no sorry, that the two disciplines
// answer identically on the response channel. So a disagreement here is a
// bug in this port, and this test is a differential check of the Go
// against a proven property of the Lean.
func TestCleanTracesLinearize(t *testing.T) {
	// The four `resonate-sdk-*` captures come from work/go driving the Go
	// SDK against a real server, so they exercise the kinds the
	// hand-written load generators never sent: `task.create`,
	// `task.fence` and `promise.register_listener`. Two of the four are
	// suspend/resume runs, which is the only way callback gets reached at all.
	for _, name := range []string{
		"resonate-sqlite-50wf.ndjson", "resonate-sqlite-200wf-debugstart.ndjson",
		"resonate-sdk-simple-run.ndjson", "resonate-sdk-simple-rpc.ndjson",
		"resonate-sdk-simple-sleep.ndjson", "resonate-sdk-fan-out.ndjson",
	} {
		ops, resps := load(t, name)
		for _, d := range disciplines {
			if got := check(t, d, ops, resps, 1, true); got != porcupine.Ok {
				t.Errorf("%s %v: got %v, want Ok", name, d, got)
			}
		}
	}
}

// The mixed-clock capture must NOT linearize, and both disciplines must
// stop at the same place. The Lean checker independently refutes this
// trace at event 886; if the Go port refutes somewhere else, one of the
// two is wrong.
func TestBadCaptureRefutedAtSameEvent(t *testing.T) {
	ops, resps := load(t, "resonate-sqlite-200wf.ndjson")
	const leanRefutesAt = 886
	for _, d := range disciplines {
		if got := check(t, d, ops, resps, 1, true); got != porcupine.Illegal {
			t.Errorf("%v: got %v, want Illegal", d, got)
		}
		_, at, ok := ReplayPartitioned(d, ops, resps)
		if ok {
			t.Fatalf("%v: replay found an explanation for a trace porcupine rejected", d)
		}
		if at != leanRefutesAt {
			t.Errorf("%v: first unexplainable event %d, Lean checker says %d", d, at, leanRefutesAt)
		}
	}
}

// Partitioning is an optimisation, so it must not change any verdict —
// in EITHER direction. Turning Illegal into Ok would be unsoundness; the
// reverse would be a false alarm.
//
// Two workflows, because unpartitioned checking is exponential in the
// history: 11 events 1ms, 22 events 88ms, 33 events 7.4s, 44 events does
// not finish in 30s. That curve is the reason partitioning exists, and
// the reason this test is small. An `Unknown` (timeout) on either side is
// a failed test, not an agreement — comparing two timeouts proves
// nothing.
func TestPartitioningAgrees(t *testing.T) {
	ops, resps := load(t, "resonate-sqlite-50wf.ndjson")
	ops, resps = ops[:22], resps[:22]

	lied := append([]Response(nil), resps...)
	liedP := *lied[0].Promise
	liedP.State = Resolved
	lied[0] = Response{Status: 200, Promise: &liedP}

	for _, tc := range []struct {
		name string
		r    []Response
		want porcupine.CheckResult
	}{
		{"honest", resps, porcupine.Ok},
		{"tampered", lied, porcupine.Illegal},
	} {
		for _, d := range disciplines {
			with := check(t, d, ops, tc.r, 1, true)
			without := check(t, d, ops, tc.r, 1, false)
			if with == porcupine.Unknown || without == porcupine.Unknown {
				t.Fatalf("%s / %v: a check timed out (%v / %v); shrink the history",
					tc.name, d, with, without)
			}
			if with != without {
				t.Errorf("%s / %v: partitioned %v, unpartitioned %v", tc.name, d, with, without)
			}
			if with != tc.want {
				t.Errorf("%s / %v: got %v, want %v", tc.name, d, with, tc.want)
			}
		}
	}
}

// The precondition partitioning rests on — every awaits-edge inside one
// origin — holds for the captures, and is detected when it does not.
func TestPartitionableIsChecked(t *testing.T) {
	ops, _ := load(t, "resonate-sqlite-50wf.ndjson")
	if err := CheckPartitionable(ops); err != nil {
		t.Fatalf("capture should be partitionable: %v", err)
	}
	bad := []Op{{Kind: "task.suspend", ID: "o0:x", Awaited: []string{"o1:a"}}}
	if err := CheckPartitionable(bad); err == nil {
		t.Error("a cross-origin suspend must be rejected, not silently partitioned")
	}
}

// Discrimination. A checker that accepts everything is worthless, so
// corrupt a response and require a rejection. Both the status channel and
// the record channel are tried, because they prune differently.
func TestTamperedTracesAreRejected(t *testing.T) {
	ops, resps := load(t, "resonate-sqlite-50wf.ndjson")
	ops, resps = ops[:55], resps[:55]

	cases := map[string]func([]Response) []Response{
		"first status lied": func(r []Response) []Response {
			c := append([]Response(nil), r...)
			c[0] = Response{Status: 404}
			return c
		},
		"task state lied": func(r []Response) []Response {
			c := append([]Response(nil), r...)
			t := *c[2].Task
			t.State = TaskFulfilled
			c[2] = Response{Status: 200, Task: &t}
			return c
		},
		"promise settled early": func(r []Response) []Response {
			c := append([]Response(nil), r...)
			p := *c[0].Promise
			p.State = Resolved
			c[0] = Response{Status: 200, Promise: &p}
			return c
		},
	}
	for name, corrupt := range cases {
		for _, d := range disciplines {
			if got := check(t, d, ops, corrupt(resps), 1, true); got != porcupine.Illegal {
				t.Errorf("%s / %v: got %v, want Illegal", name, d, got)
			}
		}
	}
}

// The witness is a certificate: the recorded run needs hidden callback firings
// to be explainable at all, so an empty witness would mean the trace was
// never asking the interesting question.
func TestWitnessRecoversHiddenResumes(t *testing.T) {
	ops, resps := load(t, "resonate-sqlite-50wf.ndjson")
	for _, d := range disciplines {
		w, _, ok := ReplayPartitioned(d, ops, resps)
		if !ok {
			t.Fatalf("%v: clean trace has no witness", d)
		}
		resumes := 0
		for _, r := range w {
			if strings.HasPrefix(r, "callback") {
				resumes++
			}
		}
		if resumes != 50 {
			t.Errorf("%v: recovered %d callback firings, want 50 (one per workflow)", d, resumes)
		}
	}
}

// With overlapping intervals porcupine has to FIND an order rather than
// verify the recorded one. A capture is sequential, so widening it can
// only make more histories legal — a clean trace must still pass.
func TestConcurrentHistoriesStillLinearize(t *testing.T) {
	ops, resps := load(t, "resonate-sqlite-50wf.ndjson")
	ops, resps = ops[:55], resps[:55]
	for _, k := range []int{2, 3} {
		for _, d := range disciplines {
			if got := check(t, d, ops, resps, k, true); got != porcupine.Ok {
				t.Errorf("concurrent=%d %v: got %v, want Ok", k, d, got)
			}
		}
	}
}

// Schedules are not modelled, and the loader must say so rather than
// check a trace against an empty calendar.
func TestScheduleTracesAreRefused(t *testing.T) {
	const line = `{"kind":"schedule.create","now":1,"req":{"id":"s"},"res":{"head":{"status":200},"data":{}}}`
	if _, _, err := LoadTrace(strings.NewReader(line)); err == nil {
		t.Error("a trace mentioning schedules must be refused")
	}
}
