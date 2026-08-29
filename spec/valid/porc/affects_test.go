package model

import (
	"strings"
	"testing"
)

// The write half of the cone's soundness obligation, on states built to
// enable each internal step in turn. The fuzzer checks it across generated runs;
// this pins the shapes that matter even if the generator drifts.
func TestAffectsCoversObservableWrites(t *testing.T) {
	tgt := Tags{"resonate:target": "w1", "resonate:external": "true"}
	ext := Tags{"resonate:external": "true"}

	cases := []struct {
		name string
		now  uint64
		s    *ServerState
	}{
		{"promiseTimeout: a promise past its deadline, with an awaiter on its ledger", 300, &ServerState{
			Objects: []*Object{
				{ID: "a", Promise: &Promise{State: Pending, Tags: ext, TimeoutAt: 250, CreatedAt: 100,
					Callbacks: []string{"x"}, Listeners: []string{"https://l"}}},
				{ID: "x", Promise: &Promise{State: Pending, Tags: tgt, TimeoutAt: 2000, CreatedAt: 100}, Task: &Task{State: TaskSuspended, Version: 1}},
			},
		}},
		{"listener/callback: a settled promise with both ledgers loaded", 300, &ServerState{
			Objects: []*Object{
				{ID: "a", Promise: &Promise{State: Resolved, Tags: ext, TimeoutAt: 2000, CreatedAt: 100,
					SettledAt: u64p(200), Callbacks: []string{"x"}, Listeners: []string{"https://l"}}},
				{ID: "x", Promise: &Promise{State: Pending, Tags: tgt, TimeoutAt: 2000, CreatedAt: 100}, Task: &Task{State: TaskSuspended, Version: 1}},
			},
		}},
		{"taskLeaseTimeout: an acquired task past its lease", 300, &ServerState{
			Objects: []*Object{
				{ID: "x", Promise: &Promise{State: Pending, Tags: tgt, TimeoutAt: 2000, CreatedAt: 100}, Task: &Task{State: TaskAcquired, Version: 1, TTL: u64p(100),
					PID: strp("p0"), LeaseTimeoutAt: u64p(200)}},
			},
		}},
		{"taskRetryTimeout: a pending task past its retry deadline", 300, &ServerState{
			Objects: []*Object{
				{ID: "x", Promise: &Promise{State: Pending, Tags: tgt, TimeoutAt: 2000, CreatedAt: 100}, Task: &Task{State: TaskPending, Version: 1, RetryTimeoutAt: u64p(200)}},
			},
		}},
	}

	for _, c := range cases {
		if len(enabledFirings(c.s, c.now)) == 0 {
			t.Fatalf("%s: no firing enabled — the fixture does not test what it claims", c.name)
		}
		if bad := AffectsSound(c.s, c.now); bad != "" {
			t.Errorf("%s: %s", c.name, bad)
		}
	}
}

// The check must be able to FAIL, or it proves nothing. Understating a
// firing's affects has to be caught.
func TestAffectsCatchesAnUnderstatedSet(t *testing.T) {
	s := &ServerState{
		Objects: []*Object{
			{ID: "a", Promise: &Promise{State: Pending, Tags: Tags{"resonate:external": "true"},
				TimeoutAt: 250, CreatedAt: 100}},
		},
	}
	fs := enabledFirings(s, 300)
	if len(fs) != 1 {
		t.Fatalf("expected exactly promiseTimeout enabled, got %d", len(fs))
	}
	if got := observableWrites(s, fs[0].fire); len(got) != 1 || got[0] != "a" {
		t.Fatalf("promiseTimeout should observably write exactly [a], got %v", got)
	}
	// declare nothing, as listener and taskRetryTimeout legitimately do, and the write must show
	declared := map[string]bool{}
	for _, id := range observableWrites(s, fs[0].fire) {
		if !declared[id] {
			return // caught, as it must be
		}
	}
	t.Fatal("an understated affects went undetected")
}

// armPromiseTimeout is the state the arming obligation exists for: a promise past its
// deadline carrying an awaiter on its callback ledger, and that awaiter
// suspended on it. Firing promiseTimeout settles the promise, which is what ENABLES
// callback, which wakes the task — so promiseTimeout can reach a write to `x` without ever
// writing `x` itself.
func armPromiseTimeout() (*ServerState, uint64) {
	ext := Tags{"resonate:external": "true"}
	tgt := Tags{"resonate:target": "poll://any@w1"}
	return &ServerState{
		Objects: []*Object{
			{ID: "a", Promise: &Promise{State: Pending, Tags: ext, TimeoutAt: 250, CreatedAt: 100,
				Callbacks: []string{"x"}}},
			{ID: "x", Promise: &Promise{State: Pending, Tags: tgt, TimeoutAt: 5000, CreatedAt: 100}, Task: &Task{State: TaskSuspended, Version: 1}},
		},
	}, 300
}

func TestArmingReachesThroughPromiseTimeout(t *testing.T) {
	s, now := armPromiseTimeout()
	fs := enabledFirings(s, now)
	var pt firing
	for _, f := range fs {
		if strings.HasPrefix(f.name, "promiseTimeout") {
			pt = f
		}
	}
	if pt.fire == nil {
		t.Fatal("promiseTimeout not enabled — the fixture does not test what it claims")
	}

	reach, complete := reachableWrites(s, now, pt.fire, 4000)
	if !complete {
		t.Fatal("search truncated on a two-promise state")
	}
	// promiseTimeout writes only `a`; it REACHES `x` by arming callback.
	direct := observableWrites(s, pt.fire)
	if len(direct) != 1 || direct[0] != "a" {
		t.Fatalf("promiseTimeout should write only [a], got %v", direct)
	}
	if len(reach) != 2 || reach[0] != "a" || reach[1] != "x" {
		t.Fatalf("promiseTimeout should reach [a x], got %v", reach)
	}

	// as declared, it is sound
	if bad, _ := ArmingSound(s, now, 4000); bad != "" {
		t.Errorf("declared affects should be sound: %s", bad)
	}
}

// The historical bug, reproduced: drop the callbacks from promiseTimeout's affects —
// leaving exactly the set it WRITES — and the check must catch it. Without
// this, the check above only shows that today's table passes.
func TestArmingCatchesTheHistoricalBug(t *testing.T) {
	s, now := armPromiseTimeout()
	var pt firing
	for _, f := range enabledFirings(s, now) {
		if strings.HasPrefix(f.name, "promiseTimeout") {
			pt = f
		}
	}
	understated := firing{internalStep: pt.internalStep, affects: []string{"a"}} // the write set alone
	if bad := armingViolation(s, now, understated, 4000); bad == "" {
		t.Fatal("promiseTimeout with only its write set declared went undetected — " +
			"this is the shape that made the Lean cone unsound")
	}
}
