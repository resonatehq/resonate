package resonate

// Timer tasks (durable sleep).
//
// A resonate:timer promise is a durable sleep: the wake IS its deadline, and
// resonate:timer makes timing out settle it *resolved*. It carries a
// resonate:target only because the server refuses to schedule a deadline for
// a promise without one — which also spawns a task, dispatched right away
// rather than at the wake. That task names no function, so Core must neither
// run it nor hand it back (a release is re-dispatched immediately, which
// would spin): it drops it and lets the deadline do the waking.

import (
	stdctx "context"
	"sync/atomic"
	"testing"
)

// countingHeartbeat counts Start calls so a test can assert Core never took
// the lease.
type countingHeartbeat struct{ started atomic.Int64 }

func (h *countingHeartbeat) Start(string, int64) { h.started.Add(1) }
func (h *countingHeartbeat) Stop(string)         {}
func (h *countingHeartbeat) Shutdown()           {}

func timerPromise(id string, state PromiseState) PromiseRecord {
	return PromiseRecord{
		ID:        id,
		State:     state,
		TimeoutAt: 1 << 50,
		Tags: map[string]string{
			"resonate:branch": id,
			"resonate:target": "any",
			"resonate:timer":  "true",
		},
	}
}

func TestNotYetDueTimerTaskIsDropped(t *testing.T) {
	hb := &countingHeartbeat{}
	// A nil sender proves the drop happens before any server interaction:
	// no fulfill (that would end the sleep early), no suspend, no release.
	core := NewCore(nil, NewCodec(nil), NewRegistry(), nil, hb, "pid", 10_000)

	status, err := core.ExecuteUntilBlocked(stdctx.Background(), "t-pending", 0,
		timerPromise("t-pending", PromiseStatePending), nil, nil)
	if err != nil {
		t.Fatalf("ExecuteUntilBlocked: %v", err)
	}
	if status != StatusSuspended {
		t.Fatalf("status = %v, want StatusSuspended", status)
	}
	// Dropped before the lease was taken, so nothing is heartbeating a task
	// this worker is not working on.
	if got := hb.started.Load(); got != 0 {
		t.Fatalf("heartbeat started %d times, want 0", got)
	}
}

func TestDueTimerReportsSettlementWithoutDecoding(t *testing.T) {
	// A delivery already in flight when the deadline settled the promise. A
	// timer's empty param holds no TaskData, so this has to short-circuit
	// before the decode rather than fail on it.
	codec := NewCodec(nil)
	core := NewCore(nil, codec, NewRegistry(), nil, NoopHeartbeat{}, "pid", 10_000)

	settled := timerPromise("t-due", PromiseStateResolved)
	eff := NewEffects(newFakeFenceClient(), "t-due", 0, "t-due", nil)

	outcome, err := core.executeUntilBlockedInner(stdctx.Background(), settled, "t-due", eff, nil)
	if err != nil {
		t.Fatalf("executeUntilBlockedInner: %v", err)
	}
	if outcome.kind != execFulfill {
		t.Fatalf("outcome kind = %v, want execFulfill", outcome.kind)
	}
	if outcome.settleState != SettleStateResolved {
		t.Fatalf("settle state = %v, want resolved", outcome.settleState)
	}
}
