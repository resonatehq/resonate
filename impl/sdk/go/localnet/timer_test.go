package localnet

// Scheduler invariant: only external and runnable promises are timed out.
//
// Mirrors the server: a pending promise someone can be blocked on — one with a
// resonate:target (runnable), or global, explicitly external, or a timer — has
// its deadline scheduled; an internal one must NOT. Divergence here is
// invisible in ordinary tests — a simulation that schedules timeouts for
// *every* promise simply lets more things succeed than the real server does —
// so it is asserted directly against the state machine.

import (
	"testing"
	"time"
)

func createPromiseReq(id string, timeoutAt int64, tags map[string]any) map[string]any {
	return map[string]any{
		"kind":      "promise.create",
		"corrId":    id,
		"id":        id,
		"timeoutAt": timeoutAt,
		"param":     map[string]any{},
		"tags":      tags,
	}
}

func mustApply(t *testing.T, s *serverState, now int64, req map[string]any) {
	t.Helper()
	if _, _, err := s.apply(now, req); err != nil {
		t.Fatalf("apply %v: %v", req["kind"], err)
	}
}

func TestOnlyExternalAndRunnablePromisesAreScheduledForTimeout(t *testing.T) {
	s := newServerState()
	now := time.Now().UnixMilli()
	deadline := now + 60_000

	mustApply(t, s, now, createPromiseReq("internal", deadline, map[string]any{}))
	mustApply(t, s, now, createPromiseReq("global", deadline,
		map[string]any{"resonate:scope": "global"}))
	mustApply(t, s, now, createPromiseReq("timer", deadline,
		map[string]any{"resonate:timer": "true"}))
	mustApply(t, s, now, createPromiseReq("runnable", deadline,
		map[string]any{"resonate:target": "poll://any@default"}))

	scheduled := map[string]bool{}
	for _, pt := range s.pTimeouts {
		scheduled[pt.id] = true
	}
	for _, id := range []string{"global", "timer", "runnable"} {
		if !scheduled[id] {
			t.Errorf("%s promise not scheduled for timeout", id)
		}
	}
	if scheduled["internal"] {
		t.Error("internal promise scheduled for timeout")
	}
}

func TestTickResolvesATimerWithoutATarget(t *testing.T) {
	s := newServerState()
	now := time.Now().UnixMilli()
	deadline := now + 60_000

	mustApply(t, s, now, createPromiseReq("internal", deadline, map[string]any{}))
	mustApply(t, s, now, createPromiseReq("timer", deadline,
		map[string]any{"resonate:scope": "global", "resonate:timer": "true"}))

	s.tick(deadline + 1)

	// The timer fires — and resonate:timer settles it RESOLVED, which is what
	// wakes a sleeping workflow. The internal promise is left alone.
	if got := s.promises["timer"].State; string(got) != "resolved" {
		t.Errorf("timer state = %q, want resolved", got)
	}
	if got := s.promises["internal"].State; string(got) != "pending" {
		t.Errorf("internal state = %q, want pending", got)
	}
}

func TestATimerWithATargetIsRefused(t *testing.T) {
	s := newServerState()
	now := time.Now().UnixMilli()
	req := createPromiseReq("timer", now+60_000, map[string]any{
		"resonate:target": "poll://any@default",
		"resonate:timer":  "true",
	})
	if _, _, err := s.apply(now, req); err == nil {
		t.Fatal("a timer with a target was accepted")
	}
	if _, ok := s.promises["timer"]; ok {
		t.Fatal("a refused timer was created")
	}
}
