package localnet

// Scheduler invariant: only targeted promises are timed out.
//
// Mirrors the server's promise.pendingHasTimeout invariant: a pending promise
// carrying resonate:target always has a timeout scheduled, and one without a
// target must NOT. Divergence here is invisible in ordinary tests — a
// simulation that schedules timeouts for *every* promise simply lets more
// things succeed than the real server does — so it is asserted directly
// against the state machine.

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

func TestOnlyPromisesWithATargetAreScheduledForTimeout(t *testing.T) {
	s := newServerState()
	now := time.Now().UnixMilli()
	deadline := now + 60_000

	mustApply(t, s, now, createPromiseReq("no-target", deadline,
		map[string]any{"resonate:scope": "global"}))
	mustApply(t, s, now, createPromiseReq("with-target", deadline,
		map[string]any{"resonate:scope": "global", "resonate:target": "poll://any@default"}))

	scheduled := map[string]bool{}
	for _, pt := range s.pTimeouts {
		scheduled[pt.id] = true
	}
	if !scheduled["with-target"] {
		t.Error("targeted promise not scheduled for timeout")
	}
	if scheduled["no-target"] {
		t.Error("target-less promise scheduled for timeout")
	}
}

func TestTickExpiresOnlyTheTargetedPromise(t *testing.T) {
	s := newServerState()
	now := time.Now().UnixMilli()
	deadline := now + 60_000

	mustApply(t, s, now, createPromiseReq("bare", deadline,
		map[string]any{"resonate:scope": "global"}))
	mustApply(t, s, now, createPromiseReq("timer", deadline,
		map[string]any{
			"resonate:scope":  "global",
			"resonate:target": "poll://any@default",
			"resonate:timer":  "true",
		}))

	s.tick(deadline + 1)

	// The timer fires — and resonate:timer settles it RESOLVED, which is what
	// wakes a sleeping workflow. The bare promise is left alone.
	if got := s.promises["timer"].State; string(got) != "resolved" {
		t.Errorf("timer state = %q, want resolved", got)
	}
	if got := s.promises["bare"].State; string(got) != "pending" {
		t.Errorf("bare state = %q, want pending", got)
	}
}
