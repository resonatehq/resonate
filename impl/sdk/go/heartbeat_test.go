package resonate_test

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/resonatehq/resonate-sdk-go"
)

// recordingNetwork captures every Send call and returns a canned 200 envelope
// whose kind/corrId match the request. It's enough to drive AsyncHeartbeat
// through the real Sender without standing up localnet.
type recordingNetwork struct {
	stubNetwork
	mu       sync.Mutex
	requests []map[string]any
}

func (n *recordingNetwork) snapshot() []map[string]any {
	n.mu.Lock()
	defer n.mu.Unlock()
	out := make([]map[string]any, len(n.requests))
	copy(out, n.requests)
	return out
}

func (n *recordingNetwork) Send(ctx context.Context, req string) (string, error) {
	var raw map[string]any
	if err := json.Unmarshal([]byte(req), &raw); err != nil {
		return "", err
	}
	n.mu.Lock()
	n.requests = append(n.requests, raw)
	n.mu.Unlock()

	kind, _ := raw["kind"].(string)
	head, _ := raw["head"].(map[string]any)
	corrID, _ := head["corrId"].(string)

	resp := map[string]any{
		"kind": kind,
		"head": map[string]any{
			"corrId":  corrID,
			"status":  200,
			"version": resonate.ProtocolVersion,
		},
		"data": map[string]any{},
	}
	body, _ := json.Marshal(resp)
	return string(body), nil
}

func newTestHeartbeat(t *testing.T) (*resonate.AsyncHeartbeat, *recordingNetwork) {
	t.Helper()
	net := &recordingNetwork{}
	s := resonate.NewSender(net, nil)
	hb := resonate.NewAsyncHeartbeat("test-pid", 50*time.Millisecond, s)
	t.Cleanup(hb.Shutdown)
	return hb, net
}

func heartbeatRequests(reqs []map[string]any) []map[string]any {
	out := []map[string]any{}
	for _, r := range reqs {
		if k, _ := r["kind"].(string); k == "task.heartbeat" {
			out = append(out, r)
		}
	}
	return out
}

func TestAsyncHeartbeatStartAddsTask(t *testing.T) {
	hb, _ := newTestHeartbeat(t)
	hb.Start("task-1", 1)

	tracked := hb.TrackedTasks()
	if tracked["task-1"] != 1 {
		t.Errorf("tracked[task-1] = %d, want 1", tracked["task-1"])
	}
	if len(tracked) != 1 {
		t.Errorf("len(tracked) = %d, want 1", len(tracked))
	}
}

func TestAsyncHeartbeatStartMultipleTasks(t *testing.T) {
	hb, _ := newTestHeartbeat(t)
	hb.Start("task-1", 1)
	hb.Start("task-2", 5)
	hb.Start("task-3", 10)

	tracked := hb.TrackedTasks()
	if len(tracked) != 3 {
		t.Fatalf("tracked len = %d, want 3", len(tracked))
	}
	if tracked["task-1"] != 1 || tracked["task-2"] != 5 || tracked["task-3"] != 10 {
		t.Errorf("tracked = %+v", tracked)
	}
}

func TestAsyncHeartbeatStopRemovesTask(t *testing.T) {
	hb, _ := newTestHeartbeat(t)
	hb.Start("task-1", 1)
	hb.Start("task-2", 2)
	hb.Stop("task-1")

	tracked := hb.TrackedTasks()
	if _, ok := tracked["task-1"]; ok {
		t.Errorf("task-1 still tracked after Stop")
	}
	if tracked["task-2"] != 2 {
		t.Errorf("task-2 = %d, want 2", tracked["task-2"])
	}
}

func TestAsyncHeartbeatStopNonexistentIsHarmless(t *testing.T) {
	hb, _ := newTestHeartbeat(t)
	hb.Start("task-1", 1)
	hb.Stop("nonexistent")
	if got := len(hb.TrackedTasks()); got != 1 {
		t.Errorf("len(tracked) = %d, want 1", got)
	}
}

func TestAsyncHeartbeatStartSameTaskUpdatesVersion(t *testing.T) {
	hb, _ := newTestHeartbeat(t)
	hb.Start("task-1", 1)
	hb.Start("task-1", 5)

	tracked := hb.TrackedTasks()
	if len(tracked) != 1 {
		t.Errorf("tracked len = %d, want 1", len(tracked))
	}
	if tracked["task-1"] != 5 {
		t.Errorf("tracked[task-1] = %d, want 5", tracked["task-1"])
	}
}

func TestAsyncHeartbeatSendsTrackedTasks(t *testing.T) {
	hb, net := newTestHeartbeat(t)
	hb.Start("task-1", 1)
	hb.Start("task-2", 5)

	time.Sleep(120 * time.Millisecond)

	hbs := heartbeatRequests(net.snapshot())
	if len(hbs) == 0 {
		t.Fatal("expected at least one task.heartbeat send")
	}

	last := hbs[len(hbs)-1]
	data, _ := last["data"].(map[string]any)
	if pid, _ := data["pid"].(string); pid != "test-pid" {
		t.Errorf("pid = %q, want test-pid", pid)
	}

	rawTasks, _ := data["tasks"].([]any)
	if len(rawTasks) != 2 {
		t.Fatalf("tasks len = %d, want 2", len(rawTasks))
	}
	ids := map[string]bool{}
	for _, raw := range rawTasks {
		ref, _ := raw.(map[string]any)
		id, _ := ref["id"].(string)
		ids[id] = true
	}
	if !ids["task-1"] || !ids["task-2"] {
		t.Errorf("missing tracked ids in heartbeat: %+v", rawTasks)
	}
}

func TestAsyncHeartbeatReflectsTaskRemoval(t *testing.T) {
	hb, net := newTestHeartbeat(t)
	hb.Start("task-1", 1)
	hb.Start("task-2", 2)

	time.Sleep(80 * time.Millisecond)
	hb.Stop("task-1")
	time.Sleep(80 * time.Millisecond)

	hbs := heartbeatRequests(net.snapshot())
	if len(hbs) == 0 {
		t.Fatal("expected at least one task.heartbeat send")
	}

	last := hbs[len(hbs)-1]
	data, _ := last["data"].(map[string]any)
	rawTasks, _ := data["tasks"].([]any)
	if len(rawTasks) != 1 {
		t.Fatalf("last heartbeat tasks len = %d, want 1", len(rawTasks))
	}
	ref, _ := rawTasks[0].(map[string]any)
	if id, _ := ref["id"].(string); id != "task-2" {
		t.Errorf("last heartbeat id = %q, want task-2", id)
	}
}

func TestAsyncHeartbeatNoSendWhenEmpty(t *testing.T) {
	_, net := newTestHeartbeat(t)
	time.Sleep(120 * time.Millisecond)

	if got := len(heartbeatRequests(net.snapshot())); got != 0 {
		t.Errorf("heartbeat sends with empty set = %d, want 0", got)
	}
}

// TestAsyncHeartbeatStopsTickerWhenSetEmpties guards the resource-management
// guarantee: removing the last task tears down the ticker so the actor stops
// firing heartbeats until a new Start re-arms it.
func TestAsyncHeartbeatStopsTickerWhenSetEmpties(t *testing.T) {
	hb, net := newTestHeartbeat(t)

	hb.Start("task-1", 1)
	time.Sleep(120 * time.Millisecond)
	hb.Stop("task-1")

	before := len(heartbeatRequests(net.snapshot()))
	time.Sleep(150 * time.Millisecond)
	after := len(heartbeatRequests(net.snapshot()))

	if after != before {
		t.Errorf("heartbeats kept firing after last task removed: before=%d after=%d", before, after)
	}

	// Re-arming with a new Start must resume sends.
	hb.Start("task-2", 2)
	time.Sleep(120 * time.Millisecond)
	resumed := len(heartbeatRequests(net.snapshot()))
	if resumed <= after {
		t.Errorf("heartbeat did not resume after re-Start: after=%d resumed=%d", after, resumed)
	}
}

func TestAsyncHeartbeatShutdownStopsSends(t *testing.T) {
	net := &recordingNetwork{}
	hb := resonate.NewAsyncHeartbeat("test-pid", 50*time.Millisecond, resonate.NewSender(net, nil))
	hb.Start("task-1", 1)
	time.Sleep(80 * time.Millisecond)
	hb.Shutdown()

	before := len(heartbeatRequests(net.snapshot()))
	time.Sleep(120 * time.Millisecond)
	after := len(heartbeatRequests(net.snapshot()))

	if after != before {
		t.Errorf("heartbeats kept firing after Shutdown: before=%d after=%d", before, after)
	}
	if hb.TrackedTasks() != nil {
		t.Errorf("TrackedTasks should return nil after Shutdown")
	}
}

func TestAsyncHeartbeatShutdownIsIdempotent(t *testing.T) {
	net := &recordingNetwork{}
	hb := resonate.NewAsyncHeartbeat("test-pid", 50*time.Millisecond, resonate.NewSender(net, nil))
	hb.Shutdown()
	hb.Shutdown()
}

func TestNoopHeartbeatIsHarmless(t *testing.T) {
	var hb resonate.Heartbeat = resonate.NoopHeartbeat{}
	hb.Start("task-1", 1)
	hb.Start("task-2", 2)
	hb.Stop("task-1")
	hb.Stop("nonexistent")
}
