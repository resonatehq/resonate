package network

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	resonate "github.com/resonatehq/resonate-sdk-go"
)

// sendEnvelope is a small test helper that wraps a flat data payload into a
// protocol envelope, sends it through the network, and returns the parsed
// response envelope.
func sendEnvelope(t *testing.T, ctx context.Context, n Network, kind, corrID string, data map[string]any) map[string]any {
	t.Helper()
	env := map[string]any{
		"kind": kind,
		"head": map[string]any{
			"corrId":  corrID,
			"version": resonate.ProtocolVersion,
		},
		"data": data,
	}
	body, err := json.Marshal(env)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	resp, err := n.Send(ctx, string(body))
	if err != nil {
		t.Fatalf("send: %v", err)
	}
	var out map[string]any
	if err := json.Unmarshal([]byte(resp), &out); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	return out
}

func statusOf(resp map[string]any) int {
	head, _ := resp["head"].(map[string]any)
	switch s := head["status"].(type) {
	case float64:
		return int(s)
	case int:
		return s
	}
	return 0
}

func dataOf(resp map[string]any) map[string]any {
	d, _ := resp["data"].(map[string]any)
	return d
}

func startLocal(t *testing.T) (*LocalNetwork, context.CancelFunc) {
	t.Helper()
	pid := "p1"
	ln := NewLocal("test", &pid)
	ctx, cancel := context.WithCancel(context.Background())
	if err := ln.Start(ctx); err != nil {
		cancel()
		t.Fatal(err)
	}
	return ln, func() {
		cancel()
		_ = ln.Stop()
	}
}

func TestLocalPromiseCreateGetRoundTrip(t *testing.T) {
	ln, stop := startLocal(t)
	defer stop()
	ctx := context.Background()

	resp := sendEnvelope(t, ctx, ln, "promise.create", "c1", map[string]any{
		"id":        "p:hello",
		"timeoutAt": int64(1) << 50,
		"param":     map[string]any{"data": "AAA="},
		"tags":      map[string]any{},
	})
	if got := statusOf(resp); got != 201 {
		t.Fatalf("create status = %d, want 201", got)
	}

	resp = sendEnvelope(t, ctx, ln, "promise.get", "c2", map[string]any{"id": "p:hello"})
	if got := statusOf(resp); got != 200 {
		t.Fatalf("get status = %d, want 200", got)
	}
	p, _ := dataOf(resp)["promise"].(map[string]any)
	if p["id"] != "p:hello" {
		t.Errorf("returned promise id = %v", p["id"])
	}
	if p["state"] != "pending" {
		t.Errorf("returned state = %v, want pending", p["state"])
	}
}

func TestLocalPromiseSettleNotifiesSubscribers(t *testing.T) {
	ln, stop := startLocal(t)
	defer stop()
	ctx := context.Background()

	var (
		mu       sync.Mutex
		received []string
	)
	done := make(chan struct{}, 1)
	ln.Recv(func(raw string) {
		mu.Lock()
		received = append(received, raw)
		mu.Unlock()
		select {
		case done <- struct{}{}:
		default:
		}
	})

	sendEnvelope(t, ctx, ln, "promise.create", "c1", map[string]any{
		"id":        "p:sub",
		"timeoutAt": int64(1) << 50,
		"param":     map[string]any{},
		"tags":      map[string]any{},
	})
	sendEnvelope(t, ctx, ln, "promise.register_listener", "c2", map[string]any{
		"awaited": "p:sub",
		"address": "test://listener",
	})
	sendEnvelope(t, ctx, ln, "promise.settle", "c3", map[string]any{
		"id":    "p:sub",
		"state": "resolved",
		"value": map[string]any{"data": "done"},
	})

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("no push delivered to subscriber")
	}
	mu.Lock()
	defer mu.Unlock()
	if len(received) == 0 {
		t.Fatal("expected at least one push frame")
	}
	msg, err := DecodeMessage([]byte(received[0]))
	if err != nil {
		t.Fatalf("decode push: %v", err)
	}
	if _, ok := msg.(UnblockMessage); !ok {
		t.Errorf("expected UnblockMessage, got %T", msg)
	}
}

func TestLocalTaskCreateAndAcquirePreload(t *testing.T) {
	ln, stop := startLocal(t)
	defer stop()
	ctx := context.Background()

	// Create the root task (task.create combines promise + task acquire).
	resp := sendEnvelope(t, ctx, ln, "task.create", "c1", map[string]any{
		"pid": "worker-1",
		"ttl": 10_000,
		"action": map[string]any{
			"kind": "promise.create",
			"data": map[string]any{
				"id":        "root",
				"timeoutAt": int64(1) << 50,
				"param":     map[string]any{},
				"tags":      map[string]any{"resonate:branch": "B"},
			},
		},
	})
	if got := statusOf(resp); got != 201 {
		t.Fatalf("task.create status = %d, want 201; resp=%+v", got, resp)
	}

	// Create a sibling promise on the same branch — should appear in preload
	// when we re-acquire later.
	sendEnvelope(t, ctx, ln, "promise.create", "c2", map[string]any{
		"id":        "sibling",
		"timeoutAt": int64(1) << 50,
		"param":     map[string]any{},
		"tags":      map[string]any{"resonate:branch": "B"},
	})

	// Release the root task so we can acquire it again and see preload.
	sendEnvelope(t, ctx, ln, "task.release", "c3", map[string]any{"id": "root"})
	resp = sendEnvelope(t, ctx, ln, "task.acquire", "c4", map[string]any{
		"id": "root", "pid": "worker-1", "ttl": 10_000,
	})
	if got := statusOf(resp); got != 200 {
		t.Fatalf("acquire status = %d, want 200", got)
	}
	preload, _ := dataOf(resp)["preload"].([]any)
	if len(preload) != 1 {
		t.Errorf("expected 1 preloaded sibling, got %d", len(preload))
	}
}

func TestLocalSuspendRedirectsWhenAwaitedAlreadySettled(t *testing.T) {
	ln, stop := startLocal(t)
	defer stop()
	ctx := context.Background()

	// Root task that we will suspend.
	sendEnvelope(t, ctx, ln, "task.create", "c1", map[string]any{
		"pid": "w", "ttl": 10_000,
		"action": map[string]any{
			"kind": "promise.create",
			"data": map[string]any{"id": "root", "timeoutAt": int64(1) << 50, "param": map[string]any{}, "tags": map[string]any{}},
		},
	})
	// Dependency promise — settle it before we try to suspend.
	sendEnvelope(t, ctx, ln, "promise.create", "c2", map[string]any{
		"id": "dep", "timeoutAt": int64(1) << 50, "param": map[string]any{}, "tags": map[string]any{},
	})
	sendEnvelope(t, ctx, ln, "promise.settle", "c3", map[string]any{
		"id": "dep", "state": "resolved", "value": map[string]any{},
	})

	resp := sendEnvelope(t, ctx, ln, "task.suspend", "c4", map[string]any{
		"id": "root",
		"actions": []any{
			map[string]any{
				"kind": "promise.register_callback",
				"data": map[string]any{"awaited": "dep", "awaiter": "root"},
			},
		},
	})
	if got := statusOf(resp); got != 300 {
		t.Fatalf("suspend status = %d, want 300 (redirect)", got)
	}
	d := dataOf(resp)
	if r, _ := d["redirect"].(bool); !r {
		t.Errorf("expected redirect=true in data, got %+v", d)
	}
}

func TestLocalTaskCreateOrConflict(t *testing.T) {
	ln, stop := startLocal(t)
	defer stop()
	ctx := context.Background()

	body := map[string]any{
		"pid": "w", "ttl": 10_000,
		"action": map[string]any{
			"kind": "promise.create",
			"data": map[string]any{"id": "dup", "timeoutAt": int64(1) << 50, "param": map[string]any{}, "tags": map[string]any{}},
		},
	}
	first := sendEnvelope(t, ctx, ln, "task.create", "c1", body)
	if got := statusOf(first); got != 201 {
		t.Fatalf("first create status = %d, want 201", got)
	}
	second := sendEnvelope(t, ctx, ln, "task.create", "c2", body)
	// Promise exists + task is in Acquired state → handler returns 409.
	if got := statusOf(second); got != 409 {
		t.Fatalf("second create status = %d, want 409", got)
	}
}
