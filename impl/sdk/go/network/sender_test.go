package network

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	resonate "github.com/resonatehq/resonate-sdk-go"
)

func newTestSender(t *testing.T) (*Sender, func()) {
	t.Helper()
	pid := "w1"
	ln := NewLocal("test", &pid)
	ctx, cancel := context.WithCancel(context.Background())
	if err := ln.Start(ctx); err != nil {
		cancel()
		t.Fatal(err)
	}
	return NewSender(ln, nil), func() {
		cancel()
		_ = ln.Stop()
	}
}

func TestSenderPromiseCreateGetRoundtrip(t *testing.T) {
	s, stop := newTestSender(t)
	defer stop()
	ctx := context.Background()

	codec := resonate.NewCodec(nil)
	paramV, err := codec.Encode(map[string]any{"hello": "world"})
	if err != nil {
		t.Fatal(err)
	}
	created, err := s.PromiseCreate(ctx, resonate.PromiseCreateReq{
		ID:        "p:roundtrip",
		TimeoutAt: int64(1) << 50,
		Param:     paramV,
		Tags:      map[string]string{},
	})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if created.ID != "p:roundtrip" || created.State != resonate.PromiseStatePending {
		t.Errorf("create returned %+v", created)
	}

	got, err := s.PromiseGet(ctx, "p:roundtrip")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	decoded, err := codec.DecodePromise(got)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	var payload map[string]any
	_ = json.Unmarshal(decoded.Param.Data, &payload)
	if payload["hello"] != "world" {
		t.Errorf("decoded param = %v, want hello=world", payload)
	}
}

func TestSenderTaskCreateReturnsConflictOnDuplicate(t *testing.T) {
	s, stop := newTestSender(t)
	defer stop()
	ctx := context.Background()

	req := resonate.PromiseCreateReq{
		ID:        "task:dup",
		TimeoutAt: int64(1) << 50,
		Tags:      map[string]string{},
	}
	first, err := s.TaskCreate(ctx, "w1", 10_000, req)
	if err != nil {
		t.Fatalf("first: %v", err)
	}
	if first.Created == nil || first.Conflict {
		t.Errorf("first call: expected Created, got %+v", first)
	}

	second, err := s.TaskCreate(ctx, "w1", 10_000, req)
	if err != nil {
		t.Fatalf("second: %v", err)
	}
	if !second.Conflict {
		t.Errorf("second call: expected Conflict, got %+v", second)
	}
}

func TestSenderTaskSuspendReturnsRedirectOnSettledDependency(t *testing.T) {
	s, stop := newTestSender(t)
	defer stop()
	ctx := context.Background()

	// Set up a root task that we can suspend.
	if _, err := s.TaskCreate(ctx, "w1", 10_000, resonate.PromiseCreateReq{
		ID:        "root",
		TimeoutAt: int64(1) << 50,
		Tags:      map[string]string{},
	}); err != nil {
		t.Fatalf("task.create root: %v", err)
	}

	// Create a dependency promise and settle it.
	if _, err := s.PromiseCreate(ctx, resonate.PromiseCreateReq{
		ID: "dep", TimeoutAt: int64(1) << 50, Tags: map[string]string{},
	}); err != nil {
		t.Fatalf("create dep: %v", err)
	}
	if _, err := s.PromiseSettle(ctx, resonate.PromiseSettleReq{
		ID: "dep", State: resonate.SettleStateResolved,
	}); err != nil {
		t.Fatalf("settle dep: %v", err)
	}

	res, err := s.TaskSuspend(ctx, "root", 0, []resonate.PromiseRegisterCallbackData{
		{Awaited: "dep", Awaiter: "root"},
	})
	if err != nil {
		t.Fatalf("suspend: %v", err)
	}
	if !res.Redirected {
		t.Errorf("expected redirect, got %+v", res)
	}
}

func TestSenderTaskFenceCreate(t *testing.T) {
	s, stop := newTestSender(t)
	defer stop()
	ctx := context.Background()

	// Acquire a task to fence against. TaskCreate returns the task already in
	// Acquired state at version 0.
	created, err := s.TaskCreate(ctx, "w1", 10_000, resonate.PromiseCreateReq{
		ID:        "root",
		TimeoutAt: int64(1) << 50,
		Tags:      map[string]string{},
	})
	if err != nil {
		t.Fatalf("task.create: %v", err)
	}

	res, err := s.TaskFenceCreate(ctx, created.Created.Task.ID, created.Created.Task.Version, resonate.PromiseCreateReq{
		ID:        "child",
		TimeoutAt: int64(1) << 50,
		Tags:      map[string]string{},
	})
	if err != nil {
		t.Fatalf("TaskFenceCreate: %v", err)
	}
	if res.Promise.ID != "child" || res.Promise.State != resonate.PromiseStatePending {
		t.Errorf("fence promise = %+v, want id=child pending", res.Promise)
	}

	got, err := s.PromiseGet(ctx, "child")
	if err != nil {
		t.Fatalf("PromiseGet: %v", err)
	}
	if got.ID != "child" {
		t.Errorf("PromiseGet returned %+v", got)
	}
}

func TestSenderTaskFenceSettle(t *testing.T) {
	s, stop := newTestSender(t)
	defer stop()
	ctx := context.Background()

	created, err := s.TaskCreate(ctx, "w1", 10_000, resonate.PromiseCreateReq{
		ID:        "root",
		TimeoutAt: int64(1) << 50,
		Tags:      map[string]string{},
	})
	if err != nil {
		t.Fatalf("task.create: %v", err)
	}
	if _, err := s.PromiseCreate(ctx, resonate.PromiseCreateReq{
		ID:        "child",
		TimeoutAt: int64(1) << 50,
		Tags:      map[string]string{},
	}); err != nil {
		t.Fatalf("create child: %v", err)
	}

	res, err := s.TaskFenceSettle(ctx, created.Created.Task.ID, created.Created.Task.Version, resonate.PromiseSettleReq{
		ID:    "child",
		State: resonate.SettleStateResolved,
	})
	if err != nil {
		t.Fatalf("TaskFenceSettle: %v", err)
	}
	if res.Promise.ID != "child" || res.Promise.State != resonate.PromiseStateResolved {
		t.Errorf("fence promise = %+v, want id=child resolved", res.Promise)
	}
}

func TestSenderTaskFenceWrongVersionReturnsConflict(t *testing.T) {
	s, stop := newTestSender(t)
	defer stop()
	ctx := context.Background()

	created, err := s.TaskCreate(ctx, "w1", 10_000, resonate.PromiseCreateReq{
		ID:        "root",
		TimeoutAt: int64(1) << 50,
		Tags:      map[string]string{},
	})
	if err != nil {
		t.Fatalf("task.create: %v", err)
	}

	_, err = s.TaskFenceCreate(ctx, created.Created.Task.ID, created.Created.Task.Version+1, resonate.PromiseCreateReq{
		ID:        "child",
		TimeoutAt: int64(1) << 50,
		Tags:      map[string]string{},
	})
	var se *resonate.ServerError
	if !errors.As(err, &se) || se.Code != 409 {
		t.Fatalf("expected ServerError(409), got %T %v", err, err)
	}
}

func TestSenderRecvDropsMalformedFrames(t *testing.T) {
	pid := "w1"
	stub := &stubNetwork{}
	s := NewSender(stub, nil)
	var received int32
	s.Recv(func(Message) { atomic.AddInt32(&received, 1) })

	// Push a bogus frame followed by a valid one.
	stub.push(`not json`)
	stub.push(`{"kind":"execute","data":{"task":{"id":"t1","version":3}}}`)

	// Wait briefly for the goroutine to run callbacks.
	time.Sleep(20 * time.Millisecond)
	if got := atomic.LoadInt32(&received); got != 1 {
		t.Errorf("received = %d, want 1 (malformed should be dropped)", got)
	}
	_ = pid
}

func TestSenderCorrIDMismatchSurfacesAsServerError(t *testing.T) {
	stub := &fixedRespNetwork{
		respKind: "promise.create",
		// Deliberately wrong corrId.
		respCorrID: "WRONG",
	}
	s := NewSender(stub, nil)
	_, err := s.PromiseGet(context.Background(), "x")
	if err == nil {
		t.Fatal("expected error")
	}
	var se *resonate.ServerError
	if !errors.As(err, &se) || se.Code != 500 {
		t.Fatalf("expected ServerError(500), got %T %v", err, err)
	}
}

// ──────────────────────────────────────────────────────────────────────────
// Test doubles for Network
// ──────────────────────────────────────────────────────────────────────────

// stubNetwork captures the most recent subscriber callback and lets the test
// invoke it. It is intentionally minimal: Send is unimplemented.
type stubNetwork struct {
	mu  sync.Mutex
	cbs []func(string)
}

func (n *stubNetwork) push(raw string) {
	n.mu.Lock()
	cbs := make([]func(string), len(n.cbs))
	copy(cbs, n.cbs)
	n.mu.Unlock()
	for _, cb := range cbs {
		cb(raw)
	}
}
func (n *stubNetwork) Recv(cb func(raw string)) {
	n.mu.Lock()
	n.cbs = append(n.cbs, cb)
	n.mu.Unlock()
}
func (n *stubNetwork) PID() string                                 { return "stub" }
func (n *stubNetwork) Group() string                               { return "stub" }
func (n *stubNetwork) Unicast() string                             { return "stub://uni" }
func (n *stubNetwork) Anycast() string                             { return "stub://any" }
func (n *stubNetwork) Start(ctx context.Context) error             { return nil }
func (n *stubNetwork) Stop() error                                 { return nil }
func (n *stubNetwork) Send(ctx context.Context, req string) (string, error) {
	return "", errors.New("not implemented")
}
func (n *stubNetwork) TargetResolver(target string) string { return "stub://any@" + target }

// fixedRespNetwork returns a fixed envelope shape regardless of input — used
// to inject a corrId mismatch.
type fixedRespNetwork struct {
	stubNetwork
	respKind   string
	respCorrID string
}

func (n *fixedRespNetwork) Send(ctx context.Context, req string) (string, error) {
	resp := map[string]any{
		"kind": n.respKind,
		"head": map[string]any{
			"corrId":  n.respCorrID,
			"status":  200,
			"version": resonate.ProtocolVersion,
		},
		"data": map[string]any{"promise": nil},
	}
	body, _ := json.Marshal(resp)
	return string(body), nil
}
