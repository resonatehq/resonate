package resonate_test

import (
	"context"
	"errors"
	"testing"

	"github.com/resonatehq/resonate-sdk-go"
	"github.com/resonatehq/resonate-sdk-go/localnet"
)

// newEffectsWithSender boots a localnet + sender, creates a root task tagged
// with branch tag b, and returns Effects holding the live lease.
func newEffectsWithSender(t *testing.T, branch string) (*resonate.Effects, *resonate.Sender, func()) {
	t.Helper()
	pid := "w1"
	ln := localnet.NewLocal("test", &pid)
	ctx, cancel := context.WithCancel(context.Background())
	if err := ln.Start(ctx); err != nil {
		cancel()
		t.Fatal(err)
	}
	cleanup := func() {
		cancel()
		_ = ln.Stop()
	}

	s := resonate.NewSender(ln, nil)
	tags := map[string]string{"resonate:branch": branch}
	res, err := s.TaskCreate(ctx, pid, 10_000, resonate.PromiseCreateReq{
		ID:        "root",
		TimeoutAt: int64(1) << 50,
		Tags:      tags,
	})
	if err != nil {
		cleanup()
		t.Fatalf("task.create root: %v", err)
	}
	e := resonate.NewEffects(s, res.Created.Task.ID, res.Created.Task.Version)
	return e, s, cleanup
}

func TestEffects_CreatePromise_RoundTrip(t *testing.T) {
	e, _, stop := newEffectsWithSender(t, "B")
	defer stop()

	rec, err := e.CreatePromise(context.Background(), resonate.PromiseCreateReq{
		ID:        "child",
		TimeoutAt: int64(1) << 50,
		Tags:      map[string]string{"resonate:branch": "B"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if rec.ID != "child" || rec.State != resonate.PromiseStatePending {
		t.Fatalf("unexpected record: %+v", rec)
	}
	cached, ok := e.Cached("child")
	if !ok {
		t.Fatal("created record should be in cache")
	}
	if cached.State != resonate.PromiseStatePending {
		t.Fatalf("cached state: %v", cached.State)
	}
}

func TestEffects_SettlePromise_RoundTrip(t *testing.T) {
	e, _, stop := newEffectsWithSender(t, "B")
	defer stop()
	ctx := context.Background()

	if _, err := e.CreatePromise(ctx, resonate.PromiseCreateReq{
		ID:        "child",
		TimeoutAt: int64(1) << 50,
		Tags:      map[string]string{"resonate:branch": "B"},
	}); err != nil {
		t.Fatal(err)
	}

	rec, err := e.SettlePromise(ctx, resonate.PromiseSettleReq{
		ID: "child", State: resonate.SettleStateResolved,
	})
	if err != nil {
		t.Fatal(err)
	}
	if rec.State != resonate.PromiseStateResolved {
		t.Fatalf("settle returned %v", rec.State)
	}
	cached, _ := e.Cached("child")
	if cached.State != resonate.PromiseStateResolved {
		t.Fatalf("cached after settle: %v", cached.State)
	}
}

func TestEffects_VersionMismatchSurfacesAsServerError(t *testing.T) {
	// Boot localnet manually so we can craft Effects with a wrong version.
	pid := "w1"
	ln := localnet.NewLocal("test", &pid)
	ctx, cancel := context.WithCancel(context.Background())
	if err := ln.Start(ctx); err != nil {
		cancel()
		t.Fatal(err)
	}
	defer func() { cancel(); _ = ln.Stop() }()

	s := resonate.NewSender(ln, nil)
	if _, err := s.TaskCreate(ctx, pid, 10_000, resonate.PromiseCreateReq{
		ID:        "root",
		TimeoutAt: int64(1) << 50,
		Tags:      map[string]string{"resonate:branch": "B"},
	}); err != nil {
		t.Fatalf("task.create: %v", err)
	}

	// Wrong version (real lease is 0).
	e := resonate.NewEffects(s, "root", 99)
	_, err := e.CreatePromise(ctx, resonate.PromiseCreateReq{
		ID:        "child",
		TimeoutAt: int64(1) << 50,
		Tags:      map[string]string{"resonate:branch": "B"},
	})
	var se *resonate.ServerError
	if !errors.As(err, &se) || se.Code != 409 {
		t.Fatalf("expected ServerError(409), got %T %v", err, err)
	}
	if _, ok := e.Cached("child"); ok {
		t.Fatal("failed create should not populate cache")
	}
}

func TestEffects_PreloadAbsorbedIntoCache(t *testing.T) {
	e, s, stop := newEffectsWithSender(t, "B")
	defer stop()
	ctx := context.Background()

	// Create a sibling on branch B and settle it directly via the sender so it
	// shows up in the preload bundle of subsequent fence calls.
	if _, err := s.PromiseCreate(ctx, resonate.PromiseCreateReq{
		ID:        "sibling",
		TimeoutAt: int64(1) << 50,
		Tags:      map[string]string{"resonate:branch": "B"},
	}); err != nil {
		t.Fatalf("create sibling: %v", err)
	}
	if _, err := s.PromiseSettle(ctx, resonate.PromiseSettleReq{
		ID: "sibling", State: resonate.SettleStateResolved,
	}); err != nil {
		t.Fatalf("settle sibling: %v", err)
	}

	// Effects.CreatePromise issues a TaskFence; the preload bundle should carry
	// the resolved sibling, which absorb stores in the cache.
	if _, err := e.CreatePromise(ctx, resonate.PromiseCreateReq{
		ID:        "child",
		TimeoutAt: int64(1) << 50,
		Tags:      map[string]string{"resonate:branch": "B"},
	}); err != nil {
		t.Fatal(err)
	}

	got, ok := e.Cached("sibling")
	if !ok {
		t.Fatal("expected sibling absorbed into cache from preload")
	}
	if got.State != resonate.PromiseStateResolved {
		t.Fatalf("cached sibling state: %v", got.State)
	}
}

// The "don't clobber terminal with stale Pending" property is an internal
// invariant of absorb. localnet never produces a stale preload snapshot, so we
// keep one targeted test using an inline stub client that does.
func TestEffects_AbsorbDoesNotClobberTerminalWithPending(t *testing.T) {
	stub := &scriptedFenceClient{}
	e := resonate.NewEffects(stub, "root", 0)

	// First call: preload carries terminal child.
	stub.next = resonate.TaskFenceResult{
		Promise: resonate.PromiseRecord{ID: "p1", State: resonate.PromiseStatePending},
		Preload: []resonate.PromiseRecord{{ID: "child", State: resonate.PromiseStateResolved}},
	}
	if _, err := e.CreatePromise(context.Background(), resonate.PromiseCreateReq{ID: "p1"}); err != nil {
		t.Fatal(err)
	}

	// Second call: preload carries a stale Pending view of the same child.
	stub.next = resonate.TaskFenceResult{
		Promise: resonate.PromiseRecord{ID: "p2", State: resonate.PromiseStatePending},
		Preload: []resonate.PromiseRecord{{ID: "child", State: resonate.PromiseStatePending}},
	}
	if _, err := e.CreatePromise(context.Background(), resonate.PromiseCreateReq{ID: "p2"}); err != nil {
		t.Fatal(err)
	}

	got, ok := e.Cached("child")
	if !ok {
		t.Fatal("expected child in cache")
	}
	if got.State != resonate.PromiseStateResolved {
		t.Fatalf("terminal record was clobbered by stale Pending: got %v", got.State)
	}
}

// scriptedFenceClient returns whatever TaskFenceResult is staged in next.
type scriptedFenceClient struct {
	next resonate.TaskFenceResult
}

func (c *scriptedFenceClient) TaskFenceCreate(_ context.Context, _ string, _ int64, _ resonate.PromiseCreateReq) (resonate.TaskFenceResult, error) {
	return c.next, nil
}
func (c *scriptedFenceClient) TaskFenceSettle(_ context.Context, _ string, _ int64, _ resonate.PromiseSettleReq) (resonate.TaskFenceResult, error) {
	return c.next, nil
}
