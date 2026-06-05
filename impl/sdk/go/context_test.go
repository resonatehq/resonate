package resonate

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// ── ID / timeout helpers ────────────────────────────────────────────────

func TestContext_NextID_Sequential(t *testing.T) {
	c := testContext("root", nil)
	for i := 1; i <= 5; i++ {
		got := c.nextID()
		want := fmt.Sprintf("root.%d", i)
		if got != want {
			t.Fatalf("want %q, got %q", want, got)
		}
	}
}

func TestContext_NextID_Concurrent(t *testing.T) {
	c := testContext("root", nil)
	const n = 256
	seen := sync.Map{}
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			id := c.nextID()
			if _, dup := seen.LoadOrStore(id, true); dup {
				t.Errorf("duplicate id %q", id)
			}
		}()
	}
	wg.Wait()
	count := 0
	seen.Range(func(_, _ any) bool { count++; return true })
	if count != n {
		t.Fatalf("expected %d ids, got %d", n, count)
	}
}

func TestContext_ChildTimeout_CapsToParent(t *testing.T) {
	now := nowMs()
	parentTimeout := now + (1 * time.Hour).Milliseconds()
	c := testContext("root", nil)
	c.timeoutAt = parentTimeout
	got := c.childTimeout(48 * time.Hour) // ask for 48h with parent at 1h
	if got != parentTimeout {
		t.Fatalf("expected cap at parent %d, got %d", parentTimeout, got)
	}
}

func TestContext_ChildTimeout_SmallerThanParent(t *testing.T) {
	now := nowMs()
	c := testContext("root", nil)
	c.timeoutAt = now + (24 * time.Hour).Milliseconds()
	got := c.childTimeout(5 * time.Minute)
	expectedLow := now + (4*time.Minute + 50*time.Second).Milliseconds()
	expectedHigh := now + (5*time.Minute + 10*time.Second).Milliseconds()
	if got < expectedLow || got > expectedHigh {
		t.Fatalf("expected ~5m from now, got delta %dms", got-now)
	}
}

func TestContext_ChildTimeout_DefaultIs24h(t *testing.T) {
	now := nowMs()
	c := testContext("root", nil)
	c.timeoutAt = 1 << 62
	got := c.childTimeout(0)
	expected := now + (24 * time.Hour).Milliseconds()
	if abs64(got-expected) > 1000 {
		t.Fatalf("expected ~24h from now (%d), got %d", expected, got)
	}
}

func abs64(x int64) int64 {
	if x < 0 {
		return -x
	}
	return x
}

// ── Request builders ────────────────────────────────────────────────────

func TestContext_LocalCreateReq_Tags(t *testing.T) {
	c := testContext("root", nil)
	req, err := c.localCreateReq("root.0", 42, 0)
	if err != nil {
		t.Fatal(err)
	}
	wantTags := map[string]string{
		"resonate:scope":  "local",
		"resonate:branch": "root",
		"resonate:parent": "root",
		"resonate:origin": "root",
	}
	for k, v := range wantTags {
		if req.Tags[k] != v {
			t.Fatalf("tag %s: want %q, got %q", k, v, req.Tags[k])
		}
	}
	if req.ID != "root.0" {
		t.Fatalf("ID: want root.0, got %s", req.ID)
	}
}

func TestContext_RemoteCreateReq_TagsAndTarget(t *testing.T) {
	c := testContext("root", nil)
	c.targetResolver = func(o *string) string {
		if o == nil {
			return "default-target"
		}
		return "resolved-" + *o
	}
	override := "custom"
	req, err := c.remoteCreateReq("root.0", "myFunc", map[string]int{"a": 1}, 0, &override)
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]string{
		"resonate:scope":  "global",
		"resonate:target": "resolved-custom",
		"resonate:branch": "root.0",
		"resonate:parent": "root",
		"resonate:origin": "root",
	}
	for k, v := range want {
		if req.Tags[k] != v {
			t.Fatalf("tag %s: want %q, got %q", k, v, req.Tags[k])
		}
	}
}

func TestContext_RemoteCreateReq_TargetDefault(t *testing.T) {
	c := testContext("root", nil)
	c.targetResolver = func(o *string) string {
		if o == nil {
			return "default"
		}
		return *o
	}
	req, _ := c.remoteCreateReq("root.0", "myFunc", nil, 0, nil)
	if req.Tags["resonate:target"] != "default" {
		t.Fatalf("expected default target, got %q", req.Tags["resonate:target"])
	}
}

func TestContext_SleepCreateReq_Tags(t *testing.T) {
	c := testContext("root", nil)
	req := c.sleepCreateReq("root.0", 30*time.Second)
	if req.Tags["resonate:timer"] != "true" {
		t.Fatalf("missing timer tag: %v", req.Tags)
	}
	if req.Tags["resonate:scope"] != "global" {
		t.Fatalf("scope: %q", req.Tags["resonate:scope"])
	}
}

func TestContext_PromiseCreateReq_NoTimerTag(t *testing.T) {
	c := testContext("root", nil)
	req, err := c.promiseCreateReq("root.0", 0, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := req.Tags["resonate:timer"]; ok {
		t.Fatal("promise should not have timer tag")
	}
}

// ── Run ─────────────────────────────────────────────────────────────────

type addArgs2 struct {
	A, B int
}

func addFn(_ *Context, a addArgs2) (int, error) { return a.A + a.B, nil }

func TestEffects_ForwardsOriginToFenceClient(t *testing.T) {
	fake := newFakeFenceClient()
	eff := NewEffects(fake, "task-1", 1, "lineage-root", nil)

	if _, err := eff.CreatePromise(context.Background(), PromiseCreateReq{
		ID: "child", TimeoutAt: 1 << 50, Tags: map[string]string{},
	}); err != nil {
		t.Fatalf("CreatePromise: %v", err)
	}
	if got := fake.lastOrigin.Load(); got != "lineage-root" {
		t.Errorf("create origin = %v, want lineage-root", got)
	}

	if _, err := eff.SettlePromise(context.Background(), PromiseSettleReq{
		ID: "child", State: SettleStateResolved,
	}); err != nil {
		t.Fatalf("SettlePromise: %v", err)
	}
	if got := fake.lastOrigin.Load(); got != "lineage-root" {
		t.Errorf("settle origin = %v, want lineage-root", got)
	}
}

func TestContext_Run_SyncCreateAndAwait(t *testing.T) {
	fake := newFakeFenceClient()
	eff := NewEffects(fake, "task-1", 1, "task-1", nil)
	ctx := testContext("root", eff)

	fut, err := ctx.Run(addFn, addArgs2{A: 2, B: 3})
	if err != nil {
		t.Fatal(err)
	}
	if fut.ID() != "root.1" {
		t.Fatalf("expected root.1, got %q", fut.ID())
	}
	if fake.createCalls.Load() != 1 {
		t.Fatalf("expected 1 create call, got %d", fake.createCalls.Load())
	}

	var got int
	if err := fut.Await(&got); err != nil {
		t.Fatal(err)
	}
	if got != 5 {
		t.Fatalf("expected 5, got %d", got)
	}
	if fake.settleCalls.Load() != 1 {
		t.Fatalf("expected 1 settle call, got %d", fake.settleCalls.Load())
	}
}

func TestContext_Run_PreSettledSkipsGoroutine(t *testing.T) {
	fake := newFakeFenceClient()
	fake.preset("root.1", resolvedPromise(t, "root.1", 99))
	eff := NewEffects(fake, "task-1", 1, "task-1", nil)
	ctx := testContext("root", eff)

	fut, err := ctx.Run(addFn, addArgs2{A: 1, B: 2})
	if err != nil {
		t.Fatal(err)
	}
	var got int
	if err := fut.Await(&got); err != nil {
		t.Fatal(err)
	}
	if got != 99 {
		t.Fatalf("expected 99 from pre-settled record, got %d", got)
	}
	if fake.settleCalls.Load() != 0 {
		t.Fatalf("pre-settled run should not call settle, got %d", fake.settleCalls.Load())
	}
}

func TestContext_Run_TwoConcurrentGoroutines(t *testing.T) {
	fake := newFakeFenceClient()
	eff := NewEffects(fake, "task-1", 1, "task-1", nil)
	ctx := testContext("root", eff)

	f1, _ := ctx.Run(addFn, addArgs2{A: 1, B: 2})
	f2, _ := ctx.Run(addFn, addArgs2{A: 10, B: 20})

	var r1, r2 int
	if err := f1.Await(&r1); err != nil {
		t.Fatal(err)
	}
	if err := f2.Await(&r2); err != nil {
		t.Fatal(err)
	}
	if r1 != 3 || r2 != 30 {
		t.Fatalf("expected 3 and 30, got %d and %d", r1, r2)
	}
}

func TestContext_Run_FunctionError(t *testing.T) {
	fake := newFakeFenceClient()
	eff := NewEffects(fake, "task-1", 1, "task-1", nil)
	ctx := testContext("root", eff)

	failing := func(*Context, int) (int, error) { return 0, errors.New("denied") }
	fut, err := ctx.Run(failing, 0)
	if err != nil {
		t.Fatal(err)
	}
	var got int
	err = fut.Await(&got)
	if err == nil {
		t.Fatal("expected error")
	}
	var app *ApplicationError
	if !errors.As(err, &app) {
		t.Fatalf("expected ApplicationError, got %T: %v", err, err)
	}
	if app.Message != "denied" {
		t.Fatalf("expected 'denied', got %q", app.Message)
	}
}

// ── RPC / Sleep / Promise: suspension ───────────────────────────────────

func TestContext_RPC_PendingSuspends(t *testing.T) {
	fake := newFakeFenceClient()
	eff := NewEffects(fake, "task-1", 1, "task-1", nil)
	ctx := testContext("root", eff)
	fut, err := ctx.RPC("payments.charge", map[string]int{"amount": 100})
	if err != nil {
		t.Fatal(err)
	}
	if fut.ID() != "root.1" {
		t.Fatalf("expected root.1, got %q", fut.ID())
	}
	assertPanicsWithSuspend(t, func() { _ = fut.Await(nil) })
	todos := ctx.drainSpawnedRemote()
	if len(todos) != 1 || todos[0] != "root.1" {
		t.Fatalf("todos: %v", todos)
	}
}

func TestContext_Sleep_PendingSuspends(t *testing.T) {
	fake := newFakeFenceClient()
	eff := NewEffects(fake, "task-1", 1, "task-1", nil)
	ctx := testContext("root", eff)
	fut, err := ctx.Sleep(time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	assertPanicsWithSuspend(t, func() { _ = fut.Await(nil) })
}

func TestContext_Promise_PendingSuspends(t *testing.T) {
	fake := newFakeFenceClient()
	eff := NewEffects(fake, "task-1", 1, "task-1", nil)
	ctx := testContext("root", eff)
	fut, err := ctx.Promise()
	if err != nil {
		t.Fatal(err)
	}
	assertPanicsWithSuspend(t, func() { _ = fut.Await(nil) })
}

func TestContext_RPC_AlreadyResolved_DecodesValue(t *testing.T) {
	fake := newFakeFenceClient()
	fake.preset("root.1", resolvedPromise(t, "root.1", "ok"))
	eff := NewEffects(fake, "task-1", 1, "task-1", nil)
	ctx := testContext("root", eff)
	fut, _ := ctx.RPC("noop", nil)
	var got string
	if err := fut.Await(&got); err != nil {
		t.Fatal(err)
	}
	if got != "ok" {
		t.Fatalf("got %q", got)
	}
}

// ── Detached ────────────────────────────────────────────────────────────

func TestContext_Detached_ReturnsIDAndCreatesPromise(t *testing.T) {
	fake := newFakeFenceClient()
	eff := NewEffects(fake, "task-1", 1, "task-1", nil)
	ctx := testContext("root", eff)
	id, err := ctx.Detached("audit", map[string]int{"v": 1})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(id, "root.") {
		t.Fatalf("expected origin prefix, got %q", id)
	}
	if _, ok := fake.record(id); !ok {
		t.Fatalf("promise %q not created", id)
	}
	if got := ctx.drainSpawnedRemote(); len(got) != 0 {
		t.Fatalf("detached should not register todo, got %v", got)
	}
}

func TestContext_Detached_IDIsHashed16Hex(t *testing.T) {
	fake := newFakeFenceClient()
	eff := NewEffects(fake, "task-1", 1, "task-1", nil)
	ctx := testContext("root", eff)
	id, _ := ctx.Detached("f", nil)
	suffix := strings.TrimPrefix(id, "root.")
	if len(suffix) != 16 {
		t.Fatalf("expected 16-char hash suffix, got %q (len=%d)", suffix, len(suffix))
	}
	for _, r := range suffix {
		if !((r >= '0' && r <= '9') || (r >= 'a' && r <= 'f')) {
			t.Fatalf("non-hex char %q in suffix %q", r, suffix)
		}
	}
}

func TestHashID_StableAndHexLength(t *testing.T) {
	a := hashID("root.0")
	b := hashID("root.0")
	if a != b {
		t.Fatalf("expected stable hash, got %s vs %s", a, b)
	}
	if len(a) != 16 {
		t.Fatalf("expected len 16, got %d", len(a))
	}
}

// ── DurableFunction integration ─────────────────────────────────────────

func TestContext_Run_ZeroArgs(t *testing.T) {
	fake := newFakeFenceClient()
	eff := NewEffects(fake, "task-1", 1, "task-1", nil)
	ctx := testContext("root", eff)

	beat := func(*Context) (string, error) { return "tick", nil }
	fut, err := ctx.Run(beat, nil)
	if err != nil {
		t.Fatal(err)
	}
	var s string
	if err := fut.Await(&s); err != nil {
		t.Fatal(err)
	}
	if s != "tick" {
		t.Fatalf("got %q", s)
	}
}

func TestContext_Run_BadFunction(t *testing.T) {
	ctx := testContext("root", nil)
	_, err := ctx.Run(42, nil)
	if err == nil {
		t.Fatal("expected validation error for non-function")
	}
}

func TestContext_Run_RetrySucceedsAfterTransientErrors(t *testing.T) {
	fake := newFakeFenceClient()
	eff := NewEffects(fake, "task-1", 1, "task-1", nil)
	ctx := testContext("root", eff)

	var calls int32
	flake := func(_ *Context, _ int) (int, error) {
		c := atomic.AddInt32(&calls, 1)
		if c < 3 {
			return 0, errors.New("flake")
		}
		return 99, nil
	}
	fut, err := ctx.Run(flake, 0, RunOpts{
		RetryPolicy: ConstantRetry{MaxAttempts: 5, Delay: time.Millisecond},
	})
	if err != nil {
		t.Fatal(err)
	}
	var got int
	if err := fut.Await(&got); err != nil {
		t.Fatal(err)
	}
	if got != 99 {
		t.Errorf("got %d, want 99", got)
	}
	if c := atomic.LoadInt32(&calls); c != 3 {
		t.Errorf("invocations = %d, want 3", c)
	}
}

func TestContext_Run_NonRetryableShortCircuits(t *testing.T) {
	fake := newFakeFenceClient()
	eff := NewEffects(fake, "task-1", 1, "task-1", nil)
	ctx := testContext("root", eff)

	var calls int32
	sentinel := errors.New("bad-input")
	bad := func(_ *Context, _ int) (int, error) {
		atomic.AddInt32(&calls, 1)
		return 0, NewNonRetryable(sentinel)
	}
	fut, err := ctx.Run(bad, 0, RunOpts{
		RetryPolicy: ConstantRetry{MaxAttempts: 10, Delay: time.Millisecond},
	})
	if err != nil {
		t.Fatal(err)
	}
	var got int
	err = fut.Await(&got)
	if err == nil {
		t.Fatal("expected error")
	}
	var appErr *ApplicationError
	if !errors.As(err, &appErr) || !strings.Contains(appErr.Message, "bad-input") {
		t.Errorf("unexpected error: %v", err)
	}
	if c := atomic.LoadInt32(&calls); c != 1 {
		t.Errorf("invocations = %d, want 1 (NonRetryable)", c)
	}
}

// retryPolicyAlwaysWithDelay is a custom RetryPolicy used to confirm ctx
// cancellation aborts a mid-sleep retry promptly.
type retryPolicyAlwaysWithDelay struct{ delay time.Duration }

func (p retryPolicyAlwaysWithDelay) NextDelay(_ int, _ error) (time.Duration, bool) {
	return p.delay, true
}

func TestContext_Run_RetryAbortsOnCtxCancel(t *testing.T) {
	fake := newFakeFenceClient()
	eff := NewEffects(fake, "task-1", 1, "task-1", nil)
	c := testContext("root", eff)
	cctx, cancel := context.WithCancel(context.Background())
	c.host = cctx

	var calls int32
	alwaysFail := func(_ *Context, _ int) (int, error) {
		atomic.AddInt32(&calls, 1)
		return 0, errors.New("nope")
	}
	// Long sleep between retries; cancelling the host context mid-sleep should
	// short-circuit the loop without waiting it out.
	fut, err := c.Run(alwaysFail, 0, RunOpts{
		RetryPolicy: retryPolicyAlwaysWithDelay{delay: 30 * time.Second},
	})
	if err != nil {
		t.Fatal(err)
	}
	// Give the first attempt a moment to fail and enter the sleep.
	time.Sleep(20 * time.Millisecond)
	cancel()
	start := time.Now()
	var got int
	if err := fut.Await(&got); err == nil {
		t.Fatal("expected error after cancellation")
	}
	if waited := time.Since(start); waited > 5*time.Second {
		t.Errorf("Await took %v after cancel; expected prompt return", waited)
	}
	if c := atomic.LoadInt32(&calls); c != 1 {
		t.Errorf("invocations after cancel = %d, want 1 (no retry should have completed)", c)
	}
}

func TestContext_NewRootContextDefaults(t *testing.T) {
	ctx := NewRootContext(context.TODO(), "root", 100, "myFunc", nil, nil, nil)
	if ctx.FuncName() != "myFunc" {
		t.Fatalf("funcName: %q", ctx.FuncName())
	}
	// Resolver default is identity.
	if got := ctx.targetResolver(nil); got != "" {
		t.Fatalf("expected empty default, got %q", got)
	}
}
