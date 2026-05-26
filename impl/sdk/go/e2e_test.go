package resonate_test

import (
	stdctx "context"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	resonate "github.com/resonatehq/resonate-sdk-go"
	"github.com/resonatehq/resonate-sdk-go/httpnet"
)

// ──────────────────────────────────────────────────────────────────────────
// E2E harness
//
// These tests target a live Resonate server at RESONATE_URL. Without that
// env var every test calls t.Skip and exits cleanly so `go test ./...`
// works out of the box. No mocks: every test exercises the real HTTP+SSE
// transport against the server.
// ──────────────────────────────────────────────────────────────────────────

const e2eTimeout = 30 * time.Second

var e2eIDCounter atomic.Uint64

func resonateURL(t *testing.T) string {
	t.Helper()
	url := os.Getenv("RESONATE_URL")
	if url == "" {
		t.Skip("RESONATE_URL is not set; skipping e2e test")
	}
	return url
}

// uniqueID returns a per-test-run id unlikely to collide on a shared server.
func uniqueID(name string) string {
	return fmt.Sprintf("e2e-%s-%d-%d", name, time.Now().UnixNano(), e2eIDCounter.Add(1))
}

// makeE2EResonate builds a Resonate instance pointed at the test server.
// Each instance uses a unique PID and group so concurrent test runs don't
// fight over message routing.
func makeE2EResonate(t *testing.T, url string) *resonate.Resonate {
	t.Helper()
	r, err := resonate.New(resonate.Config{
		Network: httpnet.NewHTTP(url, httpnet.HTTPOptions{
			PID:   uniqueID("worker"),
			Group: uniqueID("group"),
		}),
	})
	if err != nil {
		t.Fatalf("resonate.New: %v", err)
	}
	return r
}

func e2eCtx(t *testing.T) (stdctx.Context, stdctx.CancelFunc) {
	t.Helper()
	return stdctx.WithTimeout(stdctx.Background(), e2eTimeout)
}

// ──────────────────────────────────────────────────────────────────────────
// Test functions (registered via resonate.Register)
//
// Note: `add`, `noop`, `failAlways`, and `addPair` are defined in
// resonate_test.go (same package). These are e2e-only additions.
// ──────────────────────────────────────────────────────────────────────────

func e2eGreet(_ *resonate.Context, name string) (string, error) {
	return fmt.Sprintf("hello, %s!", name), nil
}

func e2eHangs(_ *resonate.Context, _ struct{}) (any, error) {
	<-make(chan struct{})
	return nil, nil
}

func e2eSequentialWorkflow(c *resonate.Context, _ struct{}) (int64, error) {
	h1, err := c.RPC("add", addPair{X: 1, Y: 2})
	if err != nil {
		return 0, err
	}
	var a int64
	if err := h1.Await(&a); err != nil {
		return 0, err
	}
	h2, err := c.RPC("add", addPair{X: a, Y: 3})
	if err != nil {
		return 0, err
	}
	var b int64
	if err := h2.Await(&b); err != nil {
		return 0, err
	}
	return b, nil
}

func e2eParallelWorkflow(c *resonate.Context, _ struct{}) (int64, error) {
	h1, err := c.RPC("add", addPair{X: 10, Y: 20})
	if err != nil {
		return 0, err
	}
	h2, err := c.RPC("add", addPair{X: 30, Y: 40})
	if err != nil {
		return 0, err
	}
	var a, b int64
	if err := h1.Await(&a); err != nil {
		return 0, err
	}
	if err := h2.Await(&b); err != nil {
		return 0, err
	}
	return a + b, nil
}

func e2eRunSubWorkflow(c *resonate.Context, _ struct{}) (int64, error) {
	h1, err := c.Run(add, addPair{X: 5, Y: 5})
	if err != nil {
		return 0, err
	}
	var a int64
	if err := h1.Await(&a); err != nil {
		return 0, err
	}
	h2, err := c.Run(add, addPair{X: a, Y: 10})
	if err != nil {
		return 0, err
	}
	var b int64
	if err := h2.Await(&b); err != nil {
		return 0, err
	}
	return b, nil
}

// ──────────────────────────────────────────────────────────────────────────
// Tests — simple functions
// ──────────────────────────────────────────────────────────────────────────

func TestE2ESimpleAdd(t *testing.T) {
	url := resonateURL(t)
	r := makeE2EResonate(t, url)
	t.Cleanup(func() { _ = r.Stop() })
	addFn, err := resonate.Register(r, "add", add)
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := e2eCtx(t)
	defer cancel()

	id := uniqueID("simple-add")
	h, err := addFn.Run(ctx, id, addPair{X: 3, Y: 4})
	if err != nil {
		t.Fatal(err)
	}
	sum, err := h.Result(ctx)
	if err != nil {
		t.Fatalf("Result: %v", err)
	}
	if sum != 7 {
		t.Errorf("sum = %d, want 7", sum)
	}
}

func TestE2ESimpleGreet(t *testing.T) {
	url := resonateURL(t)
	r := makeE2EResonate(t, url)
	t.Cleanup(func() { _ = r.Stop() })
	greetFn, err := resonate.Register(r, "greet", e2eGreet)
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := e2eCtx(t)
	defer cancel()

	id := uniqueID("simple-greet")
	h, err := greetFn.Run(ctx, id, "world")
	if err != nil {
		t.Fatal(err)
	}
	out, err := h.Result(ctx)
	if err != nil {
		t.Fatalf("Result: %v", err)
	}
	if out != "hello, world!" {
		t.Errorf("greet = %q, want %q", out, "hello, world!")
	}
}

func TestE2ESimpleNoop(t *testing.T) {
	url := resonateURL(t)
	r := makeE2EResonate(t, url)
	t.Cleanup(func() { _ = r.Stop() })
	noopFn, err := resonate.Register(r, "noop", noop)
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := e2eCtx(t)
	defer cancel()

	id := uniqueID("simple-noop")
	h, err := noopFn.Run(ctx, id, struct{}{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := h.Result(ctx); err != nil {
		t.Fatalf("Result: %v", err)
	}
}

// ──────────────────────────────────────────────────────────────────────────
// Tests — RPC & idempotency
// ──────────────────────────────────────────────────────────────────────────

func TestE2ERPCToRegisteredFunction(t *testing.T) {
	url := resonateURL(t)
	r := makeE2EResonate(t, url)
	t.Cleanup(func() { _ = r.Stop() })
	if _, err := resonate.Register(r, "add", add); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := e2eCtx(t)
	defer cancel()

	id := uniqueID("rpc-add")
	h, err := r.RPC(ctx, id, "add", addPair{X: 10, Y: 20})
	if err != nil {
		t.Fatal(err)
	}
	sum, err := resonate.ResultOf[int64](ctx, h)
	if err != nil {
		t.Fatalf("Result: %v", err)
	}
	if sum != 30 {
		t.Errorf("sum = %d, want 30", sum)
	}
}

func TestE2EIdempotentRun(t *testing.T) {
	url := resonateURL(t)
	r := makeE2EResonate(t, url)
	t.Cleanup(func() { _ = r.Stop() })
	addFn, err := resonate.Register(r, "add", add)
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := e2eCtx(t)
	defer cancel()
	id := uniqueID("idempotent-run")

	h1, err := addFn.Run(ctx, id, addPair{X: 5, Y: 5})
	if err != nil {
		t.Fatal(err)
	}
	v1, err := h1.Result(ctx)
	if err != nil {
		t.Fatal(err)
	}
	h2, err := addFn.Run(ctx, id, addPair{X: 5, Y: 5})
	if err != nil {
		t.Fatal(err)
	}
	v2, err := h2.Result(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if v1 != 10 || v2 != 10 {
		t.Errorf("(v1,v2) = (%d,%d), want (10,10)", v1, v2)
	}
}

func TestE2EIdempotentRPC(t *testing.T) {
	url := resonateURL(t)
	r := makeE2EResonate(t, url)
	t.Cleanup(func() { _ = r.Stop() })
	if _, err := resonate.Register(r, "add", add); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := e2eCtx(t)
	defer cancel()
	id := uniqueID("idempotent-rpc")

	h1, err := r.RPC(ctx, id, "add", addPair{X: 7, Y: 8})
	if err != nil {
		t.Fatal(err)
	}
	v1, err := resonate.ResultOf[int64](ctx, h1)
	if err != nil {
		t.Fatal(err)
	}
	h2, err := r.RPC(ctx, id, "add", addPair{X: 7, Y: 8})
	if err != nil {
		t.Fatal(err)
	}
	v2, err := resonate.ResultOf[int64](ctx, h2)
	if err != nil {
		t.Fatal(err)
	}
	if v1 != 15 || v2 != 15 {
		t.Errorf("(v1,v2) = (%d,%d), want (15,15)", v1, v2)
	}
}

// TestE2EIdempotentRunOrphanedTask: worker A claims a task that hangs
// forever; worker B issues Run with the same id. The server returns 409 and
// the SDK should subscribe and hand back a usable handle rather than fail.
func TestE2EIdempotentRunOrphanedTask(t *testing.T) {
	url := resonateURL(t)
	id := uniqueID("orphaned")

	rA := makeE2EResonate(t, url)
	t.Cleanup(func() { _ = rA.Stop() })
	hangsA, err := resonate.Register(rA, "hangs", e2eHangs)
	if err != nil {
		t.Fatal(err)
	}
	ctxA, cancelA := e2eCtx(t)
	defer cancelA()
	if _, err := hangsA.Run(ctxA, id, struct{}{}); err != nil {
		t.Fatal(err)
	}

	time.Sleep(500 * time.Millisecond)

	rB := makeE2EResonate(t, url)
	t.Cleanup(func() { _ = rB.Stop() })
	hangsB, err := resonate.Register(rB, "hangs", e2eHangs)
	if err != nil {
		t.Fatal(err)
	}
	ctxB, cancelB := stdctx.WithTimeout(stdctx.Background(), 5*time.Second)
	defer cancelB()
	if _, err := hangsB.Run(ctxB, id, struct{}{}); err != nil {
		t.Fatalf("expected handle on 409, got %v", err)
	}
}

// TestE2EIdempotentRPCOrphanedTask: same scenario as above but worker B
// calls RPC. promise.create is idempotent so subscribe should succeed.
func TestE2EIdempotentRPCOrphanedTask(t *testing.T) {
	url := resonateURL(t)
	id := uniqueID("orphaned-rpc")

	rA := makeE2EResonate(t, url)
	t.Cleanup(func() { _ = rA.Stop() })
	hangsA, err := resonate.Register(rA, "hangs", e2eHangs)
	if err != nil {
		t.Fatal(err)
	}
	ctxA, cancelA := e2eCtx(t)
	defer cancelA()
	if _, err := hangsA.Run(ctxA, id, struct{}{}); err != nil {
		t.Fatal(err)
	}

	time.Sleep(500 * time.Millisecond)

	rB := makeE2EResonate(t, url)
	t.Cleanup(func() { _ = rB.Stop() })
	ctxB, cancelB := stdctx.WithTimeout(stdctx.Background(), 5*time.Second)
	defer cancelB()
	if _, err := rB.RPC(ctxB, id, "hangs", nil); err != nil {
		t.Fatalf("expected handle, got %v", err)
	}
}

func TestE2ERunAfterResolvedDifferentWorker(t *testing.T) {
	url := resonateURL(t)
	id := uniqueID("run-after-resolved")

	rA := makeE2EResonate(t, url)
	t.Cleanup(func() { _ = rA.Stop() })
	addA, err := resonate.Register(rA, "add", add)
	if err != nil {
		t.Fatal(err)
	}
	ctxA, cancelA := e2eCtx(t)
	defer cancelA()
	hA, err := addA.Run(ctxA, id, addPair{X: 3, Y: 4})
	if err != nil {
		t.Fatal(err)
	}
	vA, err := hA.Result(ctxA)
	if err != nil {
		t.Fatal(err)
	}
	if vA != 7 {
		t.Errorf("rA result = %d, want 7", vA)
	}

	rB := makeE2EResonate(t, url)
	t.Cleanup(func() { _ = rB.Stop() })
	addB, err := resonate.Register(rB, "add", add)
	if err != nil {
		t.Fatal(err)
	}
	ctxB, cancelB := e2eCtx(t)
	defer cancelB()
	hB, err := addB.Run(ctxB, id, addPair{X: 3, Y: 4})
	if err != nil {
		t.Fatal(err)
	}
	vB, err := hB.Result(ctxB)
	if err != nil {
		t.Fatal(err)
	}
	if vB != 7 {
		t.Errorf("rB result = %d, want 7 (cached)", vB)
	}
}

func TestE2ERunAfterRejectedDifferentWorker(t *testing.T) {
	url := resonateURL(t)
	id := uniqueID("run-after-rejected")

	rA := makeE2EResonate(t, url)
	t.Cleanup(func() { _ = rA.Stop() })
	failA, err := resonate.Register(rA, "fail_always", failAlways)
	if err != nil {
		t.Fatal(err)
	}
	ctxA, cancelA := e2eCtx(t)
	defer cancelA()
	hA, err := failA.Run(ctxA, id, "boom")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := hA.Result(ctxA); err == nil {
		t.Fatal("expected error from rA, got nil")
	}

	rB := makeE2EResonate(t, url)
	t.Cleanup(func() { _ = rB.Stop() })
	failB, err := resonate.Register(rB, "fail_always", failAlways)
	if err != nil {
		t.Fatal(err)
	}
	ctxB, cancelB := e2eCtx(t)
	defer cancelB()
	hB, err := failB.Run(ctxB, id, "boom")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := hB.Result(ctxB); err == nil {
		t.Fatal("expected cached error from rB, got nil")
	}
}

func TestE2ERunThenRPCSameID(t *testing.T) {
	url := resonateURL(t)
	r := makeE2EResonate(t, url)
	t.Cleanup(func() { _ = r.Stop() })
	addFn, err := resonate.Register(r, "add", add)
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := e2eCtx(t)
	defer cancel()
	id := uniqueID("run-then-rpc")

	h1, err := addFn.Run(ctx, id, addPair{X: 1, Y: 2})
	if err != nil {
		t.Fatal(err)
	}
	v1, err := h1.Result(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if v1 != 3 {
		t.Errorf("v1 = %d, want 3", v1)
	}

	h2, err := r.RPC(ctx, id, "add", addPair{X: 1, Y: 2})
	if err != nil {
		t.Fatal(err)
	}
	v2, err := resonate.ResultOf[int64](ctx, h2)
	if err != nil {
		t.Fatal(err)
	}
	if v2 != 3 {
		t.Errorf("v2 = %d, want 3 (cached)", v2)
	}
}

func TestE2ERPCThenRunSameID(t *testing.T) {
	url := resonateURL(t)
	r := makeE2EResonate(t, url)
	t.Cleanup(func() { _ = r.Stop() })
	addFn, err := resonate.Register(r, "add", add)
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := e2eCtx(t)
	defer cancel()
	id := uniqueID("rpc-then-run")

	h1, err := r.RPC(ctx, id, "add", addPair{X: 4, Y: 5})
	if err != nil {
		t.Fatal(err)
	}
	v1, err := resonate.ResultOf[int64](ctx, h1)
	if err != nil {
		t.Fatal(err)
	}
	if v1 != 9 {
		t.Errorf("v1 = %d, want 9", v1)
	}

	h2, err := addFn.Run(ctx, id, addPair{X: 4, Y: 5})
	if err != nil {
		t.Fatal(err)
	}
	v2, err := h2.Result(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if v2 != 9 {
		t.Errorf("v2 = %d, want 9 (cached)", v2)
	}
}

func TestE2EConcurrentRunSameID(t *testing.T) {
	url := resonateURL(t)
	id := uniqueID("concurrent-run")

	rA := makeE2EResonate(t, url)
	t.Cleanup(func() { _ = rA.Stop() })
	addA, err := resonate.Register(rA, "add", add)
	if err != nil {
		t.Fatal(err)
	}
	rB := makeE2EResonate(t, url)
	t.Cleanup(func() { _ = rB.Stop() })
	addB, err := resonate.Register(rB, "add", add)
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := e2eCtx(t)
	defer cancel()

	var wg sync.WaitGroup
	results := make([]int64, 2)
	errs := make([]error, 2)
	wg.Add(2)
	for i, fn := range []*resonate.RegisteredFunc[addPair, int64]{addA, addB} {
		i, fn := i, fn
		go func() {
			defer wg.Done()
			h, err := fn.Run(ctx, id, addPair{X: 11, Y: 22})
			if err != nil {
				errs[i] = err
				return
			}
			v, err := h.Result(ctx)
			results[i] = v
			errs[i] = err
		}()
	}
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Errorf("worker %d: %v", i, err)
		}
		if results[i] != 33 {
			t.Errorf("worker %d result = %d, want 33", i, results[i])
		}
	}
}

// TestE2ERunMemoizedByID verifies that Run is memoized by id (not by args).
// A second Run with a different args payload but the same id should return
// the first invocation's result, NOT re-execute.
func TestE2ERunMemoizedByID(t *testing.T) {
	url := resonateURL(t)
	r := makeE2EResonate(t, url)
	t.Cleanup(func() { _ = r.Stop() })
	addFn, err := resonate.Register(r, "add", add)
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := e2eCtx(t)
	defer cancel()
	id := uniqueID("memoized-args")

	h1, err := addFn.Run(ctx, id, addPair{X: 1, Y: 1})
	if err != nil {
		t.Fatal(err)
	}
	v1, err := h1.Result(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if v1 != 2 {
		t.Errorf("v1 = %d, want 2", v1)
	}

	h2, err := addFn.Run(ctx, id, addPair{X: 5, Y: 5})
	if err != nil {
		t.Fatal(err)
	}
	v2, err := h2.Result(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if v2 != 2 {
		t.Errorf("v2 = %d, want 2 (memoized, not re-executed)", v2)
	}
}

// ──────────────────────────────────────────────────────────────────────────
// Tests — Workflows
// ──────────────────────────────────────────────────────────────────────────

func TestE2EWorkflowSequentialRPCs(t *testing.T) {
	url := resonateURL(t)
	r := makeE2EResonate(t, url)
	t.Cleanup(func() { _ = r.Stop() })
	if _, err := resonate.Register(r, "add", add); err != nil {
		t.Fatal(err)
	}
	seqFn, err := resonate.Register(r, "sequential_workflow", e2eSequentialWorkflow)
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := e2eCtx(t)
	defer cancel()
	id := uniqueID("seq-workflow")

	h, err := seqFn.Run(ctx, id, struct{}{})
	if err != nil {
		t.Fatal(err)
	}
	got, err := h.Result(ctx)
	if err != nil {
		t.Fatalf("Result: %v", err)
	}
	// 1+2=3, 3+3=6
	if got != 6 {
		t.Errorf("result = %d, want 6", got)
	}
}

func TestE2EWorkflowParallelRPCs(t *testing.T) {
	url := resonateURL(t)
	r := makeE2EResonate(t, url)
	t.Cleanup(func() { _ = r.Stop() })
	if _, err := resonate.Register(r, "add", add); err != nil {
		t.Fatal(err)
	}
	parFn, err := resonate.Register(r, "parallel_workflow", e2eParallelWorkflow)
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := e2eCtx(t)
	defer cancel()
	id := uniqueID("par-workflow")

	h, err := parFn.Run(ctx, id, struct{}{})
	if err != nil {
		t.Fatal(err)
	}
	got, err := h.Result(ctx)
	if err != nil {
		t.Fatalf("Result: %v", err)
	}
	// (10+20) + (30+40) = 100
	if got != 100 {
		t.Errorf("result = %d, want 100", got)
	}
}

func TestE2EWorkflowWithCtxRun(t *testing.T) {
	url := resonateURL(t)
	r := makeE2EResonate(t, url)
	t.Cleanup(func() { _ = r.Stop() })
	if _, err := resonate.Register(r, "add", add); err != nil {
		t.Fatal(err)
	}
	subFn, err := resonate.Register(r, "run_sub_workflow", e2eRunSubWorkflow)
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := e2eCtx(t)
	defer cancel()
	id := uniqueID("run-sub-workflow")

	h, err := subFn.Run(ctx, id, struct{}{})
	if err != nil {
		t.Fatal(err)
	}
	got, err := h.Result(ctx)
	if err != nil {
		t.Fatalf("Result: %v", err)
	}
	// 5+5=10, 10+10=20
	if got != 20 {
		t.Errorf("result = %d, want 20", got)
	}
}

// ──────────────────────────────────────────────────────────────────────────
// Tests — Error propagation, handles
// ──────────────────────────────────────────────────────────────────────────

func TestE2EErrorPropagation(t *testing.T) {
	url := resonateURL(t)
	r := makeE2EResonate(t, url)
	t.Cleanup(func() { _ = r.Stop() })
	failFn, err := resonate.Register(r, "fail_always", failAlways)
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := e2eCtx(t)
	defer cancel()
	id := uniqueID("error-prop")

	h, err := failFn.Run(ctx, id, "boom")
	if err != nil {
		t.Fatal(err)
	}
	_, err = h.Result(ctx)
	if err == nil {
		t.Fatal("expected propagated error, got nil")
	}
	if !strings.Contains(err.Error(), "boom") {
		t.Errorf("error %q should mention boom", err.Error())
	}
}

func TestE2EHandleResult(t *testing.T) {
	url := resonateURL(t)
	r := makeE2EResonate(t, url)
	t.Cleanup(func() { _ = r.Stop() })
	addFn, err := resonate.Register(r, "add", add)
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := e2eCtx(t)
	defer cancel()
	id := uniqueID("handle-spawn")

	h, err := addFn.Run(ctx, id, addPair{X: 100, Y: 200})
	if err != nil {
		t.Fatal(err)
	}
	v, err := h.Result(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if v != 300 {
		t.Errorf("v = %d, want 300", v)
	}
}

// ──────────────────────────────────────────────────────────────────────────
// Tests — Retry policy
// ──────────────────────────────────────────────────────────────────────────

func TestE2E_Run_RetrySucceedsAfterTransientErrors(t *testing.T) {
	url := resonateURL(t)
	r := makeE2EResonate(t, url)
	t.Cleanup(func() { _ = r.Stop() })

	var calls atomic.Int32
	flake := func(_ *resonate.Context, _ struct{}) (int, error) {
		n := calls.Add(1)
		if n < 3 {
			return 0, fmt.Errorf("transient %d", n)
		}
		return 42, nil
	}
	fn, err := resonate.Register(r, uniqueID("e2e_retry_ok"), flake)
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := e2eCtx(t)
	defer cancel()
	h, err := fn.Run(ctx, uniqueID("retry-ok"), struct{}{}, resonate.RunOptions{
		RetryPolicy: resonate.ExponentialRetry{MaxAttempts: 5, Base: 50 * time.Millisecond, Max: 1 * time.Second},
	})
	if err != nil {
		t.Fatal(err)
	}
	got, err := h.Result(ctx)
	if err != nil {
		t.Fatalf("Result: %v", err)
	}
	if got != 42 {
		t.Errorf("got %d, want 42", got)
	}
	if n := calls.Load(); n != 3 {
		t.Errorf("invocations = %d, want 3", n)
	}
}

func TestE2E_Run_RetryExhausted(t *testing.T) {
	url := resonateURL(t)
	r := makeE2EResonate(t, url)
	t.Cleanup(func() { _ = r.Stop() })

	var calls atomic.Int32
	alwaysFail := func(_ *resonate.Context, _ struct{}) (int, error) {
		calls.Add(1)
		return 0, errors.New("nope")
	}
	fn, err := resonate.Register(r, uniqueID("e2e_retry_exhaust"), alwaysFail)
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := e2eCtx(t)
	defer cancel()
	id := uniqueID("retry-exhausted")
	h, err := fn.Run(ctx, id, struct{}{}, resonate.RunOptions{
		RetryPolicy: resonate.ConstantRetry{MaxAttempts: 3, Delay: 50 * time.Millisecond},
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = h.Result(ctx)
	if err == nil {
		t.Fatal("expected error after exhausted retries")
	}
	if !strings.Contains(err.Error(), "nope") {
		t.Errorf("error %q should mention nope", err.Error())
	}
	if n := calls.Load(); n != 3 {
		t.Errorf("invocations = %d, want 3", n)
	}
}

func TestE2E_Run_NoRetry(t *testing.T) {
	url := resonateURL(t)
	r := makeE2EResonate(t, url)
	t.Cleanup(func() { _ = r.Stop() })

	var calls atomic.Int32
	failOnce := func(_ *resonate.Context, _ struct{}) (int, error) {
		calls.Add(1)
		return 0, errors.New("once")
	}
	fn, err := resonate.Register(r, uniqueID("e2e_no_retry"), failOnce)
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := e2eCtx(t)
	defer cancel()
	h, err := fn.Run(ctx, uniqueID("no-retry"), struct{}{}, resonate.RunOptions{
		RetryPolicy: resonate.NoRetry,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := h.Result(ctx); err == nil {
		t.Fatal("expected error")
	}
	if n := calls.Load(); n != 1 {
		t.Errorf("invocations = %d, want 1", n)
	}
}

func TestE2E_Run_NonRetryable(t *testing.T) {
	url := resonateURL(t)
	r := makeE2EResonate(t, url)
	t.Cleanup(func() { _ = r.Stop() })

	var calls atomic.Int32
	bad := func(_ *resonate.Context, _ struct{}) (int, error) {
		calls.Add(1)
		return 0, resonate.NewNonRetryable(errors.New("validation-failed"))
	}
	fn, err := resonate.Register(r, uniqueID("e2e_non_retryable"), bad)
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := e2eCtx(t)
	defer cancel()
	h, err := fn.Run(ctx, uniqueID("non-retryable"), struct{}{}, resonate.RunOptions{
		RetryPolicy: resonate.ConstantRetry{MaxAttempts: 10, Delay: 50 * time.Millisecond},
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = h.Result(ctx)
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "validation-failed") {
		t.Errorf("error %q should mention validation-failed", err.Error())
	}
	if n := calls.Load(); n != 1 {
		t.Errorf("invocations = %d, want 1 (NonRetryable)", n)
	}
}

func TestE2E_Run_RetrySpanRespectsTimeout(t *testing.T) {
	url := resonateURL(t)
	r := makeE2EResonate(t, url)
	t.Cleanup(func() { _ = r.Stop() })

	var calls atomic.Int32
	alwaysFail := func(_ *resonate.Context, _ struct{}) (int, error) {
		calls.Add(1)
		return 0, errors.New("doomed")
	}
	fn, err := resonate.Register(r, uniqueID("e2e_retry_timeout"), alwaysFail)
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := e2eCtx(t)
	defer cancel()
	id := uniqueID("retry-timeout")
	// Promise lives 300ms; constant delay is 500ms — at most 1 invocation can
	// fit before the loop refuses to sleep past TimeoutAt.
	h, err := fn.Run(ctx, id, struct{}{}, resonate.RunOptions{
		Timeout:     300 * time.Millisecond,
		RetryPolicy: resonate.ConstantRetry{MaxAttempts: 100, Delay: 500 * time.Millisecond},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := h.Result(ctx); err == nil {
		t.Fatal("expected error after timeout-bounded retry")
	}
	if n := calls.Load(); n != 1 {
		t.Errorf("invocations = %d, want 1 (TimeoutAt should have cut retry short)", n)
	}
}

func TestE2E_CtxRun_RetryWithinWorkflow(t *testing.T) {
	url := resonateURL(t)
	r := makeE2EResonate(t, url)
	t.Cleanup(func() { _ = r.Stop() })

	var inner atomic.Int32
	innerFn := func(_ *resonate.Context, _ struct{}) (int, error) {
		n := inner.Add(1)
		if n < 3 {
			return 0, errors.New("flake")
		}
		return 7, nil
	}
	outerFn := func(c *resonate.Context, _ struct{}) (int, error) {
		f, err := c.Run(innerFn, struct{}{}, resonate.RunOpts{
			RetryPolicy: resonate.ConstantRetry{MaxAttempts: 5, Delay: 10 * time.Millisecond},
		})
		if err != nil {
			return 0, err
		}
		var got int
		if err := f.Await(&got); err != nil {
			return 0, err
		}
		return got, nil
	}
	fn, err := resonate.Register(r, uniqueID("e2e_ctxrun_retry_outer"), outerFn)
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := e2eCtx(t)
	defer cancel()
	h, err := fn.Run(ctx, uniqueID("ctxrun-retry"), struct{}{})
	if err != nil {
		t.Fatal(err)
	}
	got, err := h.Result(ctx)
	if err != nil {
		t.Fatalf("Result: %v", err)
	}
	if got != 7 {
		t.Errorf("got %d, want 7", got)
	}
	if n := inner.Load(); n != 3 {
		t.Errorf("inner invocations = %d, want 3", n)
	}
}

func TestE2E_Run_RetrySurvivesHeartbeat(t *testing.T) {
	url := resonateURL(t)
	// Force a short TTL/heartbeat so this test exercises the lease being kept
	// alive across multiple retry sleeps.
	r, err := resonate.New(resonate.Config{
		Network: httpnet.NewHTTP(url, httpnet.HTTPOptions{
			PID:   uniqueID("worker-hb"),
			Group: uniqueID("group-hb"),
		}),
		TTL: 2 * time.Second,
	})
	if err != nil {
		t.Fatalf("resonate.New: %v", err)
	}
	t.Cleanup(func() { _ = r.Stop() })

	var calls atomic.Int32
	flake := func(_ *resonate.Context, _ struct{}) (int, error) {
		n := calls.Add(1)
		if n < 4 {
			return 0, fmt.Errorf("transient %d", n)
		}
		return 1, nil
	}
	fn, err := resonate.Register(r, uniqueID("e2e_retry_hb"), flake)
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := e2eCtx(t)
	defer cancel()
	// Cumulative sleep across 3 failed attempts: ~3*1.5s = 4.5s — longer than
	// the 2s TTL, so heartbeat must keep the lease alive across the loop.
	h, err := fn.Run(ctx, uniqueID("retry-hb"), struct{}{}, resonate.RunOptions{
		RetryPolicy: resonate.ConstantRetry{MaxAttempts: 6, Delay: 1500 * time.Millisecond},
	})
	if err != nil {
		t.Fatal(err)
	}
	got, err := h.Result(ctx)
	if err != nil {
		t.Fatalf("Result: %v", err)
	}
	if got != 1 {
		t.Errorf("got %d, want 1", got)
	}
	if n := calls.Load(); n != 4 {
		t.Errorf("invocations = %d, want 4", n)
	}
}
