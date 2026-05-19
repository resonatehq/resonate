package resonate_test

import (
	stdctx "context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	resonate "github.com/resonatehq/resonate-sdk-go"
	"github.com/resonatehq/resonate-sdk-go/localnet"
)

// ──────────────────────────────────────────────────────────────────────────
// Fixtures
// ──────────────────────────────────────────────────────────────────────────

// localConfig is the subset of resonate.Config most tests vary. Anything not
// set falls back to defaults appropriate for local-mode tests.
type localConfig struct {
	PID    string
	Group  string
	Prefix string
	TTL    time.Duration
}

// newLocal builds a Resonate instance backed by an in-process LocalNetwork.
// No mocks: every test exercises the real Sender/Transport/Network and the
// localnet server state machine end-to-end.
func newLocal(t *testing.T, lc localConfig) *resonate.Resonate {
	t.Helper()
	pid := lc.PID
	if pid == "" {
		pid = "default"
	}
	group := lc.Group
	if group == "" {
		group = "default"
	}
	ttl := lc.TTL
	if ttl == 0 {
		ttl = time.Hour
	}
	pidPtr := pid
	r, err := resonate.New(resonate.Config{
		Network:   localnet.NewLocal(group, &pidPtr),
		Heartbeat: resonate.NoopHeartbeat{},
		TTL:       ttl,
		Prefix:    lc.Prefix,
	})
	if err != nil {
		t.Fatalf("resonate.New: %v", err)
	}
	t.Cleanup(func() { _ = r.Stop() })
	return r
}

func testCtx(t *testing.T) (stdctx.Context, stdctx.CancelFunc) {
	t.Helper()
	return stdctx.WithTimeout(stdctx.Background(), 5*time.Second)
}

// ──────────────────────────────────────────────────────────────────────────
// Test functions (registered into Resonate by tests below)
// ──────────────────────────────────────────────────────────────────────────

type addPair struct {
	X int64 `json:"x"`
	Y int64 `json:"y"`
}

func add(_ *resonate.Context, a addPair) (int64, error) { return a.X + a.Y, nil }

func noop(_ *resonate.Context, _ struct{}) (any, error) { return nil, nil }

func failAlways(_ *resonate.Context, msg string) (string, error) { return "", errors.New(msg) }

// ──────────────────────────────────────────────────────────────────────────
// Constructor / configuration
// ──────────────────────────────────────────────────────────────────────────

func TestNewRequiresNetwork(t *testing.T) {
	t.Setenv("RESONATE_URL", "")
	_, err := resonate.New(resonate.Config{})
	if !errors.Is(err, resonate.ErrNetworkRequired) {
		t.Fatalf("expected ErrNetworkRequired, got %v", err)
	}
}

func TestDefaultTTLIs60s(t *testing.T) {
	pid := "default"
	r, err := resonate.New(resonate.Config{
		Network:   localnet.NewLocal("default", &pid),
		Heartbeat: resonate.NoopHeartbeat{},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = r.Stop() })
	if got := r.TTL(); got != 60*time.Second {
		t.Errorf("default TTL = %v, want 60s", got)
	}
}

func TestCustomTTL(t *testing.T) {
	r := newLocal(t, localConfig{TTL: 120 * time.Second})
	if got := r.TTL(); got != 120*time.Second {
		t.Errorf("TTL = %v, want 120s", got)
	}
}

func TestEmptyPrefix(t *testing.T) {
	r := newLocal(t, localConfig{})
	if r.IDPrefix() != "" {
		t.Errorf("empty Prefix should produce empty IDPrefix, got %q", r.IDPrefix())
	}
}

func TestPrefixGetsColon(t *testing.T) {
	r := newLocal(t, localConfig{Prefix: "myapp"})
	if got, want := r.IDPrefix(), "myapp:"; got != want {
		t.Errorf("IDPrefix = %q, want %q", got, want)
	}
}

func TestCustomPIDAndGroup(t *testing.T) {
	r := newLocal(t, localConfig{PID: "worker-1", Group: "workers"})
	if r.PID() != "worker-1" {
		t.Errorf("PID = %q, want worker-1", r.PID())
	}
	uni := r.Network().Unicast()
	if !strings.Contains(uni, "worker-1") || !strings.Contains(uni, "workers") {
		t.Errorf("unicast %q should contain pid and group", uni)
	}
}

func TestNetworkIdentityLocal(t *testing.T) {
	r := newLocal(t, localConfig{})
	uni := r.Network().Unicast()
	any := r.Network().Anycast()
	if !strings.HasPrefix(uni, "local://uni@") {
		t.Errorf("unicast %q should start with local://uni@", uni)
	}
	if !strings.HasPrefix(any, "local://any@") {
		t.Errorf("anycast %q should start with local://any@", any)
	}
	if r.Network().Group() != "default" {
		t.Errorf("group = %q, want default", r.Network().Group())
	}
	if r.Network().PID() != "default" {
		t.Errorf("pid = %q, want default", r.Network().PID())
	}
}

func TestTargetResolverLocal(t *testing.T) {
	r := newLocal(t, localConfig{})
	if got, want := r.Network().TargetResolver("my-target"), "local://any@my-target"; got != want {
		t.Errorf("resolver = %q, want %q", got, want)
	}
}

// ──────────────────────────────────────────────────────────────────────────
// Register
// ──────────────────────────────────────────────────────────────────────────

func TestRegisterByName(t *testing.T) {
	r := newLocal(t, localConfig{})
	addFn, err := resonate.Register(r, "add", add)
	if err != nil {
		t.Fatalf("register: %v", err)
	}
	ctx, cancel := testCtx(t)
	defer cancel()
	if _, err := addFn.Run(ctx, "test-id", addPair{1, 2}); err != nil {
		t.Fatalf("Run after register failed: %v", err)
	}
}

func TestRegisterDuplicateReturnsError(t *testing.T) {
	r := newLocal(t, localConfig{})
	if _, err := resonate.Register(r, "noop", noop); err != nil {
		t.Fatal(err)
	}
	_, err := resonate.Register(r, "noop", noop)
	var dup *resonate.AlreadyRegisteredError
	if !errors.As(err, &dup) {
		t.Fatalf("expected AlreadyRegisteredError, got %v", err)
	}
}

// ──────────────────────────────────────────────────────────────────────────
// Run
// ──────────────────────────────────────────────────────────────────────────

func TestRunReturnsHandle(t *testing.T) {
	r := newLocal(t, localConfig{})
	noopFn, err := resonate.Register(r, "noop", noop)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := testCtx(t)
	defer cancel()
	h, err := noopFn.Run(ctx, "greet-1", struct{}{})
	if err != nil {
		t.Fatal(err)
	}
	if h.ID() != "greet-1" {
		t.Errorf("handle id = %q, want greet-1", h.ID())
	}
}

func TestRunWithPrefixPrependsToID(t *testing.T) {
	r := newLocal(t, localConfig{Prefix: "app"})
	noopFn, err := resonate.Register(r, "noop", noop)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := testCtx(t)
	defer cancel()
	h, err := noopFn.Run(ctx, "my-id", struct{}{})
	if err != nil {
		t.Fatal(err)
	}
	if got, want := h.ID(), "app:my-id"; got != want {
		t.Errorf("handle id = %q, want %q", got, want)
	}
}

func TestRunIdempotentSameIDReturnsExistingPromise(t *testing.T) {
	r := newLocal(t, localConfig{})
	noopFn, err := resonate.Register(r, "noop", noop)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := testCtx(t)
	defer cancel()
	if _, err := noopFn.Run(ctx, "same-id", struct{}{}); err != nil {
		t.Fatal(err)
	}
	h2, err := noopFn.Run(ctx, "same-id", struct{}{})
	if err != nil {
		t.Fatalf("second Run with same id should succeed, got %v", err)
	}
	if h2.ID() != "same-id" {
		t.Errorf("handle id = %q, want same-id", h2.ID())
	}
}

func TestRunSetsRootTags(t *testing.T) {
	r := newLocal(t, localConfig{})
	noopFn, err := resonate.Register(r, "noop", noop)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := testCtx(t)
	defer cancel()
	if _, err := noopFn.Run(ctx, "tag-test", struct{}{}); err != nil {
		t.Fatal(err)
	}
	rec, err := r.Sender().PromiseGet(ctx, "tag-test")
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []struct{ k, v string }{
		{"resonate:scope", "global"},
		{"resonate:origin", "tag-test"},
		{"resonate:branch", "tag-test"},
		{"resonate:parent", "tag-test"},
	} {
		if got := rec.Tags[want.k]; got != want.v {
			t.Errorf("tag %s = %q, want %q", want.k, got, want.v)
		}
	}
}

func TestRunWithCustomTimeout(t *testing.T) {
	r := newLocal(t, localConfig{})
	noopFn, err := resonate.Register(r, "noop", noop)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := testCtx(t)
	defer cancel()
	before := time.Now().UnixMilli()
	if _, err := noopFn.Run(ctx, "timeout-test", struct{}{}, resonate.RunOptions{Timeout: 300 * time.Second}); err != nil {
		t.Fatal(err)
	}
	rec, err := r.Sender().PromiseGet(ctx, "timeout-test")
	if err != nil {
		t.Fatal(err)
	}
	// 300s = 300_000 ms ahead of "now" — allow generous slack for execution.
	if delta := rec.TimeoutAt - before; delta < 290_000 || delta > 310_000 {
		t.Errorf("timeoutAt-before = %d ms, expected ~300_000", delta)
	}
}

func TestRunExecutesAndResolvesToValue(t *testing.T) {
	r := newLocal(t, localConfig{})
	addFn, err := resonate.Register(r, "add", add)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := testCtx(t)
	defer cancel()
	h, err := addFn.Run(ctx, "add-1", addPair{3, 4})
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

func TestRunWithCustomTarget(t *testing.T) {
	r := newLocal(t, localConfig{})
	noopFn, err := resonate.Register(r, "noop", noop)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := testCtx(t)
	defer cancel()
	if _, err := noopFn.Run(ctx, "run-target", struct{}{}, resonate.RunOptions{Target: "my-target"}); err != nil {
		t.Fatal(err)
	}
	rec, err := r.Sender().PromiseGet(ctx, "run-target")
	if err != nil {
		t.Fatal(err)
	}
	if got, want := rec.Tags["resonate:target"], "local://any@my-target"; got != want {
		t.Errorf("target = %q, want %q", got, want)
	}
}

func TestRunURLTargetPassesThrough(t *testing.T) {
	r := newLocal(t, localConfig{})
	noopFn, err := resonate.Register(r, "noop", noop)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := testCtx(t)
	defer cancel()
	url := "https://remote:9000/workers/noop"
	if _, err := noopFn.Run(ctx, "run-url", struct{}{}, resonate.RunOptions{Target: url}); err != nil {
		t.Fatal(err)
	}
	rec, err := r.Sender().PromiseGet(ctx, "run-url")
	if err != nil {
		t.Fatal(err)
	}
	if got := rec.Tags["resonate:target"]; got != url {
		t.Errorf("target = %q, want %q", got, url)
	}
}

func TestRunDefaultTargetUsesGroupResolver(t *testing.T) {
	r := newLocal(t, localConfig{})
	noopFn, err := resonate.Register(r, "noop", noop)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := testCtx(t)
	defer cancel()
	if _, err := noopFn.Run(ctx, "run-default", struct{}{}); err != nil {
		t.Fatal(err)
	}
	rec, err := r.Sender().PromiseGet(ctx, "run-default")
	if err != nil {
		t.Fatal(err)
	}
	if got, want := rec.Tags["resonate:target"], "local://any@default"; got != want {
		t.Errorf("target = %q, want %q", got, want)
	}
}

// ──────────────────────────────────────────────────────────────────────────
// RPC
// ──────────────────────────────────────────────────────────────────────────

func TestRPCWithoutRegistration(t *testing.T) {
	r := newLocal(t, localConfig{})
	ctx, cancel := testCtx(t)
	defer cancel()
	h, err := r.RPC(ctx, "rpc-1", "remote_fn", []int{1, 2}, resonate.RPCOptions{Target: "unhandled"})
	if err != nil {
		t.Fatal(err)
	}
	if h.ID() != "rpc-1" {
		t.Errorf("id = %q, want rpc-1", h.ID())
	}
}

func TestRPCWithPrefix(t *testing.T) {
	r := newLocal(t, localConfig{Prefix: "svc"})
	ctx, cancel := testCtx(t)
	defer cancel()
	h, err := r.RPC(ctx, "rpc-2", "remote", nil, resonate.RPCOptions{Target: "unhandled"})
	if err != nil {
		t.Fatal(err)
	}
	if got, want := h.ID(), "svc:rpc-2"; got != want {
		t.Errorf("id = %q, want %q", got, want)
	}
}

func TestRPCIdempotent(t *testing.T) {
	r := newLocal(t, localConfig{})
	ctx, cancel := testCtx(t)
	defer cancel()
	opts := resonate.RPCOptions{Target: "unhandled"}
	if _, err := r.RPC(ctx, "rpc-dup", "remote", nil, opts); err != nil {
		t.Fatal(err)
	}
	if _, err := r.RPC(ctx, "rpc-dup", "remote", nil, opts); err != nil {
		t.Fatalf("second RPC same id: %v", err)
	}
}

func TestRPCBareNameTarget(t *testing.T) {
	r := newLocal(t, localConfig{})
	ctx, cancel := testCtx(t)
	defer cancel()
	if _, err := r.RPC(ctx, "target-bare", "remote", nil, resonate.RPCOptions{Target: "my-worker"}); err != nil {
		t.Fatal(err)
	}
	rec, err := r.Sender().PromiseGet(ctx, "target-bare")
	if err != nil {
		t.Fatal(err)
	}
	if got, want := rec.Tags["resonate:target"], "local://any@my-worker"; got != want {
		t.Errorf("target = %q, want %q", got, want)
	}
}

func TestRPCURLTargetPassesThrough(t *testing.T) {
	r := newLocal(t, localConfig{})
	ctx, cancel := testCtx(t)
	defer cancel()
	url := "https://remote:9000/workers/hello"
	if _, err := r.RPC(ctx, "target-url", "remote", nil, resonate.RPCOptions{Target: url}); err != nil {
		t.Fatal(err)
	}
	rec, err := r.Sender().PromiseGet(ctx, "target-url")
	if err != nil {
		t.Fatal(err)
	}
	if got := rec.Tags["resonate:target"]; got != url {
		t.Errorf("target = %q, want %q", got, url)
	}
}

func TestRPCDefaultTargetUsesGroup(t *testing.T) {
	r := newLocal(t, localConfig{})
	// Register a no-op so the local worker (which is in the default group and
	// thus accepts default-target dispatches) executes cleanly instead of
	// logging "function not found".
	if _, err := resonate.Register(r, "remote", noop); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := testCtx(t)
	defer cancel()
	h, err := r.RPC(ctx, "target-default", "remote", nil)
	if err != nil {
		t.Fatal(err)
	}
	rec, err := r.Sender().PromiseGet(ctx, "target-default")
	if err != nil {
		t.Fatal(err)
	}
	if got, want := rec.Tags["resonate:target"], "local://any@default"; got != want {
		t.Errorf("target = %q, want %q", got, want)
	}
	// Drain so the worker finishes before cancel — avoids racy "context
	// canceled" log noise from the in-flight execution.
	if err := h.Result(ctx, nil); err != nil {
		t.Fatalf("Result: %v", err)
	}
}

// ──────────────────────────────────────────────────────────────────────────
// Get
// ──────────────────────────────────────────────────────────────────────────

func TestGetNonExistentReturns404(t *testing.T) {
	r := newLocal(t, localConfig{})
	ctx, cancel := testCtx(t)
	defer cancel()
	_, err := r.Get(ctx, "nonexistent")
	var se *resonate.ServerError
	if !errors.As(err, &se) {
		t.Fatalf("expected *ServerError, got %v", err)
	}
	if se.Code != 404 {
		t.Errorf("code = %d, want 404", se.Code)
	}
}

func TestGetExistingPromise(t *testing.T) {
	r := newLocal(t, localConfig{})
	ctx, cancel := testCtx(t)
	defer cancel()
	if _, err := r.RPC(ctx, "get-test", "remote", nil, resonate.RPCOptions{Target: "unhandled"}); err != nil {
		t.Fatal(err)
	}
	h, err := r.Get(ctx, "get-test")
	if err != nil {
		t.Fatal(err)
	}
	if h.ID() != "get-test" {
		t.Errorf("id = %q, want get-test", h.ID())
	}
}

func TestGetWithPrefix(t *testing.T) {
	r := newLocal(t, localConfig{Prefix: "ns"})
	ctx, cancel := testCtx(t)
	defer cancel()
	if _, err := r.RPC(ctx, "p1", "remote", nil, resonate.RPCOptions{Target: "unhandled"}); err != nil {
		t.Fatal(err)
	}
	h, err := r.Get(ctx, "p1")
	if err != nil {
		t.Fatal(err)
	}
	if got, want := h.ID(), "ns:p1"; got != want {
		t.Errorf("id = %q, want %q", got, want)
	}
}

// ──────────────────────────────────────────────────────────────────────────
// IsURL
// ──────────────────────────────────────────────────────────────────────────

func TestIsURL(t *testing.T) {
	cases := []struct {
		in   string
		want bool
	}{
		{"http://localhost:8001", true},
		{"https://example.com/path", true},
		{"local://any@hello", true},
		{"custom://group/worker", true},
		{"hello", false},
		{"my_func", false},
		{"default", false},
		{"", false},
	}
	for _, c := range cases {
		if got := resonate.IsURL(c.in); got != c.want {
			t.Errorf("IsURL(%q) = %v, want %v", c.in, got, c.want)
		}
	}
}

// ──────────────────────────────────────────────────────────────────────────
// Stop
// ──────────────────────────────────────────────────────────────────────────

func TestStopIsClean(t *testing.T) {
	pid := "default"
	r, err := resonate.New(resonate.Config{
		Network:   localnet.NewLocal("default", &pid),
		Heartbeat: resonate.NoopHeartbeat{},
		TTL:       time.Hour,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := r.Stop(); err != nil {
		t.Errorf("Stop: %v", err)
	}
}

func TestStopIsIdempotent(t *testing.T) {
	pid := "default"
	r, err := resonate.New(resonate.Config{
		Network:   localnet.NewLocal("default", &pid),
		Heartbeat: resonate.NoopHeartbeat{},
		TTL:       time.Hour,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := r.Stop(); err != nil {
		t.Fatal(err)
	}
	if err := r.Stop(); err != nil {
		t.Errorf("second Stop: %v", err)
	}
}

// ──────────────────────────────────────────────────────────────────────────
// Prefix consistency
// ──────────────────────────────────────────────────────────────────────────

func TestPrefixConsistentAcrossRunRPCGet(t *testing.T) {
	r := newLocal(t, localConfig{Prefix: "p"})
	noopFn, err := resonate.Register(r, "noop", noop)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := testCtx(t)
	defer cancel()

	h1, err := noopFn.Run(ctx, "id1", struct{}{})
	if err != nil {
		t.Fatal(err)
	}
	if h1.ID() != "p:id1" {
		t.Errorf("run id = %q, want p:id1", h1.ID())
	}

	h2, err := r.RPC(ctx, "id2", "remote", nil, resonate.RPCOptions{Target: "unhandled"})
	if err != nil {
		t.Fatal(err)
	}
	if h2.ID() != "p:id2" {
		t.Errorf("rpc id = %q, want p:id2", h2.ID())
	}

	h3, err := r.Get(ctx, "id2")
	if err != nil {
		t.Fatal(err)
	}
	if h3.ID() != "p:id2" {
		t.Errorf("get id = %q, want p:id2", h3.ID())
	}
}

// ──────────────────────────────────────────────────────────────────────────
// Multiple operations
// ──────────────────────────────────────────────────────────────────────────

func TestMultipleRunsWithDifferentIDs(t *testing.T) {
	r := newLocal(t, localConfig{})
	noopFn, err := resonate.Register(r, "noop", noop)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := testCtx(t)
	defer cancel()

	for _, id := range []string{"m1", "m2", "m3"} {
		h, err := noopFn.Run(ctx, id, struct{}{})
		if err != nil {
			t.Fatalf("Run %s: %v", id, err)
		}
		if h.ID() != id {
			t.Errorf("id = %q, want %q", h.ID(), id)
		}
	}
}

func TestMixedRunAndRPC(t *testing.T) {
	r := newLocal(t, localConfig{})
	noopFn, err := resonate.Register(r, "noop", noop)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := testCtx(t)
	defer cancel()

	if _, err := noopFn.Run(ctx, "local-1", struct{}{}); err != nil {
		t.Fatal(err)
	}
	if _, err := r.RPC(ctx, "remote-1", "remote-fn", nil, resonate.RPCOptions{Target: "unhandled"}); err != nil {
		t.Fatal(err)
	}
}

// ──────────────────────────────────────────────────────────────────────────
// Subscription / Handle mechanics
// ──────────────────────────────────────────────────────────────────────────

// TestMultipleHandlesSameIDAllResolve verifies that two Handles pointing at
// the same id both wake up when the underlying promise settles. End-to-end
// via the LocalNetwork: the worker executes noop, server pushes unblock,
// both handles' Result return.
func TestMultipleHandlesSameIDAllResolve(t *testing.T) {
	r := newLocal(t, localConfig{})
	noopFn, err := resonate.Register(r, "noop", noop)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := testCtx(t)
	defer cancel()

	h1, err := noopFn.Run(ctx, "multi", struct{}{})
	if err != nil {
		t.Fatal(err)
	}
	h2, err := r.Get(ctx, "multi")
	if err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	wg.Add(2)
	for _, h := range []*resonate.Handle{h1.Untyped(), h2} {
		h := h
		go func() {
			defer wg.Done()
			var v any
			if err := h.Result(ctx, &v); err != nil {
				t.Errorf("Result for %s: %v", h.ID(), err)
			}
		}()
	}
	wg.Wait()
}

// TestSettledAtListenerTime verifies the path where PromiseRegisterListener
// returns an already-settled promise: the handle resolves immediately
// without waiting for an Unblock push.
func TestSettledAtListenerTime(t *testing.T) {
	r := newLocal(t, localConfig{})
	noopFn, err := resonate.Register(r, "noop", noop)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := testCtx(t)
	defer cancel()

	// Run and wait for settlement.
	h1, err := noopFn.Run(ctx, "settled-listener", struct{}{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := h1.Result(ctx); err != nil {
		t.Fatalf("h1.Result: %v", err)
	}

	// Now Get a fresh handle. The PromiseRegisterListener call inside Get
	// should observe the resolved state and settle the new subscription
	// immediately, so this should not block.
	h2, err := r.Get(ctx, "settled-listener")
	if err != nil {
		t.Fatal(err)
	}
	if err := h2.Result(ctx, new(any)); err != nil {
		t.Fatalf("h2.Result: %v", err)
	}
}

func TestErrorPropagationViaResult(t *testing.T) {
	r := newLocal(t, localConfig{})
	failFn, err := resonate.Register(r, "fail_always", failAlways)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := testCtx(t)
	defer cancel()

	h, err := failFn.Run(ctx, "fail-1", "boom")
	if err != nil {
		t.Fatal(err)
	}
	_, err = h.Result(ctx)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	var appErr *resonate.ApplicationError
	if !errors.As(err, &appErr) {
		t.Fatalf("expected ApplicationError, got %T: %v", err, err)
	}
	if !strings.Contains(appErr.Message, "boom") {
		t.Errorf("error %q should mention boom", appErr.Message)
	}
}
