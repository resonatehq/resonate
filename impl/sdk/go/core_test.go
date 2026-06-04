package resonate_test

import (
	stdctx "context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"

	resonate "github.com/resonatehq/resonate-sdk-go"
	"github.com/resonatehq/resonate-sdk-go/localnet"
)

// ── Test harness ────────────────────────────────────────────────────────

// coreFixture wires a live localnet + sender + codec + registry + core. All
// Core tests share this setup so the contract is "real server, real wire".
type coreFixture struct {
	ctx    stdctx.Context
	cancel stdctx.CancelFunc
	ln     *localnet.LocalNetwork
	sender *resonate.Sender
	codec  *resonate.Codec
	reg    *resonate.Registry
	core   *resonate.Core
	pid    string
	hb     *trackingHeartbeat
}

func newCoreFixture(t *testing.T) *coreFixture {
	t.Helper()
	pid := "core-test-pid"
	ln := localnet.NewLocal("test", &pid)
	ctx, cancel := stdctx.WithCancel(stdctx.Background())
	if err := ln.Start(ctx); err != nil {
		cancel()
		t.Fatalf("start localnet: %v", err)
	}

	sender := resonate.NewSender(ln, nil)
	codec := resonate.NewCodec(nil)
	reg := resonate.NewRegistry()
	hb := &trackingHeartbeat{}
	core := resonate.NewCore(sender, codec, reg, resonate.IdentityTargetResolver, hb, pid, 10_000)

	t.Cleanup(func() {
		cancel()
		_ = ln.Stop()
	})

	return &coreFixture{
		ctx:    ctx,
		cancel: cancel,
		ln:     ln,
		sender: sender,
		codec:  codec,
		reg:    reg,
		core:   core,
		pid:    pid,
		hb:     hb,
	}
}

// createRootTask creates a root durable promise + task atomically. The task
// is acquired by us (the fixture's pid) at version 0. funcName and args go
// into the promise param as TaskData.
func (f *coreFixture) createRootTask(t *testing.T, id, funcName string, args any) (int64, resonate.PromiseRecord, []resonate.PromiseRecord) {
	t.Helper()
	param, err := f.codec.Encode(map[string]any{"func": funcName, "args": args})
	if err != nil {
		t.Fatalf("encode task data: %v", err)
	}
	res, err := f.sender.TaskCreate(f.ctx, f.pid, 10_000, resonate.PromiseCreateReq{
		ID:        id,
		TimeoutAt: int64(1) << 50,
		Param:     param,
		Tags:      map[string]string{"resonate:branch": id, "resonate:target": "any"},
	})
	if err != nil {
		t.Fatalf("task.create: %v", err)
	}
	if res.Conflict || res.Created == nil {
		t.Fatalf("task.create unexpected conflict")
	}
	decoded, err := f.codec.DecodePromise(res.Created.Promise)
	if err != nil {
		t.Fatalf("decode promise: %v", err)
	}
	return res.Created.Task.Version, decoded, res.Created.Preload
}

// createRootTaskWithTags is createRootTask with caller-supplied tags merged in.
// Use it to simulate a promise that was spawned by another workflow — i.e. one
// that already carries a resonate:origin tag pointing at a different root.
func (f *coreFixture) createRootTaskWithTags(t *testing.T, id, funcName string, args any, extraTags map[string]string) (int64, resonate.PromiseRecord, []resonate.PromiseRecord) {
	t.Helper()
	param, err := f.codec.Encode(map[string]any{"func": funcName, "args": args})
	if err != nil {
		t.Fatalf("encode task data: %v", err)
	}
	tags := map[string]string{"resonate:branch": id, "resonate:target": "any"}
	for k, v := range extraTags {
		tags[k] = v
	}
	res, err := f.sender.TaskCreate(f.ctx, f.pid, 10_000, resonate.PromiseCreateReq{
		ID:        id,
		TimeoutAt: int64(1) << 50,
		Param:     param,
		Tags:      tags,
	})
	if err != nil {
		t.Fatalf("task.create: %v", err)
	}
	if res.Conflict || res.Created == nil {
		t.Fatalf("task.create unexpected conflict")
	}
	decoded, err := f.codec.DecodePromise(res.Created.Promise)
	if err != nil {
		t.Fatalf("decode promise: %v", err)
	}
	return res.Created.Task.Version, decoded, res.Created.Preload
}

// promiseGet fetches a promise and decodes it through the codec. Use this
// for the *root* promise (Core fulfills root promises through codec.Encode).
func (f *coreFixture) promiseGet(t *testing.T, id string) resonate.PromiseRecord {
	t.Helper()
	rec, err := f.sender.PromiseGet(f.ctx, id)
	if err != nil {
		t.Fatalf("promise.get %s: %v", id, err)
	}
	decoded, err := f.codec.DecodePromise(rec)
	if err != nil {
		t.Fatalf("decode promise: %v", err)
	}
	return decoded
}

// promiseGetRaw fetches a promise without codec decoding. Use this when the
// test only inspects state/tags and doesn't need to read param/value contents.
func (f *coreFixture) promiseGetRaw(t *testing.T, id string) resonate.PromiseRecord {
	t.Helper()
	rec, err := f.sender.PromiseGet(f.ctx, id)
	if err != nil {
		t.Fatalf("promise.get %s: %v", id, err)
	}
	return rec
}

// trackingHeartbeat counts Start/Stop calls for Core's heartbeat hook.
type trackingHeartbeat struct {
	started atomic.Int64
	stopped atomic.Int64
}

func (h *trackingHeartbeat) Start(string, int64) { h.started.Add(1) }
func (h *trackingHeartbeat) Stop(string)         { h.stopped.Add(1) }
func (h *trackingHeartbeat) Shutdown()           {}

// ── Workflow library used across tests ──────────────────────────────────

func wfReturnSeven() (int, error) { return 7, nil }

func wfReturnObj() (map[string]any, error) {
	return map[string]any{"x": 1}, nil
}

func wfFail() (int, error) {
	return 0, &resonate.ApplicationError{Message: "deliberate failure"}
}

type addArgs struct {
	A int `json:"a"`
	B int `json:"b"`
}

func wfAdd(args addArgs) (int, error) { return args.A + args.B, nil }

// wfSuspendOnPending awaits a remote child created in this run. ctx.RPC
// creates the child Pending; Await suspends.
func wfSuspendOnPending(c *resonate.Context) (int, error) {
	fut, err := c.RPC("childA", nil)
	if err != nil {
		return 0, err
	}
	return 0, fut.Await(nil)
}

// wfSuspendOnTwo registers two pending awaiteds.
func wfSuspendOnTwo(c *resonate.Context) (int, error) {
	fut1, _ := c.RPC("childA", nil)
	fut2, _ := c.RPC("childB", nil)
	_ = fut2
	_ = fut1.Await(nil)
	return 0, fut2.Await(nil)
}

// wfReadPreloaded reads a remote child the test pre-resolved on the server.
// ctx.RPC returns the resolved record; Await resolves inline.
func wfReadPreloaded(c *resonate.Context) (int, error) {
	fut, err := c.RPC("preloaded", nil)
	if err != nil {
		return 0, err
	}
	var v int
	if err := fut.Await(&v); err != nil {
		return 0, err
	}
	return v, nil
}

func wfPlainPanic(*resonate.Context) (int, error) {
	panic("something went wrong")
}

// wfUnwrapSuspend mimics a user message that mentions suspension — the
// classification logic should still treat it as a plain panic.
func wfUnwrapSuspend(*resonate.Context) (int, error) {
	panic("execution suspended (simulated .unwrap() on suspended future)")
}

// ── Fulfill (success / failure / object value) ──────────────────────────

func TestCore_FulfillResolved_ViaExecuteUntilBlocked(t *testing.T) {
	f := newCoreFixture(t)
	if err := f.reg.Register("add", wfAdd); err != nil {
		t.Fatalf("register: %v", err)
	}
	v, promise, preload := f.createRootTask(t, "p1-add", "add", addArgs{A: 3, B: 4})

	status, err := f.core.ExecuteUntilBlocked(f.ctx, "p1-add", v, promise, preload, nil)
	if err != nil {
		t.Fatalf("ExecuteUntilBlocked: %v", err)
	}
	if status != resonate.StatusDone {
		t.Fatalf("status = %v, want StatusDone", status)
	}

	got := f.promiseGet(t, "p1-add")
	if got.State != resonate.PromiseStateResolved {
		t.Fatalf("state = %v, want resolved", got.State)
	}
	var n int
	if err := got.Value.Decode(&n); err != nil {
		t.Fatalf("decode value: %v", err)
	}
	if n != 7 {
		t.Errorf("value = %d, want 7", n)
	}
}

func TestCore_FulfillRejected_ViaExecuteUntilBlocked(t *testing.T) {
	f := newCoreFixture(t)
	if err := f.reg.Register("fail", wfFail); err != nil {
		t.Fatalf("register: %v", err)
	}
	v, promise, preload := f.createRootTask(t, "p1-fail", "fail", nil)

	status, err := f.core.ExecuteUntilBlocked(f.ctx, "p1-fail", v, promise, preload, nil)
	if err != nil {
		t.Fatalf("ExecuteUntilBlocked: %v", err)
	}
	if status != resonate.StatusDone {
		t.Fatalf("status = %v, want StatusDone", status)
	}
	got := f.promiseGet(t, "p1-fail")
	if got.State != resonate.PromiseStateRejected {
		t.Fatalf("state = %v, want rejected", got.State)
	}
}

func TestCore_FulfillObjectValue_RoundTripsCodec(t *testing.T) {
	f := newCoreFixture(t)
	if err := f.reg.Register("obj", wfReturnObj); err != nil {
		t.Fatalf("register: %v", err)
	}
	v, promise, preload := f.createRootTask(t, "p1-obj", "obj", nil)

	if _, err := f.core.ExecuteUntilBlocked(f.ctx, "p1-obj", v, promise, preload, nil); err != nil {
		t.Fatalf("ExecuteUntilBlocked: %v", err)
	}
	got := f.promiseGet(t, "p1-obj")
	var m map[string]any
	if err := got.Value.Decode(&m); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if m["x"] != float64(1) {
		t.Errorf("value = %v, want {x:1}", m)
	}
}

// ── Suspend ─────────────────────────────────────────────────────────────

func TestCore_SuspendsOnPendingRemote(t *testing.T) {
	f := newCoreFixture(t)
	if err := f.reg.Register("waitOne", wfSuspendOnPending); err != nil {
		t.Fatalf("register: %v", err)
	}
	v, promise, preload := f.createRootTask(t, "p1-wait", "waitOne", nil)

	status, err := f.core.ExecuteUntilBlocked(f.ctx, "p1-wait", v, promise, preload, nil)
	if err != nil {
		t.Fatalf("ExecuteUntilBlocked: %v", err)
	}
	if status != resonate.StatusSuspended {
		t.Fatalf("status = %v, want StatusSuspended", status)
	}

	child := f.promiseGetRaw(t, "p1-wait.1")
	if child.State != resonate.PromiseStatePending {
		t.Errorf("childA state = %v, want pending", child.State)
	}
}

func TestCore_SuspendsRegistersAllAwaiteds(t *testing.T) {
	f := newCoreFixture(t)
	if err := f.reg.Register("waitTwo", wfSuspendOnTwo); err != nil {
		t.Fatalf("register: %v", err)
	}
	v, promise, preload := f.createRootTask(t, "p1-two", "waitTwo", nil)

	status, err := f.core.ExecuteUntilBlocked(f.ctx, "p1-two", v, promise, preload, nil)
	if err != nil {
		t.Fatalf("ExecuteUntilBlocked: %v", err)
	}
	if status != resonate.StatusSuspended {
		t.Fatalf("status = %v, want StatusSuspended", status)
	}
}

// ── OnMessage (Path 1: acquires then executes) ──────────────────────────

func TestCore_OnMessage_HappyPath(t *testing.T) {
	f := newCoreFixture(t)
	if err := f.reg.Register("seven", wfReturnSeven); err != nil {
		t.Fatalf("register: %v", err)
	}
	v, _, _ := f.createRootTask(t, "p1-on", "seven", nil)
	// Release so OnMessage can re-acquire under its own lease.
	if err := f.sender.TaskRelease(f.ctx, "p1-on", v); err != nil {
		t.Fatalf("release: %v", err)
	}

	status, err := f.core.OnMessage(f.ctx, "p1-on", v)
	if err != nil {
		t.Fatalf("OnMessage: %v", err)
	}
	if status != resonate.StatusDone {
		t.Fatalf("status = %v, want StatusDone", status)
	}
	got := f.promiseGet(t, "p1-on")
	if got.State != resonate.PromiseStateResolved {
		t.Fatalf("state = %v, want resolved", got.State)
	}
}

func TestCore_OnMessage_AcquireFailureReturnsError(t *testing.T) {
	f := newCoreFixture(t)
	_, err := f.core.OnMessage(f.ctx, "nonexistent-task", 0)
	if err == nil {
		t.Fatal("expected error for nonexistent task")
	}
}

func TestCore_OnMessage_ReturnsSuspended(t *testing.T) {
	f := newCoreFixture(t)
	if err := f.reg.Register("waitOne", wfSuspendOnPending); err != nil {
		t.Fatalf("register: %v", err)
	}
	v, _, _ := f.createRootTask(t, "p1-onsus", "waitOne", nil)
	if err := f.sender.TaskRelease(f.ctx, "p1-onsus", v); err != nil {
		t.Fatalf("release: %v", err)
	}

	status, err := f.core.OnMessage(f.ctx, "p1-onsus", v)
	if err != nil {
		t.Fatalf("OnMessage: %v", err)
	}
	if status != resonate.StatusSuspended {
		t.Fatalf("status = %v, want StatusSuspended", status)
	}
}

// ── ExecuteUntilBlocked specifics ───────────────────────────────────────

func TestCore_ExecuteUntilBlocked_WithPreload(t *testing.T) {
	f := newCoreFixture(t)
	v, promise, _ := f.createRootTask(t, "p1-pre", "readPre", nil)
	if err := f.reg.Register("readPre", wfReadPreloaded); err != nil {
		t.Fatalf("register: %v", err)
	}

	// Pre-resolve the child the workflow will read. ctx.RPC generates the
	// child id "p1-pre.1". Children are codec-encoded on the wire just like
	// root promises, so pre-settle with codec.Encode to match.
	encVal, _ := f.codec.Encode(99)
	if _, err := f.sender.PromiseCreate(f.ctx, resonate.PromiseCreateReq{
		ID: "p1-pre.1", TimeoutAt: int64(1) << 50,
	}); err != nil {
		t.Fatalf("promise.create child: %v", err)
	}
	if _, err := f.sender.PromiseSettle(f.ctx, resonate.PromiseSettleReq{
		ID: "p1-pre.1", State: resonate.SettleStateResolved, Value: encVal,
	}); err != nil {
		t.Fatalf("promise.settle child: %v", err)
	}

	// Feed the preloaded child to Effects via the preload arg too, exercising
	// the seed-at-construction path.
	pre, _ := f.sender.PromiseGet(f.ctx, "p1-pre.1")
	preload := []resonate.PromiseRecord{pre}

	status, err := f.core.ExecuteUntilBlocked(f.ctx, "p1-pre", v, promise, preload, nil)
	if err != nil {
		t.Fatalf("ExecuteUntilBlocked: %v", err)
	}
	if status != resonate.StatusDone {
		t.Fatalf("status = %v, want StatusDone (preloaded resolves inline)", status)
	}
	got := f.promiseGet(t, "p1-pre")
	var n int
	if err := got.Value.Decode(&n); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if n != 99 {
		t.Errorf("value = %d, want 99 (forwarded from preloaded child)", n)
	}
}

// NOTE: Short-circuit tests (Rust core.rs:915-1029) are intentionally
// omitted from this LocalNetwork-driven suite. The short-circuit branch
// fires when Core encounters an acquired task whose root promise is
// already settled — but on LocalNetwork, settling a root promise also
// auto-fulfills the task (see local_state.go:1061 enqueueSettle), so
// "Acquired task + Settled root promise" can't be constructed against
// this server. The branch is a 5-line read-only code path
// (executeUntilBlockedInner: `if promise.State != PromiseStatePending`)
// and is exercised by Rust's harness which doesn't auto-fulfill.

// ── Error path: function not found releases the task ────────────────────

func TestCore_ReleasesTaskOnFunctionNotFound(t *testing.T) {
	f := newCoreFixture(t)
	v, promise, preload := f.createRootTask(t, "p1-nofn", "missing", nil)

	_, err := f.core.ExecuteUntilBlocked(f.ctx, "p1-nofn", v, promise, preload, nil)
	if err == nil {
		t.Fatal("expected FunctionNotFoundError, got nil")
	}
	var fnfErr *resonate.FunctionNotFoundError
	if !errors.As(err, &fnfErr) {
		t.Fatalf("error = %T, want *FunctionNotFoundError", err)
	}
	// Task should be releasable: a fresh acquire under a different pid succeeds.
	if _, acqErr := f.sender.TaskAcquire(f.ctx, "p1-nofn", 0, "other-pid", 1000); acqErr != nil {
		t.Errorf("expected acquire after release, got: %v", acqErr)
	}
}

// ── Heartbeat: started and stopped on every code path ──────────────────

func TestCore_HeartbeatStartedAndStopped_OnSuccess(t *testing.T) {
	f := newCoreFixture(t)
	if err := f.reg.Register("seven2", wfReturnSeven); err != nil {
		t.Fatalf("register: %v", err)
	}
	v, promise, preload := f.createRootTask(t, "p1-hb-ok", "seven2", nil)

	if _, err := f.core.ExecuteUntilBlocked(f.ctx, "p1-hb-ok", v, promise, preload, nil); err != nil {
		t.Fatalf("ExecuteUntilBlocked: %v", err)
	}
	if got := f.hb.started.Load(); got != 1 {
		t.Errorf("started = %d, want 1", got)
	}
	if got := f.hb.stopped.Load(); got != 1 {
		t.Errorf("stopped = %d, want 1", got)
	}
}

func TestCore_HeartbeatStoppedOnError(t *testing.T) {
	f := newCoreFixture(t)
	v, promise, preload := f.createRootTask(t, "p1-hb-err", "missing", nil)

	_, _ = f.core.ExecuteUntilBlocked(f.ctx, "p1-hb-err", v, promise, preload, nil)
	if got := f.hb.started.Load(); got != 1 {
		t.Errorf("started = %d, want 1", got)
	}
	if got := f.hb.stopped.Load(); got != 1 {
		t.Errorf("stopped = %d, want 1 (even after error)", got)
	}
}

// ── Panic handling: caught, classified, task released ──────────────────

func TestCore_PlainPanicYieldsApplicationErrorAndReleasesTask(t *testing.T) {
	f := newCoreFixture(t)
	if err := f.reg.Register("boom", wfPlainPanic); err != nil {
		t.Fatalf("register: %v", err)
	}
	v, promise, preload := f.createRootTask(t, "p1-panic", "boom", nil)

	_, err := f.core.ExecuteUntilBlocked(f.ctx, "p1-panic", v, promise, preload, nil)
	if err == nil {
		t.Fatal("expected ApplicationError from panic, got nil")
	}
	var appErr *resonate.ApplicationError
	if !errors.As(err, &appErr) {
		t.Fatalf("error = %T, want *ApplicationError", err)
	}
	if !strings.Contains(appErr.Message, "something went wrong") {
		t.Errorf("error message = %q, want it to contain the panic value", appErr.Message)
	}
	if _, acqErr := f.sender.TaskAcquire(f.ctx, "p1-panic", 0, "other-pid", 1000); acqErr != nil {
		t.Errorf("expected acquire to succeed after release, got: %v", acqErr)
	}
}

func TestCore_PanicMentioningSuspendStillClassifiedAsAppError(t *testing.T) {
	f := newCoreFixture(t)
	if err := f.reg.Register("unwrap", wfUnwrapSuspend); err != nil {
		t.Fatalf("register: %v", err)
	}
	v, promise, preload := f.createRootTask(t, "p1-unwrap", "unwrap", nil)

	_, err := f.core.ExecuteUntilBlocked(f.ctx, "p1-unwrap", v, promise, preload, nil)
	if err == nil {
		t.Fatal("expected ApplicationError, got nil")
	}
	var appErr *resonate.ApplicationError
	if !errors.As(err, &appErr) {
		t.Fatalf("error = %T, want *ApplicationError", err)
	}
}

func TestCore_HeartbeatStoppedAfterPanic(t *testing.T) {
	f := newCoreFixture(t)
	if err := f.reg.Register("boomHb", wfPlainPanic); err != nil {
		t.Fatalf("register: %v", err)
	}
	v, promise, preload := f.createRootTask(t, "p1-hb-panic", "boomHb", nil)

	_, _ = f.core.ExecuteUntilBlocked(f.ctx, "p1-hb-panic", v, promise, preload, nil)
	if got := f.hb.started.Load(); got != 1 {
		t.Errorf("started = %d, want 1", got)
	}
	if got := f.hb.stopped.Load(); got != 1 {
		t.Errorf("stopped = %d, want 1 (even after panic)", got)
	}
}

// ── NoopHeartbeat sanity ───────────────────────────────────────────────

func TestCore_NoopHeartbeatDoesNotInterfere(t *testing.T) {
	pid := "noop-pid"
	ln := localnet.NewLocal("test", &pid)
	ctx, cancel := stdctx.WithCancel(stdctx.Background())
	t.Cleanup(func() { cancel(); _ = ln.Stop() })
	if err := ln.Start(ctx); err != nil {
		t.Fatalf("start: %v", err)
	}
	sender := resonate.NewSender(ln, nil)
	codec := resonate.NewCodec(nil)
	reg := resonate.NewRegistry()
	if err := reg.Register("seven3", wfReturnSeven); err != nil {
		t.Fatalf("register: %v", err)
	}
	core := resonate.NewCore(sender, codec, reg, resonate.IdentityTargetResolver, resonate.NoopHeartbeat{}, pid, 10_000)

	param, _ := codec.Encode(map[string]any{"func": "seven3", "args": nil})
	res, err := sender.TaskCreate(ctx, pid, 10_000, resonate.PromiseCreateReq{
		ID: "p1-noophb", TimeoutAt: int64(1) << 50, Param: param,
		Tags: map[string]string{"resonate:branch": "p1-noophb", "resonate:target": "any"},
	})
	if err != nil || res.Created == nil {
		t.Fatalf("task.create: %v (conflict=%v)", err, res.Conflict)
	}
	decoded, _ := codec.DecodePromise(res.Created.Promise)
	status, err := core.ExecuteUntilBlocked(ctx, "p1-noophb", res.Created.Task.Version, decoded, res.Created.Preload, nil)
	if err != nil {
		t.Fatalf("ExecuteUntilBlocked: %v", err)
	}
	if status != resonate.StatusDone {
		t.Errorf("status = %v, want StatusDone", status)
	}
}

// ── Migrated from context_test.go (RunWorkflow boundary tests) ─────────

func TestCore_DoneOnReturn(t *testing.T) {
	f := newCoreFixture(t)
	done := func(*resonate.Context) (int, error) { return 7, nil }
	if err := f.reg.Register("done", done); err != nil {
		t.Fatalf("register: %v", err)
	}
	v, promise, preload := f.createRootTask(t, "p1-done", "done", nil)

	status, err := f.core.ExecuteUntilBlocked(f.ctx, "p1-done", v, promise, preload, nil)
	if err != nil || status != resonate.StatusDone {
		t.Fatalf("status=%v err=%v, want Done/nil", status, err)
	}
	got := f.promiseGet(t, "p1-done")
	if got.State != resonate.PromiseStateResolved {
		t.Errorf("state=%v, want resolved", got.State)
	}
}

// Migrated: a workflow that swallows the suspension panic but still has
// pending todos must report Suspended.
func TestCore_SwallowedSuspendStillSuspends(t *testing.T) {
	f := newCoreFixture(t)
	wf := func(c *resonate.Context) (int, error) {
		defer func() { _ = recover() }()
		fut, _ := c.RPC("childZ", nil)
		_ = fut.Await(nil) // panics, recovered
		return 0, nil
	}
	if err := f.reg.Register("swallow", wf); err != nil {
		t.Fatalf("register: %v", err)
	}
	v, promise, preload := f.createRootTask(t, "p1-swallow", "swallow", nil)

	status, err := f.core.ExecuteUntilBlocked(f.ctx, "p1-swallow", v, promise, preload, nil)
	if err != nil {
		t.Fatalf("ExecuteUntilBlocked: %v", err)
	}
	if status != resonate.StatusSuspended {
		t.Errorf("status = %v, want StatusSuspended", status)
	}
}

// Migrated: fire-and-forget local child that suspends propagates as parent
// suspension (child todos merge via flushLocalWork).
func TestCore_FireAndForgetLocalSuspension(t *testing.T) {
	f := newCoreFixture(t)
	childThatSuspends := func(c *resonate.Context) (int, error) {
		fut, _ := c.RPC("childW", nil)
		return 0, fut.Await(nil)
	}
	parent := func(c *resonate.Context) (int, error) {
		_, _ = c.Run(childThatSuspends, nil)
		return 0, nil
	}
	if err := f.reg.Register("ffparent", parent); err != nil {
		t.Fatalf("register: %v", err)
	}
	v, promise, preload := f.createRootTask(t, "p1-ff", "ffparent", nil)

	status, err := f.core.ExecuteUntilBlocked(f.ctx, "p1-ff", v, promise, preload, nil)
	if err != nil {
		t.Fatalf("ExecuteUntilBlocked: %v", err)
	}
	if status != resonate.StatusSuspended {
		t.Errorf("status = %v, want StatusSuspended via fire-and-forget child", status)
	}
}

// ── Origin tag propagation ─────────────────────────────────────────────

// When a worker acquires a task for a promise that was spawned by another
// workflow, that promise carries the lineage's true origin in its
// resonate:origin tag. Inner promises created during the run must inherit that
// origin — NOT the acquired promise's own ID. This is the bug fixed in
// executeUntilBlockedInner (seed origin from promise.Tags["resonate:origin"]).
func TestCore_InnerPromiseInheritsOriginFromTag(t *testing.T) {
	f := newCoreFixture(t)
	if err := f.reg.Register("waitOrigin", wfSuspendOnPending); err != nil {
		t.Fatalf("register: %v", err)
	}
	// "child-of-other" is itself a child of root "real-root": it carries the
	// lineage origin in its tag, exactly as a parent's baseTags would stamp it.
	const realRoot = "real-root"
	v, promise, preload := f.createRootTaskWithTags(t, "child-of-other", "waitOrigin", nil,
		map[string]string{"resonate:origin": realRoot})

	status, err := f.core.ExecuteUntilBlocked(f.ctx, "child-of-other", v, promise, preload, nil)
	if err != nil {
		t.Fatalf("ExecuteUntilBlocked: %v", err)
	}
	if status != resonate.StatusSuspended {
		t.Fatalf("status = %v, want StatusSuspended", status)
	}

	// The inner RPC promise (child-of-other.1) must carry the lineage origin,
	// not "child-of-other".
	inner := f.promiseGetRaw(t, "child-of-other.1")
	if got := inner.Tags["resonate:origin"]; got != realRoot {
		t.Errorf("inner resonate:origin = %q, want %q (lineage origin from the acquired promise's tag)", got, realRoot)
	}
}

// A genuine root promise has no inherited origin tag (or its origin tag equals
// its own ID). Inner promises must then fall back to the promise's own ID.
func TestCore_InnerPromiseFallsBackToPromiseIDWhenNoOriginTag(t *testing.T) {
	f := newCoreFixture(t)
	if err := f.reg.Register("waitNoOrigin", wfSuspendOnPending); err != nil {
		t.Fatalf("register: %v", err)
	}
	// createRootTask sets no resonate:origin tag, so the fallback applies.
	v, promise, preload := f.createRootTask(t, "genuine-root", "waitNoOrigin", nil)

	status, err := f.core.ExecuteUntilBlocked(f.ctx, "genuine-root", v, promise, preload, nil)
	if err != nil {
		t.Fatalf("ExecuteUntilBlocked: %v", err)
	}
	if status != resonate.StatusSuspended {
		t.Fatalf("status = %v, want StatusSuspended", status)
	}

	inner := f.promiseGetRaw(t, "genuine-root.1")
	if got := inner.Tags["resonate:origin"]; got != "genuine-root" {
		t.Errorf("inner resonate:origin = %q, want %q (fallback to promise ID)", got, "genuine-root")
	}
}

// NOTE: Redirect-loop tests (Rust core.rs:606-674) are intentionally
// omitted from this LocalNetwork-driven suite. Triggering redirect from a
// real workflow requires an awaited promise to be settled at the exact
// moment Core's TaskSuspend lands — a race that LocalNetwork cannot
// produce deterministically (Future.Await on a settled record doesn't
// register a remote todo, so the workflow can't ask for suspension on a
// settled awaited). The redirect logic itself is a 3-line code path in
// core.go (see executeUntilBlockedInner) and the server-side redirect
// response is covered by localnet/local_test.go's
// TestLocalSuspendRedirectsWhenAwaitedAlreadySettled. End-to-end coverage
// will arrive with the top-level Resonate worker which can be exercised
// across natural race windows.
