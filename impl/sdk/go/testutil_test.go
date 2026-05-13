package resonate

import (
	stdctx "context"
	"encoding/json"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
)

// fakeFenceClient is the unified in-memory fenceClient used by all tests in
// this package. Behaviors a test may configure:
//
//   - preset(id, rec): the next Create for that id returns rec (used to test
//     idempotent recovery — promise already settled when we re-create).
//   - setPreload(id, preload): preload bundle returned by Create(id=...).
//   - enforceVersion + expected{TaskID,Version}: rejects mismatched leases.
//   - nextErr: short-circuits all calls until cleared.
type fakeFenceClient struct {
	mu sync.Mutex

	expectedTaskID  string
	expectedVersion int64
	enforceVersion  bool

	records map[string]PromiseRecord   // current state
	preload map[string][]PromiseRecord // preload bundle for create(id=...)

	lastTaskID  atomic.Value
	lastVersion atomic.Int64
	createCalls atomic.Int32
	settleCalls atomic.Int32

	nextErr error
}

func newFakeFenceClient() *fakeFenceClient {
	return &fakeFenceClient{
		records: map[string]PromiseRecord{},
		preload: map[string][]PromiseRecord{},
	}
}

func (f *fakeFenceClient) preset(id string, rec PromiseRecord) {
	f.mu.Lock()
	f.records[id] = rec
	f.mu.Unlock()
}

func (f *fakeFenceClient) setPreload(id string, p []PromiseRecord) {
	f.mu.Lock()
	f.preload[id] = p
	f.mu.Unlock()
}

func (f *fakeFenceClient) record(id string) (PromiseRecord, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	r, ok := f.records[id]
	return r, ok
}

func (f *fakeFenceClient) TaskFenceCreate(_ stdctx.Context, taskID string, taskVersion int64, req PromiseCreateReq) (TaskFenceResult, error) {
	f.createCalls.Add(1)
	f.lastTaskID.Store(taskID)
	f.lastVersion.Store(taskVersion)
	if f.enforceVersion && (taskID != f.expectedTaskID || taskVersion != f.expectedVersion) {
		return TaskFenceResult{}, errors.New("fence mismatch")
	}
	if f.nextErr != nil {
		return TaskFenceResult{}, f.nextErr
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if rec, ok := f.records[req.ID]; ok {
		return TaskFenceResult{Promise: rec, Preload: f.preload[req.ID]}, nil
	}
	rec := PromiseRecord{
		ID:        req.ID,
		State:     PromiseStatePending,
		Tags:      req.Tags,
		TimeoutAt: req.TimeoutAt,
		Param:     req.Param,
	}
	f.records[req.ID] = rec
	return TaskFenceResult{Promise: rec, Preload: f.preload[req.ID]}, nil
}

func (f *fakeFenceClient) TaskFenceSettle(_ stdctx.Context, taskID string, taskVersion int64, req PromiseSettleReq) (TaskFenceResult, error) {
	f.settleCalls.Add(1)
	f.lastTaskID.Store(taskID)
	f.lastVersion.Store(taskVersion)
	if f.enforceVersion && (taskID != f.expectedTaskID || taskVersion != f.expectedVersion) {
		return TaskFenceResult{}, errors.New("fence mismatch")
	}
	if f.nextErr != nil {
		return TaskFenceResult{}, f.nextErr
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	state := PromiseStateResolved
	switch req.State {
	case SettleStateRejected:
		state = PromiseStateRejected
	case SettleStateRejectedCanceled:
		state = PromiseStateRejectedCanceled
	}
	rec := f.records[req.ID]
	rec.ID = req.ID
	rec.State = state
	rec.Value = req.Value
	f.records[req.ID] = rec
	return TaskFenceResult{Promise: rec}, nil
}

// testContext builds a minimal root Context for unit tests.
func testContext(rootID string, eff *Effects) *Context {
	return &Context{
		host:           stdctx.Background(),
		id:             rootID,
		originID:       rootID,
		branchID:       rootID,
		funcName:       "test",
		timeoutAt:      1 << 62,
		effects:        eff,
		targetResolver: IdentityTargetResolver,
	}
}

// pendingPromise returns a Pending promise record with the given ID.
func pendingPromise(id string) PromiseRecord {
	return PromiseRecord{ID: id, State: PromiseStatePending}
}

// resolvedPromise returns a Resolved promise record carrying the JSON of v.
func resolvedPromise(t *testing.T, id string, v any) PromiseRecord {
	t.Helper()
	raw, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("resolvedPromise marshal: %v", err)
	}
	return PromiseRecord{
		ID:    id,
		State: PromiseStateResolved,
		Value: Value{Data: raw},
	}
}

// rejectedPromise returns a Rejected promise record carrying an application
// error payload with the given message.
func rejectedPromise(t *testing.T, id, msg string) PromiseRecord {
	t.Helper()
	return PromiseRecord{
		ID:    id,
		State: PromiseStateRejected,
		Value: Value{Data: EncodeError(errors.New(msg))},
	}
}

// assertPanicsWithSuspend runs fn and asserts that it panicked with
// suspendSignal{}.
func assertPanicsWithSuspend(t *testing.T, fn func()) {
	t.Helper()
	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("expected suspendSignal panic, got no panic")
		}
		if _, ok := r.(suspendSignal); !ok {
			t.Fatalf("expected suspendSignal, got %T (%v)", r, r)
		}
	}()
	fn()
}
