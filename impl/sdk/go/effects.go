package resonate

import (
	stdctx "context"
	"sync"
)

// fenceClient is the minimal contract Effects needs from the transport.
// *Sender satisfies it directly, so production code wires Effects to a
// Sender with zero adapter glue. Tests use an in-package fake.
type fenceClient interface {
	TaskFenceCreate(ctx stdctx.Context, taskID string, taskVersion int64, req PromiseCreateReq) (TaskFenceResult, error)
	TaskFenceSettle(ctx stdctx.Context, taskID string, taskVersion int64, req PromiseSettleReq) (TaskFenceResult, error)
}

// Effects owns the per-worker side-effecting state: the active task's lease
// (id+version, used as the fencing token on every promise mutation) and an
// in-process promise cache fed by TaskFence{Create,Settle} responses' Preload
// list.
type Effects struct {
	client      fenceClient
	taskID      string
	taskVersion int64

	mu    sync.Mutex
	cache map[string]PromiseRecord
}

// NewEffects constructs an Effects with the given transport and initial
// task lease.
func NewEffects(client fenceClient, taskID string, taskVersion int64) *Effects {
	return &Effects{
		client:      client,
		taskID:      taskID,
		taskVersion: taskVersion,
		cache:       map[string]PromiseRecord{},
	}
}

// CreatePromise creates a durable promise via task.fence, absorbing any
// preloaded records the server returns.
func (e *Effects) CreatePromise(ctx stdctx.Context, req PromiseCreateReq) (PromiseRecord, error) {
	res, err := e.client.TaskFenceCreate(ctx, e.taskID, e.taskVersion, req)
	if err != nil {
		return PromiseRecord{}, err
	}
	e.absorb(append(res.Preload, res.Promise))
	return res.Promise, nil
}

// SettlePromise settles a durable promise via task.fence, absorbing any
// preloaded records the server returns.
func (e *Effects) SettlePromise(ctx stdctx.Context, req PromiseSettleReq) (PromiseRecord, error) {
	res, err := e.client.TaskFenceSettle(ctx, e.taskID, e.taskVersion, req)
	if err != nil {
		return PromiseRecord{}, err
	}
	e.absorb(append(res.Preload, res.Promise))
	return res.Promise, nil
}

// Cached returns a cached promise record by ID, if present.
func (e *Effects) Cached(id string) (PromiseRecord, bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	rec, ok := e.cache[id]
	return rec, ok
}

// absorb merges records into the cache under a single critical section.
// Promise state is monotonic (Pending → terminal, then immutable), so a
// terminal entry in the cache is never overwritten — that prevents a stale
// preload snapshot from clobbering a fresher terminal record written by a
// racing goroutine.
func (e *Effects) absorb(records []PromiseRecord) {
	e.mu.Lock()
	defer e.mu.Unlock()
	for _, p := range records {
		if existing, ok := e.cache[p.ID]; ok && existing.State != PromiseStatePending {
			continue
		}
		e.cache[p.ID] = p
	}
}
