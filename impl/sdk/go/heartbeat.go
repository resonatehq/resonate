package resonate

import (
	"context"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"
)

// Heartbeat is the lease-renewal contract Core uses to keep an acquired
// task's TTL alive across long-running executions. Start is called before
// execution begins; Stop is called once execution completes (success, error,
// or panic). Implementations must be safe to call concurrently with distinct
// taskIDs — Core may have many tasks in flight in parallel goroutines.
//
// Shutdown is invoked exactly once by Resonate.Stop and must release any
// long-lived resources owned by the implementation (background goroutines,
// timers, network connections). After Shutdown returns, no further Start /
// Stop calls will be made. Implementations that hold no resources may
// implement Shutdown as a no-op.
type Heartbeat interface {
	Start(taskID string, taskVersion int64)
	Stop(taskID string)
	Shutdown()
}

// NoopHeartbeat is the default Heartbeat for local mode and tests where the
// server does not enforce lease TTL.
type NoopHeartbeat struct{}

func (NoopHeartbeat) Start(string, int64) {}
func (NoopHeartbeat) Stop(string)         {}
func (NoopHeartbeat) Shutdown()           {}

// AsyncHeartbeat is a Heartbeat that runs a single background goroutine which
// periodically issues task.heartbeat for every tracked task. Start and Stop
// submit add/remove commands to the actor; the actor owns the task set, so
// no caller-side locking is required. The internal ticker is created on the
// first Start and torn down when the last task is removed, so an idle
// AsyncHeartbeat performs no periodic work. Shutdown halts the actor and
// waits for it to exit.
type AsyncHeartbeat struct {
	pid      string
	interval time.Duration
	sender   *Sender

	cmds   chan heartbeatCmd
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

const heartbeatCmdBuffer = 64

type heartbeatOp uint8

const (
	hbStart heartbeatOp = iota
	hbStop
	hbSnapshot
)

type heartbeatCmd struct {
	op      heartbeatOp
	taskID  string
	version int64
	reply   chan map[string]int64
}

// NewAsyncHeartbeat creates and starts the heartbeat actor goroutine.
// Callers must invoke Shutdown when the heartbeat is no longer needed.
func NewAsyncHeartbeat(pid string, interval time.Duration, sender *Sender) *AsyncHeartbeat {
	ctx, cancel := context.WithCancel(context.Background())
	h := &AsyncHeartbeat{
		pid:      pid,
		interval: interval,
		sender:   sender,
		cmds:     make(chan heartbeatCmd, heartbeatCmdBuffer),
		ctx:      ctx,
		cancel:   cancel,
	}
	h.wg.Add(1)
	go h.run()
	return h
}

// Start adds a task to the heartbeat set. Calling Start with an existing
// taskID updates its tracked version.
func (h *AsyncHeartbeat) Start(taskID string, version int64) {
	h.submit(heartbeatCmd{op: hbStart, taskID: taskID, version: version})
}

// Stop removes a task from the heartbeat set. Stop on an unknown taskID is
// a no-op.
func (h *AsyncHeartbeat) Stop(taskID string) {
	h.submit(heartbeatCmd{op: hbStop, taskID: taskID})
}

// Shutdown cancels the actor and blocks until it has exited. Idempotent.
// Any in-flight task.heartbeat RPC is cancelled via the shared context.
func (h *AsyncHeartbeat) Shutdown() {
	h.cancel()
	h.wg.Wait()
}

// TrackedTasks returns a snapshot of the currently tracked tasks (id →
// version). Returns nil if the actor has shut down.
func (h *AsyncHeartbeat) TrackedTasks() map[string]int64 {
	reply := make(chan map[string]int64, 1)
	if !h.submit(heartbeatCmd{op: hbSnapshot, reply: reply}) {
		return nil
	}
	select {
	case snap := <-reply:
		return snap
	case <-h.ctx.Done():
		return nil
	}
}

func (h *AsyncHeartbeat) submit(cmd heartbeatCmd) bool {
	select {
	case h.cmds <- cmd:
		return true
	case <-h.ctx.Done():
		return false
	}
}

func (h *AsyncHeartbeat) run() {
	defer h.wg.Done()
	tasks := map[string]int64{}

	// The ticker is only allocated while at least one task is tracked. When
	// idle, tickC is nil and that select arm never fires — the actor just
	// blocks on cmds / ctx.Done, doing no periodic work.
	var ticker *time.Ticker
	var tickC <-chan time.Time
	startTicker := func() {
		if ticker == nil {
			ticker = time.NewTicker(h.interval)
			tickC = ticker.C
		}
	}
	stopTicker := func() {
		if ticker != nil {
			ticker.Stop()
			ticker = nil
			tickC = nil
		}
	}
	defer stopTicker()

	// inflight gates the heartbeat RPC: when a tick fires while a previous
	// send is still in flight, we skip rather than serializing or piling up
	// goroutines. A slow server therefore can't stall command processing or
	// queue stale heartbeats — it just drops ticks until the in-flight one
	// returns.
	var inflight atomic.Bool

	for {
		select {
		case <-h.ctx.Done():
			return
		case cmd := <-h.cmds:
			switch cmd.op {
			case hbStart:
				tasks[cmd.taskID] = cmd.version
				startTicker()
			case hbStop:
				delete(tasks, cmd.taskID)
				if len(tasks) == 0 {
					stopTicker()
				}
			case hbSnapshot:
				snap := make(map[string]int64, len(tasks))
				for k, v := range tasks {
					snap[k] = v
				}
				cmd.reply <- snap
			}
		case <-tickC:
			if !inflight.CompareAndSwap(false, true) {
				continue
			}
			refs := make([]TaskRef, 0, len(tasks))
			for id, v := range tasks {
				refs = append(refs, TaskRef{ID: id, Version: v})
			}
			h.wg.Add(1)
			go func(refs []TaskRef) {
				defer h.wg.Done()
				defer inflight.Store(false)
				if err := h.sender.TaskHeartbeat(h.ctx, h.pid, refs); err != nil {
					slog.Default().Warn("heartbeat failed", "err", err)
				}
			}(refs)
		}
	}
}
