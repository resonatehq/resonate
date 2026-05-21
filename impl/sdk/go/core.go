package resonate

import (
	stdctx "context"
	"encoding/json"
	"fmt"
	"log/slog"
)

// Status reports the outcome of a single Core invocation: the workflow
// completed (StatusDone), suspended waiting on remote work (StatusSuspended),
// or failed with an error (StatusErr). StatusErr is always paired with a
// non-nil error from the call.
type Status int

const (
	// StatusDone means the workflow ran to completion and its root promise has been fulfilled.
	StatusDone Status = iota
	// StatusSuspended means the workflow is waiting on one or more remote promises
	// and has been suspended; callbacks are registered so it resumes when they settle.
	StatusSuspended
	// StatusErr means the workflow could not be executed or suspended; the accompanying
	// non-nil error from Core.OnMessage or Core.ExecuteUntilBlocked carries the cause.
	StatusErr
)

func (s Status) String() string {
	switch s {
	case StatusDone:
		return "done"
	case StatusSuspended:
		return "suspended"
	case StatusErr:
		return "err"
	default:
		return fmt.Sprintf("Status(%d)", int(s))
	}
}

// Core orchestrates the full lifecycle of one task: acquire (via OnMessage)
// or skip-acquire (via ExecuteUntilBlocked), execute the registered function
// inside a panic/flush boundary, then fulfill on Done, suspend on remote
// work, or release on error. All methods are safe to call concurrently from
// many goroutines — the typical worker spawns one goroutine per incoming
// execute message.
type Core struct {
	sender    *Sender
	codec     *Codec
	registry  *Registry
	resolver  TargetResolver
	heartbeat Heartbeat
	pid       string
	ttl       int64
	log       *slog.Logger
}

// NewCore builds a Core. resolver may be nil — it falls back to
// IdentityTargetResolver. heartbeat may be nil — it falls back to NoopHeartbeat.
func NewCore(sender *Sender, codec *Codec, reg *Registry, resolver TargetResolver,
	hb Heartbeat, pid string, ttl int64) *Core {
	if resolver == nil {
		resolver = IdentityTargetResolver
	}
	if hb == nil {
		hb = NoopHeartbeat{}
	}
	return &Core{
		sender:    sender,
		codec:     codec,
		registry:  reg,
		resolver:  resolver,
		heartbeat: hb,
		pid:       pid,
		ttl:       ttl,
		log:       slog.Default(),
	}
}

// ═══════════════════════════════════════════════════════════════
//  Path 1: OnMessage — acquire then execute
// ═══════════════════════════════════════════════════════════════

// OnMessage handles an execute push message: acquires the task, decodes
// the root promise, and runs ExecuteUntilBlocked.
func (c *Core) OnMessage(ctx stdctx.Context, taskID string, version int64) (Status, error) {
	res, err := c.sender.TaskAcquire(ctx, taskID, version, c.pid, c.ttl)
	if err != nil {
		return StatusErr, err
	}
	c.log.Debug("core: task acquired", "task_id", taskID)

	promise, err := c.codec.DecodePromise(res.Promise)
	if err != nil {
		return StatusErr, err
	}

	return c.ExecuteUntilBlocked(ctx, taskID, res.Task.Version, promise, res.Preload)
}

// ═══════════════════════════════════════════════════════════════
//  Path 2: ExecuteUntilBlocked — task already acquired
// ═══════════════════════════════════════════════════════════════

// ExecuteUntilBlocked runs an already-acquired task to completion or
// suspension. Caller is responsible for the acquire step (and for passing a
// promise whose Param/Value have been run through Codec.DecodePromise).
//
// It owns the task lifecycle: it builds Effects, drives the redirect loop,
// fulfills or suspends based on the inner's outcome, and releases on error.
// executeUntilBlockedInner runs the workflow body and reports back an
// execOutcome — it does not touch task lifecycle state itself.
func (c *Core) ExecuteUntilBlocked(ctx stdctx.Context, taskID string, taskVersion int64,
	promise PromiseRecord, preload []PromiseRecord) (status Status, retErr error) {

	c.heartbeat.Start(taskID, taskVersion)
	defer c.heartbeat.Stop(taskID)

	defer func() {
		if retErr != nil {
			c.log.Error("core: execution failed, releasing task",
				"err", retErr, "task_id", taskID, "promise_id", promise.ID)
			if relErr := c.releaseTask(ctx, taskID, taskVersion); relErr != nil {
				c.log.Error("core: failed to release task after error",
					"err", relErr, "task_id", taskID)
			}
		}
	}()

	c.log.Debug("core: starting execution", "task_id", taskID, "promise_id", promise.ID)

	currentPreload := preload
	for {
		effects := NewEffects(c.sender, taskID, taskVersion, currentPreload)
		outcome, err := c.executeUntilBlockedInner(ctx, promise, effects)
		if err != nil {
			return StatusErr, err
		}

		switch outcome.kind {
		case execFulfill:
			if err := c.fulfillTaskEncoded(ctx, taskID, taskVersion, promise.ID, outcome.settleState, outcome.value); err != nil {
				return StatusErr, err
			}
			c.log.Debug("core: task fulfilled", "task_id", taskID, "promise_id", promise.ID)
			return StatusDone, nil

		case execSuspend:
			c.log.Debug("core: attempting to suspend task",
				"task_id", taskID, "remote_deps", len(outcome.todos))
			sr, err := c.suspendTask(ctx, taskID, taskVersion, outcome.todos)
			if err != nil {
				return StatusErr, err
			}
			if !sr.Redirected {
				c.log.Debug("core: task suspended", "task_id", taskID)
				return StatusSuspended, nil
			}
			c.log.Debug("core: suspend returned redirect, re-executing task",
				"task_id", taskID, "preload", len(sr.Preload))
			currentPreload = sr.Preload
		}
	}
}

// execOutcome is what executeUntilBlockedInner reports back: either the
// workflow finished and the task should be fulfilled with these encoded
// settle args, or it has remote dependencies and the task should be
// suspended on these awaiteds.
type execOutcome struct {
	kind        execOutcomeKind
	settleState SettleState // for execFulfill
	value       Value       // for execFulfill — already codec-encoded
	todos       []string    // for execSuspend
}

type execOutcomeKind int

const (
	execFulfill execOutcomeKind = iota
	execSuspend
)

// executeUntilBlockedInner runs the workflow body once and returns the
// outcome. It does not call into task lifecycle APIs (fulfill/suspend/
// release) — the caller owns that. It does encode return values through
// the codec so the caller has a single, uniform "fulfill" path.
func (c *Core) executeUntilBlockedInner(ctx stdctx.Context, promise PromiseRecord, effects *Effects) (execOutcome, error) {
	// 1. Decode TaskData from the (already-decoded) promise param.
	var taskData TaskData
	if err := json.Unmarshal(promise.Param.DataOrNull(), &taskData); err != nil {
		return execOutcome{}, &DecodingError{Msg: fmt.Sprintf("invalid task data: %v", err)}
	}
	// JSON unmarshal of `"args":null` yields a 4-byte RawMessage("null"),
	// which durableFunction.invoke would mis-classify as "args provided" for
	// no-arg functions. Normalize null/empty here.
	var args any = taskData.Args
	if len(taskData.Args) == 0 || string(taskData.Args) == "null" {
		args = nil
	}

	// 2. Look up the function in the registry.
	df, ok := c.registry.Get(taskData.Func)
	if !ok {
		return execOutcome{}, &FunctionNotFoundError{Name: taskData.Func}
	}

	// 3. SHORT-CIRCUIT: if the root promise is already settled, report a
	//    fulfill outcome without invoking the function.
	if promise.State != PromiseStatePending {
		c.log.Info("core: promise already settled, fulfilling task without execution",
			"promise_id", promise.ID, "state", promise.State)
		settleState, ok := settleStateFromPromiseState(promise.State)
		if !ok {
			return execOutcome{}, &DecodingError{Msg: fmt.Sprintf("unexpected promise state %q", promise.State)}
		}
		encoded, err := c.codec.Encode(promise.Value.DataOrNull())
		if err != nil {
			return execOutcome{}, err
		}
		return execOutcome{kind: execFulfill, settleState: settleState, value: encoded}, nil
	}

	// 4. EXECUTE the workflow.
	rootCtx := NewRootContext(ctx, promise.ID, promise.TimeoutAt, taskData.Func, effects, c.resolver, c.codec)

	res, runErr, suspended, panicErr := c.invoke(df, rootCtx, args)
	if panicErr != nil {
		return execOutcome{}, panicErr
	}

	// Flush local work and collect remote todos.
	rootCtx.flushLocalWork()
	todos := rootCtx.drainSpawnedRemote()

	// 5. FINALIZE: fulfill when no remote todos remain and the function did
	//    not request suspension.
	if !suspended && len(todos) == 0 {
		var settleState SettleState
		var encoded Value
		if runErr != nil {
			settleState = SettleStateRejected
			v, err := c.codec.Encode(EncodeError(runErr))
			if err != nil {
				return execOutcome{}, err
			}
			encoded = v
		} else {
			settleState = SettleStateResolved
			v, err := c.codec.Encode(res)
			if err != nil {
				return execOutcome{}, err
			}
			encoded = v
		}
		return execOutcome{kind: execFulfill, settleState: settleState, value: encoded}, nil
	}

	// If the function returned Done but there are pending todos, treat as
	// suspended (matches Rust's structured-concurrency rule; covers the
	// fire-and-forget child case).
	if !suspended && len(todos) > 0 {
		c.log.Warn("core: workflow returned Done with pending remote todos — "+
			"either a fire-and-forget child suspended or the function swallowed a panic",
			"func", taskData.Func, "id", promise.ID, "todos", todos)
	}

	return execOutcome{kind: execSuspend, todos: todos}, nil
}

// invoke runs the durable function with panic recovery. Returns:
//   - res, runErr  : the function's normal return values
//   - suspended    : true if the function panicked with suspendSignal{}
//   - panicErr     : non-nil if the function panicked with any other value
//     (converted to *ApplicationError so the outer loop can release the task)
func (c *Core) invoke(df *durableFunction, ctx *Context, args any) (res any, runErr error, suspended bool, panicErr error) {
	defer func() {
		r := recover()
		if r == nil {
			return
		}
		if _, ok := r.(suspendSignal); ok {
			suspended = true
			return
		}
		msg := fmt.Sprintf("%v", r)
		c.log.Error("core: user function panicked", "task", ctx.ID(), "panic", msg)
		panicErr = &ApplicationError{Message: fmt.Sprintf("user function panicked: %s", msg)}
	}()
	res, runErr = df.invoke(ctx, args)
	return
}

// ═══════════════════════════════════════════════════════════════
//  Task lifecycle helpers
// ═══════════════════════════════════════════════════════════════

// fulfillTaskEncoded sends an already-encoded value via TaskFulfill.
func (c *Core) fulfillTaskEncoded(ctx stdctx.Context, taskID string, taskVersion int64,
	promiseID string, state SettleState, encoded Value) error {

	_, err := c.sender.TaskFulfill(ctx, taskID, taskVersion, PromiseSettleReq{
		ID:    promiseID,
		State: state,
		Value: encoded,
	})
	return err
}

// suspendTask registers callbacks for each remote todo and suspends the task.
// A redirect response (SuspendResult.Redirected) means at least one awaited
// promise is already settled; the caller should retry rather than suspend.
func (c *Core) suspendTask(ctx stdctx.Context, taskID string, taskVersion int64,
	todos []string) (SuspendResult, error) {

	actions := make([]PromiseRegisterCallbackData, len(todos))
	for i, awaited := range todos {
		actions[i] = PromiseRegisterCallbackData{Awaited: awaited, Awaiter: taskID}
	}
	return c.sender.TaskSuspend(ctx, taskID, taskVersion, actions)
}

// releaseTask releases the lease on a task so another worker can retry it.
func (c *Core) releaseTask(ctx stdctx.Context, taskID string, taskVersion int64) error {
	return c.sender.TaskRelease(ctx, taskID, taskVersion)
}

// settleStateFromPromiseState maps a settled promise state to its settle action.
// Pending is not settled and is unexpected here.
func settleStateFromPromiseState(s PromiseState) (SettleState, bool) {
	switch s {
	case PromiseStateResolved:
		return SettleStateResolved, true
	case PromiseStateRejected, PromiseStateRejectedTimedout:
		return SettleStateRejected, true
	case PromiseStateRejectedCanceled:
		return SettleStateRejectedCanceled, true
	default:
		return "", false
	}
}
