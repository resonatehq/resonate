package resonate

import (
	stdctx "context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	mrand "math/rand/v2"
	"time"
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
// the root promise, and runs ExecuteUntilBlocked. The task was dispatched
// by the server (not locally created here), so the per-call retry policy
// is unknown; DefaultRetryPolicy applies.
func (c *Core) OnMessage(ctx stdctx.Context, taskID string, version int64, origin string) (Status, error) {
	res, err := c.sender.TaskAcquire(ctx, taskID, version, c.pid, c.ttl, origin)
	if err != nil {
		return StatusErr, err
	}
	c.log.Debug("core: task acquired", "task_id", taskID)

	promise, err := c.codec.DecodePromise(res.Promise)
	if err != nil {
		return StatusErr, err
	}

	return c.ExecuteUntilBlocked(ctx, taskID, res.Task.Version, promise, res.Preload, nil)
}

// ═══════════════════════════════════════════════════════════════
//  Path 2: ExecuteUntilBlocked — task already acquired
// ═══════════════════════════════════════════════════════════════

// ExecuteUntilBlocked runs an already-acquired task to completion or
// suspension. Caller is responsible for the acquire step (and for passing a
// promise whose Param/Value have been run through Codec.DecodePromise).
//
// retryPolicy governs re-invocation when the registered function returns
// an error; nil applies DefaultRetryPolicy.
//
// It owns the task lifecycle: it builds Effects, drives the redirect loop,
// fulfills or suspends based on the inner's outcome, and releases on error.
// executeUntilBlockedInner runs the workflow body and reports back an
// execOutcome — it does not touch task lifecycle state itself.
func (c *Core) ExecuteUntilBlocked(ctx stdctx.Context, taskID string, taskVersion int64,
	promise PromiseRecord, preload []PromiseRecord, retryPolicy RetryPolicy) (status Status, retErr error) {

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

	// Seed the origin once from the acquired promise: a promise spawned by
	// another workflow carries the lineage's true origin in its resonate:origin
	// tag, and both the execution context and every effect-driven mutation must
	// use it. Fall back to the promise's own ID for a genuine root.
	origin := originFromPromise(promise)

	currentPreload := preload
	for {
		effects := NewEffects(c.sender, taskID, taskVersion, origin, currentPreload)
		outcome, err := c.executeUntilBlockedInner(ctx, promise, origin, effects, retryPolicy)
		if err != nil {
			return StatusErr, err
		}

		switch outcome.kind {
		case execFulfill:
			if err := c.fulfillTaskEncoded(ctx, taskID, taskVersion, origin, promise.ID, outcome.settleState, outcome.value); err != nil {
				return StatusErr, err
			}
			c.log.Debug("core: task fulfilled", "task_id", taskID, "promise_id", promise.ID)
			return StatusDone, nil

		case execSuspend:
			c.log.Debug("core: attempting to suspend task",
				"task_id", taskID, "remote_deps", len(outcome.todos))
			sr, err := c.suspendTask(ctx, taskID, taskVersion, origin, outcome.todos)
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
func (c *Core) executeUntilBlockedInner(ctx stdctx.Context, promise PromiseRecord, origin string, effects *Effects, retryPolicy RetryPolicy) (execOutcome, error) {
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

	// 4. EXECUTE the workflow. origin is the lineage's resonate:origin (computed
	//    by the caller from the acquired promise); inner promises inherit it.
	rootCtx := newRootContext(ctx, promise.ID, origin, promise.TimeoutAt, taskData.Func, effects, c.resolver, c.codec)

	res, runErr, suspended, panicErr := invokeWithRetry(ctx, df, rootCtx, args, retryPolicy, c.log)
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

// ═══════════════════════════════════════════════════════════════
//  Task lifecycle helpers
// ═══════════════════════════════════════════════════════════════

// fulfillTaskEncoded sends an already-encoded value via TaskFulfill. origin is
// the lineage's resonate:origin, stamped into the message head.
func (c *Core) fulfillTaskEncoded(ctx stdctx.Context, taskID string, taskVersion int64,
	origin, promiseID string, state SettleState, encoded Value) error {

	_, err := c.sender.TaskFulfill(ctx, taskID, taskVersion, origin, PromiseSettleReq{
		ID:    promiseID,
		State: state,
		Value: encoded,
	})
	return err
}

// suspendTask registers callbacks for each remote todo and suspends the task.
// A redirect response (SuspendResult.Redirected) means at least one awaited
// promise is already settled; the caller should retry rather than suspend.
// origin is the lineage's resonate:origin, stamped into the message head.
func (c *Core) suspendTask(ctx stdctx.Context, taskID string, taskVersion int64,
	origin string, todos []string) (SuspendResult, error) {

	actions := make([]PromiseRegisterCallbackData, len(todos))
	for i, awaited := range todos {
		actions[i] = PromiseRegisterCallbackData{Awaited: awaited, Awaiter: taskID}
	}
	return c.sender.TaskSuspend(ctx, taskID, taskVersion, origin, actions)
}

// releaseTask releases the lease on a task so another worker can retry it.
func (c *Core) releaseTask(ctx stdctx.Context, taskID string, taskVersion int64) error {
	return c.sender.TaskRelease(ctx, taskID, taskVersion)
}

// originFromPromise returns the lineage origin for a run: the acquired
// promise's resonate:origin tag when set (a promise spawned by another workflow
// carries its lineage's origin there), or the promise's own ID for a genuine
// root that has no origin tag.
func originFromPromise(p PromiseRecord) string {
	if o, ok := p.Tags["resonate:origin"]; ok && o != "" {
		return o
	}
	return p.ID
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

// ═══════════════════════════════════════════════════════════════
//  Retry policies
// ═══════════════════════════════════════════════════════════════

// RetryPolicy decides whether and how long to wait before re-invoking a user
// function that returned an error. Built-in implementations: ConstantRetry,
// LinearRetry, ExponentialRetry. Implement this interface for custom logic.
//
// Lives with Core (not Resonate) because retry behavior is a property of how
// the execution layer drives user code, independent of any specific transport
// or top-level entrypoint struct.
type RetryPolicy interface {
	// NextDelay reports how long to wait before the next attempt and whether
	// to retry at all. attempt is 1-indexed and is the attempt that just
	// failed. Returning retry=false stops the loop and propagates err to the
	// caller (the promise is settled rejected).
	NextDelay(attempt int, err error) (delay time.Duration, retry bool)
}

// ConstantRetry retries with a fixed delay between attempts.
type ConstantRetry struct {
	// MaxAttempts is the total number of attempts including the first; a
	// value of 1 (or less) disables retries.
	MaxAttempts int
	// Delay is the wait between attempts.
	Delay time.Duration
}

// NextDelay implements RetryPolicy.
func (p ConstantRetry) NextDelay(attempt int, _ error) (time.Duration, bool) {
	if attempt >= p.MaxAttempts {
		return 0, false
	}
	return p.Delay, true
}

// LinearRetry scales the delay linearly with the attempt number: attempt N
// waits Base*N.
type LinearRetry struct {
	MaxAttempts int
	Base        time.Duration
}

// NextDelay implements RetryPolicy.
func (p LinearRetry) NextDelay(attempt int, _ error) (time.Duration, bool) {
	if attempt >= p.MaxAttempts {
		return 0, false
	}
	return p.Base * time.Duration(attempt), true
}

// ExponentialRetry doubles the delay between attempts up to an optional cap,
// with optional jitter.
type ExponentialRetry struct {
	MaxAttempts int
	// Base is the delay before attempt 2 (after attempt 1 fails). Subsequent
	// delays double: Base, 2*Base, 4*Base, ... up to Max.
	Base time.Duration
	// Max caps the per-attempt delay. Zero means no cap.
	Max time.Duration
	// Jitter, when true, adds a uniform random value in [0, delay/2) to each
	// computed delay. Helps avoid thundering-herd retries.
	Jitter bool
}

// NextDelay implements RetryPolicy.
func (p ExponentialRetry) NextDelay(attempt int, _ error) (time.Duration, bool) {
	if attempt >= p.MaxAttempts {
		return 0, false
	}
	shift := attempt - 1
	if shift < 0 {
		shift = 0
	}
	const maxShift = 62
	if shift > maxShift {
		shift = maxShift
	}
	delay := p.Base << shift
	if p.Max > 0 && delay > p.Max {
		delay = p.Max
	}
	if delay < 0 {
		if p.Max > 0 {
			delay = p.Max
		} else {
			delay = time.Duration(1<<62 - 1)
		}
	}
	if p.Jitter && delay > 0 {
		half := int64(delay / 2)
		if half > 0 {
			delay += time.Duration(mrand.Int64N(half))
		}
	}
	return delay, true
}

// NoRetry disables retries (a single attempt).
var NoRetry RetryPolicy = ConstantRetry{MaxAttempts: 1}

// DefaultRetryPolicy applies when RunOptions.RetryPolicy / RunOpts.RetryPolicy
// is nil. Exposed so callers can read the default in wrappers.
var DefaultRetryPolicy RetryPolicy = ExponentialRetry{
	MaxAttempts: 3,
	Base:        100 * time.Millisecond,
	Max:         30 * time.Second,
	Jitter:      true,
}

// policyOrDefault returns p, or DefaultRetryPolicy when p is nil.
func policyOrDefault(p RetryPolicy) RetryPolicy {
	if p == nil {
		return DefaultRetryPolicy
	}
	return p
}

// ═══════════════════════════════════════════════════════════════
//  Non-retryable errors
// ═══════════════════════════════════════════════════════════════

// NonRetryable marks an error as terminal: the retry loop must not retry it,
// regardless of policy. Use NewNonRetryable to wrap an existing error.
type NonRetryable interface {
	error
	NonRetryable()
}

// NewNonRetryable wraps err so the retry loop treats it as terminal. The
// wrapped error remains reachable via errors.Is / errors.As / errors.Unwrap.
// Passing nil returns nil.
func NewNonRetryable(err error) error {
	if err == nil {
		return nil
	}
	return nonRetryableError{err: err}
}

type nonRetryableError struct{ err error }

func (e nonRetryableError) Error() string { return e.err.Error() }
func (e nonRetryableError) Unwrap() error { return e.err }
func (e nonRetryableError) NonRetryable() {}

// isNonRetryable reports whether err is or wraps a NonRetryable.
func isNonRetryable(err error) bool {
	if err == nil {
		return false
	}
	var nr NonRetryable
	return errors.As(err, &nr)
}

// ═══════════════════════════════════════════════════════════════
//  Invocation: retry loop + per-attempt panic recovery
// ═══════════════════════════════════════════════════════════════

// invokeWithRetry runs df.invoke under policy. On panic, suspendSignal is
// surfaced as suspended=true (workflow suspension, not retried); any other
// panic becomes panicErr (also not retried). Returned errors are retried
// per policy until the policy reports retry=false, the std context is
// cancelled, the promise's TimeoutAt is about to elapse, or the error is
// NonRetryable.
func invokeWithRetry(stdCtx stdctx.Context, df *durableFunction, ctx *Context, args any, policy RetryPolicy, log *slog.Logger) (res any, runErr error, suspended bool, panicErr error) {
	policy = policyOrDefault(policy)
	attempt := 1
	for {
		attemptRes, attemptErr, attemptSuspended, attemptPanic := invokeOnce(df, ctx, args, log)
		if attemptSuspended {
			return nil, nil, true, nil
		}
		if attemptPanic != nil {
			return nil, nil, false, attemptPanic
		}
		if attemptErr == nil {
			return attemptRes, nil, false, nil
		}
		if isNonRetryable(attemptErr) {
			return nil, attemptErr, false, nil
		}
		delay, shouldRetry := policy.NextDelay(attempt, attemptErr)
		if !shouldRetry {
			return nil, attemptErr, false, nil
		}
		if ctx.timeoutAt > 0 {
			if time.Now().UnixMilli()+delay.Milliseconds() >= ctx.timeoutAt {
				return nil, attemptErr, false, nil
			}
		}
		if log != nil {
			log.Debug("retry: function returned error, sleeping before retry",
				"func", df.name, "attempt", attempt, "delay", delay, "err", attemptErr)
		}
		select {
		case <-stdCtx.Done():
			return nil, attemptErr, false, nil
		case <-time.After(delay):
		}
		attempt++
	}
}

// invokeOnce wraps a single df.invoke call with panic recovery.
func invokeOnce(df *durableFunction, ctx *Context, args any, log *slog.Logger) (res any, runErr error, suspended bool, panicErr error) {
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
		if log != nil {
			log.Error("user function panicked", "task", ctx.ID(), "panic", msg)
		}
		panicErr = &ApplicationError{Message: fmt.Sprintf("user function panicked: %s", msg)}
	}()
	res, runErr = df.invoke(ctx, args)
	return
}
