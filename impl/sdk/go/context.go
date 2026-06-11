package resonate

import (
	stdctx "context"
	"fmt"
	"hash/fnv"
	"sync"
	"sync/atomic"
	"time"
)

// DefaultChildTimeout is used by entrypoints when the caller does not pass
// an explicit timeout via the options struct.
const DefaultChildTimeout = 24 * time.Hour

// TargetResolver maps a logical target name (e.g. a function name) to a
// routable address. If override is nil the resolver picks a default.
type TargetResolver func(override *string) string

// IdentityTargetResolver returns the override string unchanged, or the empty
// string when there is no override. Useful for tests and for clusters that
// dispatch by raw function name.
func IdentityTargetResolver(override *string) string {
	if override == nil {
		return ""
	}
	return *override
}

type spawnedLocal struct {
	id     string
	result chan localResult
}

type localResult struct {
	suspended bool
	err       error
}

// Context is the workflow-facing API. It carries a host context.Context
// internally (used to plumb cancellation into effect calls and to carry
// dependencies registered via Resonate.SetDependency) and adds the
// durable-promise entrypoints. The host context's Deadline/Done/Err are
// intentionally NOT exposed: they reflect host cancellation, not workflow
// suspension or durable-promise timeout, and exposing them invites
// workflow code to branch on non-durable signals.
type Context struct {
	host stdctx.Context

	id, originID, branchID, parentID, funcName string
	timeoutAt                                  int64
	seq                                        atomic.Uint32

	effects        *Effects
	targetResolver TargetResolver
	codec          *Codec

	mu            sync.Mutex
	spawnedRemote []string
	spawnedLocals []spawnedLocal
	wg            sync.WaitGroup
}

// NewRootContext constructs a root Context for a top-level workflow run.
// The Resonate worker layer (out of scope here) is the typical caller.
// A nil codec defaults to NewCodec(nil) (NoopEncryptor).
func NewRootContext(host stdctx.Context, id string, timeoutAt int64, funcName string, effects *Effects, resolver TargetResolver, codec *Codec) *Context {
	return newRootContext(host, id, id, timeoutAt, funcName, effects, resolver, codec)
}

// newRootContext is the origin-aware constructor backing NewRootContext. When
// a worker acquires a task for a promise spawned by another workflow (via RPC,
// a remote child, or Detached), that promise already carries the lineage's
// origin in its resonate:origin tag. The worker must seed the execution
// context with that origin — not the promise's own ID — so inner promises
// created during the run inherit the correct origin. A genuine root passes its
// own ID as origin (see NewRootContext).
func newRootContext(host stdctx.Context, id, originID string, timeoutAt int64, funcName string, effects *Effects, resolver TargetResolver, codec *Codec) *Context {
	if host == nil {
		host = stdctx.Background()
	}
	if resolver == nil {
		resolver = IdentityTargetResolver
	}
	if codec == nil {
		codec = NewCodec(nil)
	}
	return &Context{
		host:           host,
		id:             id,
		originID:       originID,
		branchID:       id,
		funcName:       funcName,
		timeoutAt:      timeoutAt,
		effects:        effects,
		targetResolver: resolver,
		codec:          codec,
	}
}

// depKey is the private context-key type under which dependencies registered
// via Resonate.SetDependency travel on the host context, one value per name.
type depKey string

// GetDependency returns the dependency registered under name via
// Resonate.SetDependency, or (nil, false) if no such dependency exists.
// Dependencies are process-local resources (database connections, cloud
// clients, ...) — not durable state. Each worker process must register its
// own; an execution sees the dependencies registered before it started.
func (c *Context) GetDependency(name string) (any, bool) {
	v := c.host.Value(depKey(name))
	return v, v != nil
}

// DependencyOf is the type-safe convenience wrapper around
// Context.GetDependency. It returns false when no dependency is registered
// under name or the registered value is not a T.
func DependencyOf[T any](c *Context, name string) (T, bool) {
	v, _ := c.GetDependency(name)
	t, ok := v.(T)
	return t, ok
}

// ID returns the current execution's promise ID.
func (c *Context) ID() string { return c.id }

// ParentID returns the parent promise's ID (empty for root).
func (c *Context) ParentID() string { return c.parentID }

// OriginID returns the root workflow's ID — stable across the whole tree.
func (c *Context) OriginID() string { return c.originID }

// TimeoutAt returns the promise deadline in epoch milliseconds.
func (c *Context) TimeoutAt() int64 { return c.timeoutAt }

// FuncName returns the registered function name for this execution.
func (c *Context) FuncName() string { return c.funcName }

func (c *Context) appendRemoteTodo(id string) {
	c.mu.Lock()
	c.spawnedRemote = append(c.spawnedRemote, id)
	c.mu.Unlock()
}

func (c *Context) drainSpawnedRemote() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := c.spawnedRemote
	c.spawnedRemote = nil
	return out
}

func (c *Context) child(id, funcName string, timeoutAt int64) *Context {
	return &Context{
		host:           c.host,
		id:             id,
		originID:       c.originID,
		branchID:       id,
		parentID:       c.id,
		funcName:       funcName,
		timeoutAt:      timeoutAt,
		effects:        c.effects,
		targetResolver: c.targetResolver,
		codec:          c.codec,
	}
}

func (c *Context) nextID() string {
	return fmt.Sprintf("%s.%d", c.id, c.seq.Add(1))
}

func (c *Context) childTimeout(requested time.Duration) int64 {
	if requested <= 0 {
		requested = DefaultChildTimeout
	}
	deadline := nowMs() + requested.Milliseconds()
	if deadline > c.timeoutAt {
		return c.timeoutAt
	}
	return deadline
}

// ── Request builders ────────────────────────────────────────────────────

func (c *Context) baseTags(scope, branch string) map[string]string {
	return map[string]string{
		"resonate:scope":  scope,
		"resonate:branch": branch,
		"resonate:parent": c.id,
		"resonate:origin": c.originID,
	}
}

func (c *Context) localCreateReq(id string, args any, timeout time.Duration) (PromiseCreateReq, error) {
	param, err := c.codec.Encode(args)
	if err != nil {
		return PromiseCreateReq{}, err
	}
	return PromiseCreateReq{
		ID:        id,
		TimeoutAt: c.childTimeout(timeout),
		Param:     param,
		Tags:      c.baseTags("local", c.branchID),
	}, nil
}

func (c *Context) remoteCreateReq(id, funcName string, args any, timeout time.Duration, targetOverride *string) (PromiseCreateReq, error) {
	param, err := c.codec.Encode(map[string]any{"func": funcName, "args": args})
	if err != nil {
		return PromiseCreateReq{}, err
	}
	tags := c.baseTags("global", id)
	tags["resonate:target"] = c.targetResolver(targetOverride)
	return PromiseCreateReq{
		ID:        id,
		TimeoutAt: c.childTimeout(timeout),
		Param:     param,
		Tags:      tags,
	}, nil
}

func (c *Context) promiseCreateReq(id string, timeout time.Duration, data any) (PromiseCreateReq, error) {
	var param Value
	if data != nil {
		v, err := c.codec.Encode(data)
		if err != nil {
			return PromiseCreateReq{}, err
		}
		param = v
	}
	return PromiseCreateReq{
		ID:        id,
		TimeoutAt: c.childTimeout(timeout),
		Param:     param,
		Tags:      c.baseTags("global", id),
	}, nil
}

func (c *Context) sleepCreateReq(id string, duration time.Duration) PromiseCreateReq {
	tags := c.baseTags("global", id)
	tags["resonate:timer"] = "true"
	return PromiseCreateReq{
		ID:        id,
		TimeoutAt: c.childTimeout(duration),
		Tags:      tags,
	}
}

// ── Options ─────────────────────────────────────────────────────────────

// RunOpts controls a ctx.Run invocation (local, in-process child workflow).
type RunOpts struct {
	// Timeout caps the child promise's deadline. Zero inherits the parent's
	// remaining deadline (via DefaultChildTimeout if none is set).
	Timeout time.Duration
	// RetryPolicy governs re-execution when the child function returns an
	// error. Nil applies DefaultRetryPolicy (exponential backoff).
	RetryPolicy RetryPolicy
}

// RPCOpts controls a ctx.RPC invocation (remote, cross-worker dispatch).
type RPCOpts struct {
	// Timeout caps the child promise's deadline. Zero inherits the parent's
	// remaining deadline (via DefaultChildTimeout if none is set).
	Timeout time.Duration
	// Target is the routing address for the remote worker. Empty delegates to
	// the Context's TargetResolver, which picks a default group address.
	Target string // empty = use the default target resolver
}

// PromiseOpts controls a latent ctx.Promise creation (caller-settled durable
// promise that the workflow awaits as an external signal).
type PromiseOpts struct {
	// Timeout caps the promise's deadline. Zero uses DefaultChildTimeout.
	Timeout time.Duration
	// Data is an optional initial payload stored in the promise's param field.
	Data any
}

// DetachedOpts controls a ctx.Detached invocation (fire-and-forget remote
// dispatch whose lifecycle is independent of the parent workflow).
type DetachedOpts struct {
	// Timeout caps the detached promise's deadline. Zero uses DefaultChildTimeout.
	Timeout time.Duration
	// Target is the routing address for the remote worker. Empty delegates to
	// the Context's TargetResolver.
	Target string
}

func firstOpt[T any](opts []T) T {
	var zero T
	if len(opts) > 0 {
		return opts[0]
	}
	return zero
}

func optTarget(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

// ── Entrypoints ─────────────────────────────────────────────────────────

// Run executes a local Go function durably. fn must be a function returning
// (T, error) and taking either (a) zero args, (b) one args value, (c)
// *Context, or (d) *Context plus one args value. The promise is created
// synchronously; if already settled (idempotent recovery) Run skips spawning
// the goroutine.
//
// # Execution model
//
// Run spawns one goroutine that invokes fn and (on success) settles the
// child's durable promise. The goroutine is owned by this Context and is
// joined by flushLocalWork before the workflow returns or suspends — see
// the contract notes below.
//
// # Contract for fn
//
//   - fn MUST return promptly. The workflow runtime joins every spawned-local
//     goroutine before it can suspend or fulfill the parent task; a fn that
//     blocks indefinitely will hold the task lease open until it expires and
//     the worker is forcibly evicted. Long-running or external-blocking work
//     belongs in RPC (remote dispatch) or Promise (latent durable promise),
//     not Run.
//   - fn MUST be deterministic enough to tolerate re-execution. Whenever a
//     workflow suspends and is later resumed, the entire workflow body runs
//     again from the top; every Run call repeats. The durable promise layer
//     short-circuits children that have already settled (by ID), but a fn
//     whose body executes before reaching that boundary will execute again.
//     Wrap external side effects (DB writes, payments, emails) in their own
//     ctx.Run / ctx.RPC so the durable promise records the result.
//   - fn MAY itself call ctx.Run / ctx.RPC / ctx.Sleep / ctx.Promise. The
//     Future.Await machinery panics suspendSignal{} on a pending dependency;
//     executeLocal recovers that panic, drains the child's remote todos into
//     the parent, and reports back as suspended.
func (c *Context) Run(fn any, args any, opts ...RunOpts) (*Future, error) {
	df, err := durableFunctionFor(fn)
	if err != nil {
		return nil, err
	}
	opt := firstOpt(opts)
	childID := c.nextID()
	req, err := c.localCreateReq(childID, args, opt.Timeout)
	if err != nil {
		return nil, err
	}

	rec, err := c.effects.CreatePromise(c.host, req)
	if err != nil {
		return nil, err
	}

	if rec.State != PromiseStatePending {
		return &Future{id: childID, ctx: c, kind: futureLocal, record: &rec}, nil
	}

	ch := make(chan localResult, 1)
	f := &Future{id: childID, ctx: c, kind: futureLocal, result: ch}
	childCtx := c.child(childID, df.name, rec.TimeoutAt)

	c.mu.Lock()
	c.spawnedLocals = append(c.spawnedLocals, spawnedLocal{id: childID, result: ch})
	c.mu.Unlock()
	c.wg.Add(1)

	go c.executeLocal(f, df, childCtx, args, opt.RetryPolicy)

	return f, nil
}

func (c *Context) executeLocal(f *Future, df *durableFunction, childCtx *Context, args any, policy RetryPolicy) {
	defer c.wg.Done()
	// Defensive recover: invokeWithRetry already converts user-function
	// panics (including suspendSignal) into return values, so any panic
	// reaching here is from flushLocalWork, codec, or settle paths.
	defer func() {
		r := recover()
		if r == nil {
			return
		}
		f.result <- localResult{err: fmt.Errorf("resonate: panic in %s: %v", df.name, r)}
	}()

	res, runErr, suspended, panicErr := invokeWithRetry(childCtx.host, df, childCtx, args, policy, nil)
	if panicErr != nil {
		f.result <- localResult{err: fmt.Errorf("resonate: panic in %s: %v", df.name, panicErr)}
		return
	}

	childCtx.flushLocalWork()
	childTodos := childCtx.drainSpawnedRemote()

	if suspended {
		if len(childTodos) > 0 {
			c.mu.Lock()
			c.spawnedRemote = append(c.spawnedRemote, childTodos...)
			c.mu.Unlock()
		}
		f.result <- localResult{suspended: true}
		return
	}

	if len(childTodos) > 0 {
		// A child suspended in the background; merge todos and treat parent
		// as suspended too — structured concurrency: the parent cannot
		// complete while a child is pending.
		c.mu.Lock()
		c.spawnedRemote = append(c.spawnedRemote, childTodos...)
		c.mu.Unlock()
		f.result <- localResult{suspended: true}
		return
	}

	settleReq := PromiseSettleReq{ID: f.id}
	if runErr != nil {
		v, err := c.codec.Encode(EncodeError(runErr))
		if err != nil {
			f.result <- localResult{err: err}
			return
		}
		settleReq.State = SettleStateRejected
		settleReq.Value = v
	} else {
		resVal, mErr := c.codec.Encode(res)
		if mErr != nil {
			f.result <- localResult{err: mErr}
			return
		}
		settleReq.State = SettleStateResolved
		settleReq.Value = resVal
	}
	settled, sErr := c.effects.SettlePromise(c.host, settleReq)
	if sErr != nil {
		f.result <- localResult{err: sErr}
		return
	}
	f.record = &settled
	f.result <- localResult{}
}

// RPC dispatches a function remotely via a durable promise. The promise is
// created synchronously and the returned Future's Await yields on
// Pending state.
func (c *Context) RPC(funcName string, args any, opts ...RPCOpts) (*Future, error) {
	opt := firstOpt(opts)
	childID := c.nextID()
	req, err := c.remoteCreateReq(childID, funcName, args, opt.Timeout, optTarget(opt.Target))
	if err != nil {
		return nil, err
	}
	rec, err := c.effects.CreatePromise(c.host, req)
	if err != nil {
		return nil, err
	}
	return newRemoteFuture(childID, c, rec), nil
}

// Sleep creates a durable timer promise. Await on the returned future yields
// (suspends) until the timer elapses. Because workflow functions re-execute
// from the top on resume, any code above a ctx.Sleep call runs again after the
// timer resolves — wrap observable side effects in [Context.Run] so the durable
// child record short-circuits them on replay.
func (c *Context) Sleep(d time.Duration) (*Future, error) {
	childID := c.nextID()
	req := c.sleepCreateReq(childID, d)
	rec, err := c.effects.CreatePromise(c.host, req)
	if err != nil {
		return nil, err
	}
	return newRemoteFuture(childID, c, rec), nil
}

// Promise creates a latent durable promise resolved by an external system
// (webhook, human, another process). Hand the promise ID to the resolver.
func (c *Context) Promise(opts ...PromiseOpts) (*Future, error) {
	opt := firstOpt(opts)
	childID := c.nextID()
	req, err := c.promiseCreateReq(childID, opt.Timeout, opt.Data)
	if err != nil {
		return nil, err
	}
	rec, err := c.effects.CreatePromise(c.host, req)
	if err != nil {
		return nil, err
	}
	return newRemoteFuture(childID, c, rec), nil
}

// Detached dispatches a remote function and returns only its promise ID. It
// is NOT registered in spawnedRemote — the parent workflow does not suspend
// on it. The ID is deterministic: `{origin}.{16-hex FNV-1a 64 of "id.seq"}`.
//
// The hash is FNV-1a 64; other Resonate runtimes may hash differently, so
// Detached IDs are NOT portable across runtimes. If you need cross-runtime
// determinism, do not run the same workflow body on multiple runtimes.
func (c *Context) Detached(funcName string, args any, opts ...DetachedOpts) (string, error) {
	opt := firstOpt(opts)
	raw := c.nextID()
	childID := fmt.Sprintf("%s.%s", c.originID, hashID(raw))
	req, err := c.remoteCreateReq(childID, funcName, args, opt.Timeout, optTarget(opt.Target))
	if err != nil {
		return "", err
	}
	if _, err := c.effects.CreatePromise(c.host, req); err != nil {
		return "", err
	}
	return childID, nil
}

// flushLocalWork waits for every spawned-local goroutine on this context to
// finish. Each goroutine has already merged its todos / set its record by
// the time it exits, so the caller need only wait.
//
// This is an UNBOUNDED wait by design. The structured-concurrency invariant
// requires that all child remote-todos are merged into the parent before the
// parent decides to suspend (otherwise the suspend would register a partial
// awaited list and the missing dependencies would never wake the task). A
// timeout here would trade correctness for liveness; a misbehaving fn that
// blocks forever holds the task lease open instead, which is a recoverable
// failure mode (the server eventually evicts the worker on TTL expiry).
//
// See Context.Run for the contract user functions must respect to keep this
// wait short.
func (c *Context) flushLocalWork() {
	c.wg.Wait()
}

// ── Helpers ─────────────────────────────────────────────────────────────

func nowMs() int64 { return time.Now().UnixMilli() }

func hashID(s string) string {
	h := fnv.New64a()
	_, _ = h.Write([]byte(s))
	return fmt.Sprintf("%016x", h.Sum64())
}
