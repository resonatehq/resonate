package resonate

import (
	stdctx "context"
	"fmt"
	"hash/fnv"
	"log/slog"
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

// Outcome is the workflow runtime's view of one execution attempt. A Done
// outcome means the workflow function returned (with Err set if it failed).
// A non-Done outcome means execution suspended on remote work; the runtime
// should register callbacks on RemoteTodos before releasing the lease.
type Outcome struct {
	Done        bool
	Err         error
	RemoteTodos []string
}

// Context is the workflow-facing API. It carries a host context.Context
// internally (used to plumb cancellation into effect calls) and adds the
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

	mu            sync.Mutex
	spawnedRemote []string
	spawnedLocals []spawnedLocal
	wg            sync.WaitGroup
}

// NewRootContext constructs a root Context for a top-level workflow run.
// The Resonate worker layer (out of scope here) is the typical caller.
func NewRootContext(host stdctx.Context, id string, timeoutAt int64, funcName string, effects *Effects, resolver TargetResolver) *Context {
	if host == nil {
		host = stdctx.Background()
	}
	if resolver == nil {
		resolver = IdentityTargetResolver
	}
	return &Context{
		host:           host,
		id:             id,
		originID:       id,
		branchID:       id,
		funcName:       funcName,
		timeoutAt:      timeoutAt,
		effects:        effects,
		targetResolver: resolver,
	}
}

// Value delegates to the embedded host context for dependency injection.
func (c *Context) Value(key any) any { return c.host.Value(key) }

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
	param, err := NewValue(args)
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
	param, err := TaskDataValue(funcName, args)
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
		v, err := NewValue(data)
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

// RunOpts controls a Run invocation.
type RunOpts struct {
	Timeout time.Duration
}

// RPCOpts controls an RPC invocation.
type RPCOpts struct {
	Timeout time.Duration
	Target  string // empty = use the default target resolver
}

// PromiseOpts controls a latent Promise creation.
type PromiseOpts struct {
	Timeout time.Duration
	Data    any
}

// DetachedOpts controls a Detached invocation.
type DetachedOpts struct {
	Timeout time.Duration
	Target  string
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

	go c.executeLocal(f, df, childCtx, args)

	return f, nil
}

func (c *Context) executeLocal(f *Future, df *durableFunction, childCtx *Context, args any) {
	defer c.wg.Done()
	defer func() {
		r := recover()
		if r == nil {
			return
		}
		if _, ok := r.(suspendSignal); ok {
			childCtx.flushLocalWork()
			childTodos := childCtx.drainSpawnedRemote()
			if len(childTodos) > 0 {
				c.mu.Lock()
				c.spawnedRemote = append(c.spawnedRemote, childTodos...)
				c.mu.Unlock()
			}
			f.result <- localResult{suspended: true}
			return
		}
		f.result <- localResult{err: fmt.Errorf("resonate: panic in %s: %v", df.name, r)}
	}()

	res, runErr := df.invoke(childCtx, args)
	childCtx.flushLocalWork()
	childTodos := childCtx.drainSpawnedRemote()
	if len(childTodos) > 0 {
		// A child suspended in the background; merge todos and treat parent
		// as suspended too — matches Rust's structured-concurrency rule.
		c.mu.Lock()
		c.spawnedRemote = append(c.spawnedRemote, childTodos...)
		c.mu.Unlock()
		f.result <- localResult{suspended: true}
		return
	}

	settleReq := PromiseSettleReq{ID: f.id}
	if runErr != nil {
		settleReq.State = SettleStateRejected
		settleReq.Value = Value{Data: EncodeError(runErr)}
	} else {
		resVal, mErr := NewValue(res)
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
// (suspends) until the timer elapses.
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
// The hash is FNV-1a 64 — the Rust SDK uses seahash, so Detached IDs are
// NOT cross-SDK-portable. If you need cross-runtime determinism, do not
// run the same workflow body on both SDKs.
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
func (c *Context) flushLocalWork() {
	c.wg.Wait()
}

// ── Runtime boundary ────────────────────────────────────────────────────

// RunWorkflow invokes a durable function inside the panic-based suspension
// boundary. It recovers suspendSignal{} and converts to Outcome.Suspended;
// any other panic is re-raised. After the function returns, flushLocalWork
// drains in-flight Run goroutines and any merged remote todos convert a
// Done outcome into Suspended (matching Rust's structured-concurrency rule;
// also acts as a safety net for a user who swallowed the suspension panic).
func RunWorkflow(ctx *Context, fn any, args any) Outcome {
	df, err := durableFunctionFor(fn)
	if err != nil {
		return Outcome{Done: true, Err: err}
	}

	var out Outcome
	suspended := false
	func() {
		defer func() {
			r := recover()
			if r == nil {
				return
			}
			if _, ok := r.(suspendSignal); ok {
				suspended = true
				return
			}
			panic(r)
		}()
		_, err := df.invoke(ctx, args)
		if err != nil {
			out = Outcome{Done: true, Err: err}
		} else {
			out = Outcome{Done: true}
		}
	}()

	ctx.flushLocalWork()
	todos := ctx.drainSpawnedRemote()

	if suspended {
		return Outcome{Done: false, RemoteTodos: todos}
	}
	if len(todos) > 0 {
		slog.Warn("resonate: workflow returned Done with pending remote todos — "+
			"either a fire-and-forget child suspended, or the function swallowed a panic with bare recover()",
			"func", ctx.funcName, "id", ctx.id, "todos", todos)
		return Outcome{Done: false, RemoteTodos: todos}
	}
	return out
}

// ── Helpers ─────────────────────────────────────────────────────────────

func nowMs() int64 { return time.Now().UnixMilli() }

func hashID(s string) string {
	h := fnv.New64a()
	_, _ = h.Write([]byte(s))
	return fmt.Sprintf("%016x", h.Sum64())
}
