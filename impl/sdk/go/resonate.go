package resonate

import (
	stdctx "context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/resonatehq/resonate-sdk-go/httpnet"
)

// ──────────────────────────────────────────────────────────────────────────
// Config
// ──────────────────────────────────────────────────────────────────────────

// Config configures a Resonate instance. Callers must supply one of URL or
// Network, or set the RESONATE_URL environment variable. Other fields have
// sensible defaults.
type Config struct {
	// URL, if non-empty, builds a default HTTPNetwork pointing at this address.
	// Takes precedence over both Network and the RESONATE_URL env variable.
	URL string

	// Network is the transport for all server communication. Used when URL is
	// empty. If both URL and Network are empty, the RESONATE_URL env variable
	// is consulted; if all three are empty, New returns ErrNetworkRequired.
	Network Network

	// Heartbeat keeps an acquired task's lease alive. Defaults to a fresh
	// AsyncHeartbeat with interval TTL/2; pass NoopHeartbeat{} for local mode
	// or tests where the server doesn't enforce lease TTL.
	Heartbeat Heartbeat

	// Encryptor controls codec encryption at the durability boundary.
	// Defaults to NoopEncryptor.
	Encryptor Encryptor

	// TTL is the per-task lease duration. Defaults to 60s.
	TTL time.Duration

	// Prefix is prepended (with ":") to every promise/task ID created by
	// Run, RPC, or Get. Empty means no prefix.
	Prefix string

	// Token, if non-empty, is sent as Bearer auth in every protocol request.
	Token string
}

// ErrNetworkRequired is returned by New when none of cfg.URL, cfg.Network, or
// the RESONATE_URL env variable are set.
var ErrNetworkRequired = errors.New("resonate.New: one of cfg.URL, cfg.Network, or RESONATE_URL env is required")

// ──────────────────────────────────────────────────────────────────────────
// Subscription map
// ──────────────────────────────────────────────────────────────────────────

// promiseResult is the settled value carried by a subscription.
type promiseResult struct {
	state PromiseState
	value Value
}

// subscription is the wake-up primitive for a single promise id. Settlement is
// guarded by sync.Once so a duplicate UnblockMessage (e.g. the 60s refresh
// firing after settlement) is safe. Invariant: when done is closed,
// result.Load() != nil.
type subscription struct {
	done   chan struct{}
	result atomic.Pointer[promiseResult]
	once   sync.Once
}

func newSubscription() *subscription {
	return &subscription{done: make(chan struct{})}
}

// settle stores the result and closes done. Idempotent.
func (s *subscription) settle(state PromiseState, value Value) {
	s.once.Do(func() {
		s.result.Store(&promiseResult{state: state, value: value})
		close(s.done)
	})
}

// settled reports whether the subscription has been settled.
func (s *subscription) settled() bool {
	select {
	case <-s.done:
		return true
	default:
		return false
	}
}

// ──────────────────────────────────────────────────────────────────────────
// Options
// ──────────────────────────────────────────────────────────────────────────

// DefaultTopLevelTimeout is the default deadline applied to a top-level Run
// or RPC when the caller does not pass one. Matches the Rust SDK default.
const DefaultTopLevelTimeout = 24 * time.Hour

// RunOptions controls a top-level Run (local execution via RegisteredFunc.Run).
type RunOptions struct {
	// Timeout caps the root promise's deadline. Zero uses DefaultTopLevelTimeout (24h).
	Timeout time.Duration
	// Version is reserved for future use; it is declared but not yet consumed
	// by the SDK when building the root promise create request. See
	// https://github.com/resonatehq/resonate-sdk-go/issues/5 for status.
	Version uint32
	// Tags are merged into the root promise's tag set alongside the SDK's
	// built-in resonate:origin/branch/parent/scope/target tags.
	Tags map[string]string
	// Target is the logical routing address sent as the resonate:target tag.
	// Empty falls back to the Resonate instance's configured network group.
	Target string
}

// RPCOptions controls a top-level RPC (remote dispatch via Resonate.RPC).
type RPCOptions struct {
	// Timeout caps the root promise's deadline. Zero uses DefaultTopLevelTimeout (24h).
	Timeout time.Duration
	// Version is reserved for future use; it is declared but not yet consumed
	// by the SDK when building the root promise create request. See
	// https://github.com/resonatehq/resonate-sdk-go/issues/5 for status.
	Version uint32
	// Tags are merged into the root promise's tag set alongside the SDK's
	// built-in resonate:origin/branch/parent/scope/target tags.
	Tags map[string]string
	// Target is the logical routing address sent as the resonate:target tag.
	// Empty falls back to the Resonate instance's configured network group.
	Target string
}

// ──────────────────────────────────────────────────────────────────────────
// Resonate
// ──────────────────────────────────────────────────────────────────────────

// Resonate is the top-level SDK entrypoint. It owns the network, sender,
// core, registry, heartbeat, and a subscription map that wakes Handles when
// their promise settles via an UnblockMessage from the server.
//
// Goroutine ownership: workflow execution goroutines are spawned by Resonate
// itself — once per ExecuteMessage from the network, once per Run that wins
// the create-and-acquire race. Core.OnMessage and Core.ExecuteUntilBlocked
// are synchronous from the caller's perspective.
type Resonate struct {
	pid      string
	idPrefix string
	ttl      time.Duration

	codec     *Codec
	network   Network
	core      *Core
	registry  *Registry
	heartbeat Heartbeat
	sender    *Sender

	subsMu sync.RWMutex
	subs   map[string]*subscription

	bgCtx    stdctx.Context
	bgCancel stdctx.CancelFunc
	stopOnce sync.Once
	// refreshWG tracks the periodic listener-refresh goroutine. Workflow
	// execution goroutines are intentionally NOT tracked: a registered
	// function may legitimately block indefinitely (e.g. waiting on an
	// external event), and we don't want Stop to hang on it. Stop cancels
	// bgCtx so any IO-bound goroutines exit promptly.
	refreshWG sync.WaitGroup

	log *slog.Logger
}

// subscriptionRefreshInterval is how often the background goroutine re-issues
// PromiseRegisterListener for pending subscriptions, to defend against missed
// pushes (e.g. SSE reconnects).
const subscriptionRefreshInterval = 60 * time.Second

// ──────────────────────────────────────────────────────────────────────────
// Constructors
// ──────────────────────────────────────────────────────────────────────────

// New builds a Resonate instance. Network selection precedence: cfg.URL >
// cfg.Network > RESONATE_URL env variable. When a URL is used, a default
// HTTPNetwork is constructed against it. Defaults: TTL 60s, AsyncHeartbeat at
// TTL/2, NoopEncryptor, no prefix.
func New(cfg Config) (*Resonate, error) {
	network := cfg.Network
	switch {
	case cfg.URL != "":
		network = httpnet.NewHTTP(cfg.URL, httpnet.HTTPOptions{})
	case network == nil:
		if envURL := os.Getenv("RESONATE_URL"); envURL != "" {
			network = httpnet.NewHTTP(envURL, httpnet.HTTPOptions{})
		}
	}
	if network == nil {
		return nil, ErrNetworkRequired
	}

	ttl := cfg.TTL
	if ttl == 0 {
		ttl = 60 * time.Second
	}

	idPrefix := ""
	if cfg.Prefix != "" {
		idPrefix = cfg.Prefix + ":"
	}

	var authPtr *string
	if cfg.Token != "" {
		t := cfg.Token
		authPtr = &t
	}

	sender := NewSender(network, authPtr)
	codec := NewCodec(cfg.Encryptor)
	registry := NewRegistry()

	hb := cfg.Heartbeat
	if hb == nil {
		hbInterval := ttl / 2
		if hbInterval <= 0 {
			hbInterval = 30 * time.Second
		}
		hb = NewAsyncHeartbeat(network.PID(), hbInterval, sender)
	}

	bgCtx, bgCancel := stdctx.WithCancel(stdctx.Background())

	r := &Resonate{
		pid:       network.PID(),
		idPrefix:  idPrefix,
		ttl:       ttl,
		codec:     codec,
		network:   network,
		registry:  registry,
		heartbeat: hb,
		sender:    sender,
		subs:      map[string]*subscription{},
		bgCtx:     bgCtx,
		bgCancel:  bgCancel,
		log:       slog.Default(),
	}

	resolver := func(override *string) string {
		target := ""
		if override != nil {
			target = *override
		}
		return r.resolveTarget(target)
	}
	r.core = NewCore(sender, codec, registry, resolver, hb, network.PID(), r.safeTTLMs())

	// Wire push-message dispatch BEFORE starting the network so we don't miss
	// the initial frames.
	r.installMessageHandler()

	// Start the network. Errors are logged but not fatal — HTTPNetwork
	// reconnects via SSE backoff if the server isn't up yet; LocalNetwork
	// never fails here.
	if err := network.Start(bgCtx); err != nil {
		r.log.Error("network start failed", "err", err)
	}

	// Spawn the periodic listener refresh.
	r.refreshWG.Add(1)
	go r.runRefresh()

	return r, nil
}

// ──────────────────────────────────────────────────────────────────────────
// Public methods
// ──────────────────────────────────────────────────────────────────────────

// RPC dispatches a function remotely. Does not require local registration of
// the target function. Returns a Handle.
func (r *Resonate) RPC(ctx stdctx.Context, id, funcName string, args any, opts ...RPCOptions) (*Handle, error) {
	opt := firstOpt(opts)
	prefixedID := r.prefixID(id)
	req, err := r.buildRootPromiseCreateReq(prefixedID, funcName, args, opt.Timeout, opt.Target, opt.Tags)
	if err != nil {
		return nil, err
	}
	if _, err := r.sender.PromiseCreate(ctx, req); err != nil {
		return nil, err
	}
	return r.handleFromID(ctx, prefixedID)
}

// Get returns a Handle for an existing promise. Returns *ServerError{Code: 404}
// when the promise does not exist.
func (r *Resonate) Get(ctx stdctx.Context, id string) (*Handle, error) {
	return r.handleFromID(ctx, r.prefixID(id))
}

// Stop tears down background goroutines and the network. Idempotent.
func (r *Resonate) Stop() error {
	var stopErr error
	r.stopOnce.Do(func() {
		if err := r.network.Stop(); err != nil {
			stopErr = err
		}
		r.heartbeat.Shutdown()
		r.bgCancel()
		r.refreshWG.Wait()
	})
	return stopErr
}

// ──────────────────────────────────────────────────────────────────────────
// Accessors
// ──────────────────────────────────────────────────────────────────────────

// PID returns the process ID this Resonate identifies as.
func (r *Resonate) PID() string { return r.pid }

// TTL returns the configured TTL.
func (r *Resonate) TTL() time.Duration { return r.ttl }

// IDPrefix returns the configured ID prefix (including trailing ":" when set).
func (r *Resonate) IDPrefix() string { return r.idPrefix }

// Sender returns the internal Sender — useful for tests that need to inspect
// or manipulate server state via raw RPC (no mocks; runs through the same
// Network as the Resonate instance).
func (r *Resonate) Sender() *Sender { return r.sender }

// Network returns the underlying Network — useful for tests that need to
// inspect network identity / addresses.
func (r *Resonate) Network() Network { return r.network }

// ──────────────────────────────────────────────────────────────────────────
// Helpers
// ──────────────────────────────────────────────────────────────────────────

// IsURL reports whether s looks like a URL (i.e. contains "://").
func IsURL(s string) bool { return strings.Contains(s, "://") }

func (r *Resonate) prefixID(id string) string {
	if r.idPrefix == "" {
		return id
	}
	return r.idPrefix + id
}

// resolveTarget returns target unchanged if it's a URL, runs it through the
// network resolver if it's a bare name, and falls back to the network's group
// name when target is empty.
func (r *Resonate) resolveTarget(target string) string {
	if target == "" {
		return r.network.TargetResolver(r.network.Group())
	}
	if IsURL(target) {
		return target
	}
	return r.network.TargetResolver(target)
}

func (r *Resonate) safeTTLMs() int64 {
	ms := r.ttl.Milliseconds()
	if ms <= 0 {
		return int64(1) << 50
	}
	return ms
}

// buildRootPromiseCreateReq builds the PromiseCreateReq used by Run and RPC
// at the top level. Sets the resonate:origin/branch/parent/scope/target tags
// to match Rust's build_root_tags. The param is codec-encoded so the worker
// that acquires the task can decode it via Codec.DecodePromise (the symmetric
// inverse used by Core).
func (r *Resonate) buildRootPromiseCreateReq(prefixedID, funcName string, args any, timeout time.Duration, target string, extraTags map[string]string) (PromiseCreateReq, error) {
	if timeout <= 0 {
		timeout = DefaultTopLevelTimeout
	}
	param, err := r.codec.Encode(map[string]any{"func": funcName, "args": args})
	if err != nil {
		return PromiseCreateReq{}, err
	}
	resolvedTarget := r.resolveTarget(target)
	tags := make(map[string]string, len(extraTags)+5)
	for k, v := range extraTags {
		tags[k] = v
	}
	tags["resonate:origin"] = prefixedID
	tags["resonate:branch"] = prefixedID
	tags["resonate:parent"] = prefixedID
	tags["resonate:scope"] = "global"
	tags["resonate:target"] = resolvedTarget

	return PromiseCreateReq{
		ID:        prefixedID,
		TimeoutAt: nowMs() + timeout.Milliseconds(),
		Param:     param,
		Tags:      tags,
	}, nil
}

// ──────────────────────────────────────────────────────────────────────────
// Subscription / handle path
// ──────────────────────────────────────────────────────────────────────────

// subscribe returns (sub, isNew). When isNew is true, the caller must call
// PromiseRegisterListener for the id to ensure the server pushes Unblock.
func (r *Resonate) subscribe(id string) (*subscription, bool) {
	r.subsMu.RLock()
	if sub, ok := r.subs[id]; ok {
		r.subsMu.RUnlock()
		return sub, false
	}
	r.subsMu.RUnlock()

	r.subsMu.Lock()
	defer r.subsMu.Unlock()
	if sub, ok := r.subs[id]; ok {
		return sub, false
	}
	sub := newSubscription()
	r.subs[id] = sub
	return sub, true
}

// handleFromID subscribes to the id (registering a listener if needed) and
// returns a Handle. If the server reports the promise is already settled, the
// subscription is settled immediately.
func (r *Resonate) handleFromID(ctx stdctx.Context, id string) (*Handle, error) {
	sub, isNew := r.subscribe(id)
	if isNew {
		record, err := r.sender.PromiseRegisterListener(ctx, id, r.network.Unicast())
		if err != nil {
			r.dropSub(id, sub)
			return nil, err
		}
		if record.State != PromiseStatePending {
			r.settleAndCleanup(id, sub, record.State, record.Value)
		}
	}
	return &Handle{id: id, sub: sub, codec: r.codec}, nil
}

// settleAndCleanup settles sub and removes id from the subs map. Holders of
// sub still observe the settled result via their pointer; the map deletion
// just prevents unbounded growth as promises complete.
func (r *Resonate) settleAndCleanup(id string, sub *subscription, state PromiseState, value Value) {
	sub.settle(state, value)
	r.subsMu.Lock()
	if cur, ok := r.subs[id]; ok && cur == sub {
		delete(r.subs, id)
	}
	r.subsMu.Unlock()
}

// dropSub removes an unsettled subscription that we just inserted but failed
// to register a listener for (e.g. 404 on Get) so we don't accumulate stale
// entries on transient errors.
func (r *Resonate) dropSub(id string, sub *subscription) {
	r.subsMu.Lock()
	if cur, ok := r.subs[id]; ok && cur == sub && !sub.settled() {
		delete(r.subs, id)
	}
	r.subsMu.Unlock()
}

// installMessageHandler wires Sender.Recv to dispatch Execute messages to
// Core (each in its own goroutine) and Unblock messages to the subscription
// map (synchronously — fast atomic+close).
func (r *Resonate) installMessageHandler() {
	r.sender.Recv(func(msg Message) {
		switch m := msg.(type) {
		case ExecuteMessage:
			taskID := m.TaskID
			version := m.Version
			go func() {
				if _, err := r.core.OnMessage(r.bgCtx, taskID, version); err != nil {
					r.log.Error("core.OnMessage failed", "task_id", taskID, "err", err)
				}
			}()
		case UnblockMessage:
			r.onUnblock(m)
		}
	})
}

func (r *Resonate) onUnblock(m UnblockMessage) {
	var rec PromiseRecord
	if err := json.Unmarshal(m.Promise, &rec); err != nil {
		r.log.Warn("unblock: failed to parse promise", "err", err)
		return
	}
	if rec.State == PromiseStatePending {
		return
	}
	r.subsMu.RLock()
	sub, ok := r.subs[rec.ID]
	r.subsMu.RUnlock()
	if !ok {
		// No one is waiting on this id (already settled+cleaned, or a duplicate
		// push). A future Get will learn the settled state via PromiseRegisterListener.
		return
	}
	r.settleAndCleanup(rec.ID, sub, rec.State, rec.Value)
}

// runRefresh re-issues PromiseRegisterListener for every still-pending
// subscription every subscriptionRefreshInterval, so a dropped SSE connection
// doesn't strand a Handle. Returns when r.bgCtx is cancelled.
func (r *Resonate) runRefresh() {
	defer r.refreshWG.Done()
	ticker := time.NewTicker(subscriptionRefreshInterval)
	defer ticker.Stop()
	for {
		select {
		case <-r.bgCtx.Done():
			return
		case <-ticker.C:
			r.refreshPending()
		}
	}
}

func (r *Resonate) refreshPending() {
	r.subsMu.RLock()
	if len(r.subs) == 0 {
		r.subsMu.RUnlock()
		return
	}
	type pendingEntry struct {
		id  string
		sub *subscription
	}
	pending := make([]pendingEntry, 0, len(r.subs))
	for id, sub := range r.subs {
		if !sub.settled() {
			pending = append(pending, pendingEntry{id, sub})
		}
	}
	r.subsMu.RUnlock()

	addr := r.network.Unicast()
	for _, p := range pending {
		record, err := r.sender.PromiseRegisterListener(r.bgCtx, p.id, addr)
		if err != nil {
			r.log.Warn("subscription refresh failed", "id", p.id, "err", err)
			continue
		}
		if record.State != PromiseStatePending {
			r.settleAndCleanup(p.id, p.sub, record.State, record.Value)
		}
	}
}

// ──────────────────────────────────────────────────────────────────────────
// Handle
// ──────────────────────────────────────────────────────────────────────────

// Handle is a top-level reference to a running or settled durable promise.
// It is created by Run, RPC, and Get. Multiple Handles can refer to the same
// id; all of them resolve when the underlying promise settles.
//
// Handle is intentionally distinct from Future: Future is the in-workflow
// primitive that panics suspendSignal{} to unwind a workflow when its result
// isn't yet known. Handle lives outside any workflow and simply blocks a
// goroutine on settlement.
type Handle struct {
	id    string
	sub   *subscription
	codec *Codec
}

// ID returns the (prefix-prepended) promise id this handle refers to.
func (h *Handle) ID() string { return h.id }

// Result blocks until the underlying promise settles or ctx is cancelled, then
// decodes the settled value into out. out must be a non-nil pointer (or nil
// when the workflow returns a unit-equivalent value). A rejected promise
// returns the application error embedded in the rejection payload.
func (h *Handle) Result(ctx stdctx.Context, out any) error {
	select {
	case <-h.sub.done:
	case <-ctx.Done():
		return ctx.Err()
	}
	res := h.sub.result.Load()
	if res == nil {
		return &DecodingError{Msg: "subscription closed without a settled result"}
	}
	return h.decode(res, out)
}

func (h *Handle) decode(res *promiseResult, out any) error {
	switch res.state {
	case PromiseStateResolved:
		if out == nil {
			return nil
		}
		if _, err := h.codec.Decode(res.value, out); err != nil {
			return err
		}
		return nil
	case PromiseStateRejected, PromiseStateRejectedCanceled, PromiseStateRejectedTimedout:
		var inner json.RawMessage
		ok, err := h.codec.Decode(res.value, &inner)
		if err != nil {
			return err
		}
		if !ok || len(inner) == 0 {
			return &ApplicationError{Message: fmt.Sprintf("promise %s rejected with no payload", h.id)}
		}
		return DeserializeError(inner)
	default:
		return fmt.Errorf("resonate: handle %s has unexpected state %q", h.id, res.state)
	}
}

// ResultOf is the type-safe convenience wrapper around Handle.Result. It
// blocks until h settles and returns its decoded value as T.
func ResultOf[T any](ctx stdctx.Context, h *Handle) (T, error) {
	var v T
	err := h.Result(ctx, &v)
	return v, err
}

// ──────────────────────────────────────────────────────────────────────────
// Typed registration and Run
// ──────────────────────────────────────────────────────────────────────────

// RegisteredFunc is a typed reference to a function previously installed via
// Register. Call Run to start an invocation; the returned TypedHandle carries
// the function's result type so callers don't restate it at the call site.
type RegisteredFunc[A, R any] struct {
	r    *Resonate
	name string
}

// Register installs fn in r's registry under name and returns a typed handle.
// The function must have signature func(*Context, A) (R, error); use struct{}
// for A when the function takes no arguments. Returns *AlreadyRegisteredError
// if name is already taken.
func Register[A, R any](r *Resonate, name string, fn func(*Context, A) (R, error)) (*RegisteredFunc[A, R], error) {
	if err := r.registry.Register(name, fn); err != nil {
		return nil, err
	}
	return &RegisteredFunc[A, R]{r: r, name: name}, nil
}

// Run starts a durable invocation of rf locally. Returns a TypedHandle whose
// Result method yields R. Calling Run twice with the same id returns a handle
// to the existing promise.
func (rf *RegisteredFunc[A, R]) Run(ctx stdctx.Context, id string, args A, opts ...RunOptions) (*TypedHandle[R], error) {
	r := rf.r
	if _, ok := r.registry.Get(rf.name); !ok {
		return nil, &FunctionNotFoundError{Name: rf.name}
	}
	opt := firstOpt(opts)

	prefixedID := r.prefixID(id)
	req, err := r.buildRootPromiseCreateReq(prefixedID, rf.name, args, opt.Timeout, opt.Target, opt.Tags)
	if err != nil {
		return nil, err
	}

	ttlMs := r.safeTTLMs()
	res, err := r.sender.TaskCreate(ctx, r.pid, ttlMs, req)
	if err != nil {
		return nil, err
	}

	switch {
	case res.Conflict:
		// Existing promise; just subscribe.
	case res.Created != nil:
		created := res.Created
		if created.Task.State == TaskStateAcquired {
			// We won the create-and-acquire race; we're responsible for running it.
			decoded, derr := r.codec.DecodePromise(created.Promise)
			if derr != nil {
				return nil, derr
			}
			taskID := created.Task.ID
			version := created.Task.Version
			preload := created.Preload
			go func() {
				if _, e := r.core.ExecuteUntilBlocked(r.bgCtx, taskID, version, decoded, preload); e != nil {
					r.log.Error("ExecuteUntilBlocked failed", "task_id", taskID, "err", e)
				}
			}()
		}
	default:
		return nil, &DecodingError{Msg: "task.create returned neither Created nor Conflict"}
	}

	h, err := r.handleFromID(ctx, prefixedID)
	if err != nil {
		return nil, err
	}
	return &TypedHandle[R]{h: h}, nil
}

// TypedHandle wraps Handle with a static result type. It has the same id and
// blocking semantics as Handle; Result returns R directly.
type TypedHandle[R any] struct {
	h *Handle
}

// ID returns the (prefix-prepended) promise id this handle refers to.
func (th *TypedHandle[R]) ID() string { return th.h.ID() }

// Result blocks until the underlying promise settles or ctx is cancelled, then
// returns the decoded R. A rejected promise produces the application error.
func (th *TypedHandle[R]) Result(ctx stdctx.Context) (R, error) {
	return ResultOf[R](ctx, th.h)
}

// Untyped returns the underlying untyped *Handle for interop with code that
// expects *Handle (e.g., mixing typed handles with Resonate.Get results in a
// slice).
func (th *TypedHandle[R]) Untyped() *Handle { return th.h }
