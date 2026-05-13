// Package localnet provides an in-process implementation of the
// resonate.Network interface. It runs the server state machine in a single
// actor goroutine — useful for tests and "no-server-required" local
// development.
package localnet

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	resonate "github.com/resonatehq/resonate-sdk-go"
)

// LocalNetwork is an in-process Network implementation that runs the entire
// server state machine in a single actor goroutine. It exists for tests and
// for "no-server-required" local development.
//
// Concurrency model: every request, tick, and subscribe operation flows
// through the actor's inbound channel as a stateOp. The actor owns
// serverState entirely; no mutex is needed inside the state machine. Push
// messages produced as side-effects of an apply are fanned out to subscribers
// in a separate goroutine after the reply is sent, so the actor stays
// responsive.
type LocalNetwork struct {
	pid     string
	group   string
	unicast string
	anycast string

	in      chan stateOp
	started bool
	cancel  context.CancelFunc
	wg      sync.WaitGroup
	mu      sync.Mutex // guards started/cancel
}

// stateOp is the unit of work submitted to the actor goroutine.
type stateOp struct {
	kind  opKind
	now   int64
	req   map[string]any
	cb    func(string)
	reply chan opReply
}

type opKind uint8

const (
	opApply opKind = iota
	opTick
	opSubscribe
)

type opReply struct {
	resp     map[string]any
	outgoing []outgoingMsg
	err      error
}

// NewLocal builds a LocalNetwork. Pass nil for pid to auto-generate one;
// group defaults to "default".
func NewLocal(group string, pid *string) *LocalNetwork {
	if group == "" {
		group = "default"
	}
	pidStr := ""
	if pid != nil {
		pidStr = *pid
	}
	if pidStr == "" {
		pidStr = randomPID()
	}
	return &LocalNetwork{
		pid:     pidStr,
		group:   group,
		unicast: fmt.Sprintf("local://uni@%s/%s", group, pidStr),
		anycast: fmt.Sprintf("local://any@%s/%s", group, pidStr),
		in:      make(chan stateOp, 64),
	}
}

func (l *LocalNetwork) PID() string     { return l.pid }
func (l *LocalNetwork) Group() string   { return l.group }
func (l *LocalNetwork) Unicast() string { return l.unicast }
func (l *LocalNetwork) Anycast() string { return l.anycast }

func (l *LocalNetwork) TargetResolver(target string) string {
	return "local://any@" + target
}

// Start spawns the actor goroutine and the ticker goroutine. The supplied
// context governs the lifetime of both.
func (l *LocalNetwork) Start(ctx context.Context) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.started {
		return nil
	}
	runCtx, cancel := context.WithCancel(ctx)
	l.cancel = cancel
	l.started = true

	subscribers := &subscriberList{}

	l.wg.Add(1)
	go func() {
		defer l.wg.Done()
		l.runActor(runCtx, subscribers)
	}()
	l.wg.Add(1)
	go func() {
		defer l.wg.Done()
		l.runTicker(runCtx)
	}()
	return nil
}

// Stop tears down the actor and ticker goroutines and waits for them to exit.
func (l *LocalNetwork) Stop() error {
	l.mu.Lock()
	if !l.started {
		l.mu.Unlock()
		return nil
	}
	l.started = false
	cancel := l.cancel
	l.cancel = nil
	l.mu.Unlock()
	cancel()
	l.wg.Wait()
	return nil
}

// Send issues a request and blocks until the actor produces a reply. The
// outgoing push messages (if any) are fanned out asynchronously after the
// reply is built.
func (l *LocalNetwork) Send(ctx context.Context, req string) (string, error) {
	var raw any
	if err := json.Unmarshal([]byte(req), &raw); err != nil {
		return "", &resonate.DecodingError{Msg: "invalid JSON request: " + err.Error()}
	}
	rawMap, ok := raw.(map[string]any)
	if !ok {
		return "", &resonate.DecodingError{Msg: "request must be a JSON object"}
	}
	flat := unwrapRequestEnvelope(rawMap)

	reply := make(chan opReply, 1)
	op := stateOp{kind: opApply, now: time.Now().UnixMilli(), req: flat, reply: reply}

	if err := l.submit(ctx, op); err != nil {
		return "", err
	}
	select {
	case <-ctx.Done():
		return "", ctx.Err()
	case r := <-reply:
		if r.err != nil {
			return "", r.err
		}
		env := wrapResponseEnvelope(r.resp)
		body, err := json.Marshal(env)
		if err != nil {
			return "", err
		}
		return string(body), nil
	}
}

// Recv registers a callback that receives every push frame from the actor.
// Registration is serialized through the actor itself.
func (l *LocalNetwork) Recv(cb func(raw string)) {
	op := stateOp{kind: opSubscribe, cb: cb}
	// Best-effort, non-blocking. Recv is fire-and-forget.
	_ = l.submit(context.Background(), op)
}

func (l *LocalNetwork) submit(ctx context.Context, op stateOp) error {
	select {
	case l.in <- op:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// runActor is the single goroutine that owns serverState.
func (l *LocalNetwork) runActor(ctx context.Context, subs *subscriberList) {
	state := newServerState()
	for {
		select {
		case <-ctx.Done():
			return
		case op := <-l.in:
			l.handle(state, subs, op)
		}
	}
}

func (l *LocalNetwork) handle(state *serverState, subs *subscriberList, op stateOp) {
	defer func() {
		if r := recover(); r != nil {
			if op.kind == opApply && op.reply != nil {
				op.reply <- opReply{err: fmt.Errorf("local actor panic: %v", r)}
			}
		}
	}()

	switch op.kind {
	case opApply:
		resp, outgoing, err := state.apply(op.now, op.req)
		op.reply <- opReply{resp: resp, outgoing: outgoing, err: err}
		if len(outgoing) > 0 {
			go dispatchOutgoing(subs, outgoing)
		}
	case opTick:
		outgoing := state.tick(op.now)
		if len(outgoing) > 0 {
			go dispatchOutgoing(subs, outgoing)
		}
	case opSubscribe:
		subs.add(op.cb)
	}
}

func (l *LocalNetwork) runTicker(ctx context.Context) {
	t := time.NewTicker(time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			select {
			case l.in <- stateOp{kind: opTick, now: time.Now().UnixMilli()}:
			case <-ctx.Done():
				return
			}
		}
	}
}

// subscriberList is the actor-owned callback registry. Reads from
// dispatchOutgoing happen on a separate goroutine, so the list itself uses a
// small RWMutex — the actor only ever appends.
type subscriberList struct {
	mu sync.RWMutex
	cb []func(string)
}

func (s *subscriberList) add(cb func(string)) {
	if cb == nil {
		return
	}
	s.mu.Lock()
	s.cb = append(s.cb, cb)
	s.mu.Unlock()
}

func (s *subscriberList) snapshot() []func(string) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]func(string), len(s.cb))
	copy(out, s.cb)
	return out
}

func dispatchOutgoing(subs *subscriberList, msgs []outgoingMsg) {
	cbs := subs.snapshot()
	if len(cbs) == 0 {
		return
	}
	for _, m := range msgs {
		body, err := json.Marshal(m.message)
		if err != nil {
			continue
		}
		raw := string(body)
		for _, cb := range cbs {
			cb(raw)
		}
	}
}

func randomPID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		// Fall back to a timestamp-derived id.
		return fmt.Sprintf("%016x", time.Now().UnixMilli())
	}
	return hex.EncodeToString(b[:])
}

// ──────────────────────────────────────────────────────────────────────────
// Protocol envelope helpers (local transport only)
// ──────────────────────────────────────────────────────────────────────────

// unwrapRequestEnvelope flattens a protocol envelope `{kind, head, data}`
// into a single map by lifting kind and head.corrId next to the data fields.
// If the request is already flat, it is returned unchanged.
func unwrapRequestEnvelope(req map[string]any) map[string]any {
	headRaw, hasHead := req["head"]
	dataRaw, hasData := req["data"]
	if !hasHead || !hasData {
		return req
	}
	flat := map[string]any{}
	if dataObj, ok := dataRaw.(map[string]any); ok {
		for k, v := range dataObj {
			flat[k] = v
		}
	}
	if kind, ok := req["kind"]; ok {
		flat["kind"] = kind
	}
	if headObj, ok := headRaw.(map[string]any); ok {
		if corr, ok := headObj["corrId"]; ok {
			flat["corrId"] = corr
		}
	}
	return flat
}

// wrapResponseEnvelope wraps a flat response from the server state machine
// into the protocol envelope shape sent over the wire.
func wrapResponseEnvelope(flat map[string]any) map[string]any {
	kind := flat["kind"]
	corrID := flat["corrId"]
	status := 200
	if s, ok := flat["status"].(int); ok {
		status = s
	}
	data := map[string]any{}
	for k, v := range flat {
		switch k {
		case "kind", "corrId", "status":
			continue
		default:
			data[k] = v
		}
	}
	return map[string]any{
		"kind": kind,
		"head": map[string]any{
			"corrId":  corrID,
			"status":  status,
			"version": resonate.ProtocolVersion,
		},
		"data": data,
	}
}

// extractActionData returns the data portion of a value that may be a
// sub-envelope `{kind, head, data}` or already a bare data object.
func extractActionData(v any) any {
	obj, ok := v.(map[string]any)
	if !ok {
		return v
	}
	_, hasKind := obj["kind"]
	data, hasData := obj["data"]
	if hasKind && hasData {
		return data
	}
	return v
}
