package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	resonate "github.com/resonatehq/resonate-sdk-go"
	"github.com/resonatehq/resonate-sdk-go/httpnet"
)

// Recording every request and response.
//
// `resonate.Config.Network` takes any `Network`, and every call the SDK
// makes to the server goes through exactly one method:
//
//	Send(ctx context.Context, body string) (string, error)
//
// So a decorator around `httpnet.HTTPNetwork` sees the whole conversation
// — no proxy, no packet capture, and no risk of recording something the
// SDK did not actually send. That is the tap point.
//
// The recorded shape is the one `valid/traces` already uses, so the output
// feeds `lake exe checktrace` and `porcupine/cmd/lincheck` unmodified:
//
//	{"kind":…,"now":…,"req":…,"res":…}
//
// plus `client`, `call` and `return` (nanoseconds), which the NDJSON
// consumers ignore and the porcupine history consumer needs.

// Recorder collects wire events from every client.
type Recorder struct {
	mu      sync.Mutex
	events  []Event
	skipped map[string]int

	// clock hands out the `resonate:debug_time` values. The Lean checker
	// needs a monotone clock and concurrent clients have no single instant
	// each, so requests share instants in batches — ties are legal, the
	// specification says non-decreasing.
	now       atomic.Uint64
	batch     atomic.Uint64
	batchSize uint64
	debug     bool
}

// Event is one request/response pair as it went over the wire.
type Event struct {
	Kind   string          `json:"kind"`
	Now    uint64          `json:"now"`
	Req    json.RawMessage `json:"req"`
	Res    json.RawMessage `json:"res"`
	Client string          `json:"client"`
	Call   int64           `json:"call"`
	Return int64           `json:"return"`
}

func NewRecorder(startAt, batchSize uint64, debug bool) *Recorder {
	r := &Recorder{batchSize: batchSize, debug: debug}
	r.now.Store(startAt)
	return r
}

// tick returns the instant the next request should carry. Every
// `batchSize` requests the clock advances, so requests within a batch
// share an instant and can legitimately overlap.
func (r *Recorder) tick() uint64 {
	n := r.batch.Add(1)
	if n%r.batchSize == 0 {
		return r.now.Add(uint64(10 + n%37))
	}
	return r.now.Load()
}

func (r *Recorder) add(e Event) {
	r.mu.Lock()
	r.events = append(r.events, e)
	r.mu.Unlock()
}

// skip counts wire kinds the checkers cannot read, so a run reports what
// it DROPPED rather than silently producing a partial trace.
func (r *Recorder) skip(kind string) {
	r.mu.Lock()
	if r.skipped == nil {
		r.skipped = map[string]int{}
	}
	r.skipped[kind]++
	r.mu.Unlock()
}

func (r *Recorder) Skipped() map[string]int {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := map[string]int{}
	for k, v := range r.skipped {
		out[k] = v
	}
	return out
}

func (r *Recorder) Len() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.events)
}

// Network wraps a transport and records everything crossing it.
func (r *Recorder) Network(url, client string) resonate.Network {
	return &recordingNet{
		inner:  httpnet.NewHTTP(url, httpnet.HTTPOptions{}),
		rec:    r,
		client: client,
	}
}

type recordingNet struct {
	inner  *httpnet.HTTPNetwork
	rec    *Recorder
	client string
}

func (n *recordingNet) PID() string     { return n.inner.PID() }
func (n *recordingNet) Group() string   { return n.inner.Group() }
func (n *recordingNet) Unicast() string { return n.inner.Unicast() }
func (n *recordingNet) Anycast() string { return n.inner.Anycast() }
func (n *recordingNet) TargetResolver(t string) string {
	return n.inner.TargetResolver(t)
}
func (n *recordingNet) Start(ctx context.Context) error { return n.inner.Start(ctx) }
func (n *recordingNet) Stop() error                     { return n.inner.Stop() }
func (n *recordingNet) Recv(cb func(raw string))        { n.inner.Recv(cb) }

// Send is the whole recorder. The SDK builds the envelope; this stamps a
// debug instant into its head so the trace has a monotone clock, then tees
// the pair.
func (n *recordingNet) Send(ctx context.Context, body string) (string, error) {
	now := n.rec.tick()
	kind, stamped := stampDebugTime(body, now, n.rec.debug)

	call := time.Now().UnixNano()
	res, err := n.inner.Send(ctx, stamped)
	ret := time.Now().UnixNano()
	if err != nil {
		return res, err
	}

	// SCENARIOS_TRACE_KINDS prints every kind the SDK sends, recorded or
	// not. This is how the `task.fence` gap below was found: the run
	// succeeded, the trace looked thin, and only the raw kinds said why.
	if os.Getenv("SCENARIOS_TRACE_KINDS") != "" {
		fmt.Fprintf(os.Stderr, "[kind] %s\n", kind)
	}
	// The checkers understand protocol calls, not the SDK's transport
	// chatter. Anything else is dropped rather than recorded as an event
	// neither checker can decode.
	if !recordable(kind) {
		n.rec.skip(kind)
		return res, nil
	}
	req, ok := dataOf(stamped)
	if !ok {
		return res, nil
	}
	n.rec.add(Event{
		Kind: kind, Now: now, Req: req, Res: json.RawMessage(res),
		Client: n.client, Call: call, Return: ret,
	})
	return res, nil
}

// recordable is the set of kinds BOTH checkers can decode.
//
// `task.fence` is in it. The SDK issues one per fenced create/settle — 24
// of 36 events in a small fan-out run — and it used to be dropped, which
// made every trace from this binary a partial view. It is now decoded by
// `valid/json.lean` and modelled by `porcupine`'s `TaskFence`, both
// transcribed from `spec/02-abstract/p.lean:229`.
//
// `task.create` is in it too, for the same reason: the SDK issues one per
// root workflow, so without it the trace has no promise for anything else
// to refer to, and the very first event refutes.
//
// Schedules are excluded on purpose rather than by omission:
// `occurrences` is opaque in the specification, so neither checker judges
// them.
func recordable(kind string) bool {
	switch kind {
	case "promise.create", "promise.get", "promise.settle",
		"promise.register_callback", "promise.register_listener",
		"task.get", "task.acquire", "task.suspend", "task.fulfill",
		"task.create", "task.fence", "task.release", "task.heartbeat":
		return true
	}
	return false
}

// stampDebugTime injects `resonate:debug_time` into the envelope head and
// returns the kind. Without it the server uses wall clock, and a trace
// whose instants are 1.7e12 while promises carry small deadlines is not
// checkable — that mismatch is what produced the one REFUTED capture in
// valid/traces.
func stampDebugTime(body string, now uint64, debug bool) (string, string) {
	var env map[string]json.RawMessage
	if err := json.Unmarshal([]byte(body), &env); err != nil {
		return "", body
	}
	var kind string
	_ = json.Unmarshal(env["kind"], &kind)
	if !debug {
		return kind, body
	}
	var head map[string]any
	if raw, ok := env["head"]; ok {
		_ = json.Unmarshal(raw, &head)
	}
	if head == nil {
		head = map[string]any{}
	}
	head["resonate:debug_time"] = now
	h, err := json.Marshal(head)
	if err != nil {
		return kind, body
	}
	env["head"] = h
	out, err := json.Marshal(env)
	if err != nil {
		return kind, body
	}
	return kind, string(out)
}

func dataOf(body string) (json.RawMessage, bool) {
	var env map[string]json.RawMessage
	if err := json.Unmarshal([]byte(body), &env); err != nil {
		return nil, false
	}
	d, ok := env["data"]
	return d, ok
}

// ---------------------------------------------------------------- output

// WriteNDJSON emits the trace both checkers read, ordered by (instant,
// return time). Return order respects the real-time partial order, so it
// is a legal linearization candidate — one of possibly many, which is
// exactly why the porcupine history is written too.
func (r *Recorder) WriteNDJSON(path string) error {
	events := r.sorted()
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	for _, e := range events {
		line, err := json.Marshal(map[string]any{
			"kind": e.Kind, "now": e.Now, "req": e.Req, "res": e.Res})
		if err != nil {
			return err
		}
		fmt.Fprintln(f, string(line))
	}
	return nil
}

// WriteHistory emits call/return intervals for porcupine, which searches
// every order consistent with them rather than checking the one above.
func (r *Recorder) WriteHistory(path string) error {
	events := r.sorted()
	ids := map[string]int{}
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	for _, e := range events {
		cid, ok := ids[e.Client]
		if !ok {
			cid = len(ids)
			ids[e.Client] = cid
		}
		line, err := json.Marshal(map[string]any{
			"kind": e.Kind, "now": e.Now, "req": e.Req, "res": e.Res,
			"call": e.Call, "return": e.Return, "client": cid})
		if err != nil {
			return err
		}
		fmt.Fprintln(f, string(line))
	}
	return nil
}

func (r *Recorder) sorted() []Event {
	r.mu.Lock()
	out := append([]Event(nil), r.events...)
	r.mu.Unlock()
	for i := 1; i < len(out); i++ {
		for j := i; j > 0; j-- {
			a, b := out[j-1], out[j]
			if a.Now < b.Now || (a.Now == b.Now && a.Return <= b.Return) {
				break
			}
			out[j-1], out[j] = b, a
		}
	}
	return out
}

// Overlaps reports whether the run was ACTUALLY concurrent. A harness that
// means to produce overlap and does not is the easiest way to run a
// concurrency experiment that proves nothing, so it is measured.
func (r *Recorder) Overlaps() (pairs, max int) {
	e := r.sorted()
	for i := range e {
		live := 1
		for j := range e {
			if i == j {
				continue
			}
			if e[i].Call < e[j].Return && e[j].Call < e[i].Return {
				if j > i {
					pairs++
				}
				live++
			}
		}
		if live > max {
			max = live
		}
	}
	return pairs, max
}

// Summary counts kinds and statuses, so a run reports what it exercised
// rather than only how big it was.
func (r *Recorder) Summary() (kinds map[string]int, statuses map[int]int) {
	kinds, statuses = map[string]int{}, map[int]int{}
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, e := range r.events {
		kinds[e.Kind]++
		var res struct {
			Head struct {
				Status int `json:"status"`
			} `json:"head"`
		}
		if err := json.Unmarshal(e.Res, &res); err == nil {
			statuses[res.Head.Status]++
		}
	}
	return kinds, statuses
}

func sortedKinds(m map[string]int) string {
	var ks []string
	for k := range m {
		ks = append(ks, k)
	}
	for i := 1; i < len(ks); i++ {
		for j := i; j > 0 && ks[j] < ks[j-1]; j-- {
			ks[j], ks[j-1] = ks[j-1], ks[j]
		}
	}
	var b strings.Builder
	for _, k := range ks {
		fmt.Fprintf(&b, "%s=%d ", k, m[k])
	}
	return strings.TrimSpace(b.String())
}
