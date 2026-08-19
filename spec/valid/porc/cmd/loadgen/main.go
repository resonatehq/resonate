// Command loadgen drives a real resonate server with CONCURRENT random
// traffic and records a history both checkers can read.
//
//	resonate serve &
//	go run ./cmd/loadgen -clients 8 -ops 400 -out run
//
// It writes two files, and the difference between them is the point:
//
//	run.ndjson    the events in RETURN order — one candidate linearization,
//	              which is what the Lean checker in valid/ can consume,
//	              because its `Valid` is a SEQUENCE of observations.
//	run.history   call/return nanosecond intervals per operation, which is
//	              what porcupine consumes. Overlapping operations are left
//	              overlapping, so porcupine must SEARCH for an order.
//
// ## Why the clock is the hard part
//
// The Lean checker requires `(tr t).now ≤ (tr (t+1)).now` — a monotone
// clock — and a real concurrent run has no single instant per operation.
// So every request carries an explicit `resonate:debug_time` drawn from a
// counter that only advances, and concurrent requests within a batch SHARE
// an instant. Ties are legal: `ValidM` says non-decreasing, not
// increasing.
//
// That still leaves the ORDER within a tie to the harness. Ordering by
// return time respects the real-time partial order (if A returned before B
// was called, A precedes B), so it is a legal linearization candidate —
// but it is only ONE, and the server may have picked another. A refutation
// from the Lean checker on a concurrent run therefore means "this
// particular order does not work", not "no order does". Porcupine answers
// the second question. Comparing them is the experiment.
package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

type event struct {
	Kind string          `json:"kind"`
	Now  uint64          `json:"now"`
	Req  json.RawMessage `json:"req"`
	Res  json.RawMessage `json:"res"`
	// not part of the wire format; used to build the porcupine history
	Call   int64 `json:"-"`
	Return int64 `json:"-"`
	Client int   `json:"-"`
}

type historyRow struct {
	Kind   string          `json:"kind"`
	Now    uint64          `json:"now"`
	Req    json.RawMessage `json:"req"`
	Res    json.RawMessage `json:"res"`
	Call   int64           `json:"call"`
	Return int64           `json:"return"`
	Client int             `json:"client"`
}

var (
	url     = flag.String("url", "http://127.0.0.1:8001/", "resonate address")
	clients = flag.Int("clients", 8, "concurrent clients")
	nops    = flag.Int("ops", 400, "total operations")
	out     = flag.String("out", "run", "output prefix")
	seed    = flag.Int64("seed", 1, "rng seed")
	batch   = flag.Int("batch", 4, "operations sharing one debug instant")
	// Deadlines relative to the debug clock, because absolute ones do not fire.
	//
	// The generator used to hard-code `timeoutAt: 900000` and `ttl: 60000`
	// while the debug clock reaches ~30k over 4000 ops — so R1 promise_timeout
	// and R5 lease_expiry NEVER fired, and with them T task_fulfil and R3
	// notify, which need a settled promise to couple to. Five of six rules
	// silently unexercised while the run reported clean.
	//
	// Defaults preserve the old behaviour; set them inside the clock's range to
	// exercise the time-driven half.
	horizon = flag.Uint64("horizon", 900000, "promise timeoutAt, relative to the debug instant")
	lease   = flag.Uint64("lease", 60000, "task ttl")
)

// clock hands out debug instants. Operations in the same batch share one,
// which is what makes concurrency expressible under a monotone clock.
type clock struct {
	mu  sync.Mutex
	now uint64
	n   int
}

func (c *clock) next(batchSize int) uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.n++
	if c.n%batchSize == 0 {
		c.now += uint64(10 + rand.Intn(40))
	}
	return c.now
}

func main() {
	flag.Parse()
	rand.Seed(*seed)

	var hello json.RawMessage
	if err := post(*url, "debug.start", map[string]any{}, &hello); err != nil {
		fmt.Fprintln(os.Stderr, "loadgen: debug.start failed:", err)
		fmt.Fprintln(os.Stderr, "         start the server with RESONATE_DEBUG=true")
		os.Exit(2)
	}
	_ = post(*url, "debug.reset", map[string]any{}, nil)

	// **The handshake is where you learn the clock.** A server that keeps its
	// own time answers `debug.start` with the instant it is at, and the
	// generator starts its counter there.
	//
	// Without this the counter starts at 1000 and every deadline is computed
	// as `1000 + horizon` — a moment in 1970. Against a server with a real
	// clock (epoch milliseconds, or a VM's RTC) every promise is already past
	// its `timeoutAt` when it arrives, so the whole run collapses into R1 and
	// no other rule is ever reached. The failure is silent: the trace is a
	// perfectly linearizable wall of timeouts.
	//
	// Zero, or an absent field, means the server takes our instants instead —
	// then 1000 is right and nothing changes.
	base := serverNow(hello, 0)
	if base == 0 {
		base = 1000
	}
	clk := &clock{now: base}
	var (
		mu     sync.Mutex
		events []event
		errs   int64
		seq    int64
	)

	var wg sync.WaitGroup
	for c := 0; c < *clients; c++ {
		wg.Add(1)
		go func(cid int) {
			defer wg.Done()
			r := rand.New(rand.NewSource(*seed*1000 + int64(cid)))
			for {
				i := atomic.AddInt64(&seq, 1)
				if int(i) > *nops {
					return
				}
				// Each client owns its own origin, so awaits-edges stay
				// inside one partition — the property the checkers'
				// partitioning depends on, and the reason clients do not
				// share objects.
				op, kind := nextOp(r, cid, int(i))
				now := clk.next(*batch)
				if _, ok := op["timeoutAt"]; ok {
					op["timeoutAt"] = now + *horizon
				}
				if _, ok := op["ttl"]; ok {
					op["ttl"] = *lease
				}
				op["__now"] = now

				call := time.Now().UnixNano()
				var res json.RawMessage
				err := post(*url, kind, op, &res)
				ret := time.Now().UnixNano()
				if err != nil {
					atomic.AddInt64(&errs, 1)
					continue
				}
				delete(op, "__now")
				reqJSON, _ := json.Marshal(op)
				mu.Lock()
				// Record the instant the SERVER stamped, not the one we asked
				// for. A server that picks its own time must say which time it
				// picked, and a harness that records its own guess instead is
				// writing fiction: the checker then replays against a `now` the
				// server never used and refutes a correct implementation. We
				// fall back to our hint for a server that does not echo.
				events = append(events, event{Kind: kind, Now: serverNow(res, now), Req: reqJSON,
					Res: res, Call: call, Return: ret, Client: cid})
				mu.Unlock()
			}
		}(c)
	}
	wg.Wait()

	// RETURN order: a legal linearization candidate, because it respects
	// the real-time partial order. Ties broken by call time, then by the
	// debug instant, so the sequence the Lean checker sees has a
	// non-decreasing clock.
	sort.SliceStable(events, func(i, j int) bool {
		if events[i].Now != events[j].Now {
			return events[i].Now < events[j].Now
		}
		return events[i].Return < events[j].Return
	})

	writeNDJSON(*out+".ndjson", events)
	writeHistory(*out+".history", events)

	overlap := countOverlaps(events)
	fmt.Printf("%d events (%d transport errors) from %d clients\n", len(events), errs, *clients)
	fmt.Printf("  overlapping pairs: %d   max concurrency: %d\n", overlap.pairs, overlap.max)
	fmt.Printf("  wrote %s.ndjson (return order, for the Lean checker)\n", *out)
	fmt.Printf("  wrote %s.history (real intervals, for porcupine)\n", *out)
	if overlap.max < 2 {
		fmt.Println("  WARNING: nothing actually overlapped — this is a sequential run")
	}
}

// nextOp draws one request. Each client walks its own workflow so the
// traffic is well-formed often enough to be interesting, with noise on top
// to reach the error paths.
func nextOp(r *rand.Rand, cid, i int) (map[string]any, string) {
	origin := fmt.Sprintf("c%d", cid)
	wf := i / 6
	a := fmt.Sprintf("%s.a%d", origin, wf)
	x := fmt.Sprintf("%s.x%d", origin, wf)
	ext := map[string]string{"resonate:external": "true", "resonate:origin": origin}
	tgt := map[string]string{"resonate:target": "poll://any@w1", "resonate:origin": origin}

	switch r.Intn(12) {
	case 0:
		return map[string]any{"id": a, "timeoutAt": 900000, "param": map[string]any{}, "tags": ext}, "promise.create"
	case 1:
		return map[string]any{"id": x, "timeoutAt": 900000, "param": map[string]any{}, "tags": tgt}, "promise.create"
	case 2, 3:
		return map[string]any{"id": x}, "task.get"
	case 4:
		return map[string]any{"id": x, "version": r.Intn(3), "pid": fmt.Sprintf("p%d", cid), "ttl": 60000}, "task.acquire"
	case 5:
		return map[string]any{"id": x, "version": r.Intn(3), "actions": []any{
			map[string]any{"kind": "promise.register_callback", "head": map[string]any{},
				"data": map[string]any{"awaited": a, "awaiter": x}}}}, "task.suspend"
	case 6:
		return map[string]any{"id": a, "state": "resolved", "value": map[string]any{}}, "promise.settle"
	case 7:
		return map[string]any{"id": x, "version": r.Intn(4), "action": map[string]any{
			"kind": "promise.settle", "head": map[string]any{},
			"data": map[string]any{"id": x, "state": "resolved", "value": map[string]any{}}}}, "task.fulfill"
	case 8, 9:
		id := a
		if r.Intn(2) == 0 {
			id = x
		}
		return map[string]any{"id": id}, "promise.get"
	case 10:
		return map[string]any{"id": x, "version": r.Intn(4)}, "task.release"
	default:
		return map[string]any{"awaited": a, "address": "poll://any@w1"}, "promise.register_listener"
	}
}

// serverNow reads the instant the server stamped out of the response head.
//
// The client's `resonate:debug_time` is a HINT: concurrent clients race, so
// the higher hint can arrive first and a server that used hints verbatim would
// see time go backwards. A server that takes `max(last+1, hint)` under its own
// lock has instants that are strictly increasing in the order it served
// requests — which is the linearization order, and exactly what the sort below
// wants. `fallback` keeps this working against a server that does not echo.
func serverNow(res json.RawMessage, fallback uint64) uint64 {
	var env struct {
		Head map[string]any `json:"head"`
	}
	if json.Unmarshal(res, &env) != nil {
		return fallback
	}
	if n, ok := env.Head["resonate:debug_time"].(float64); ok {
		return uint64(n)
	}
	return fallback
}

func post(url, kind string, data map[string]any, out *json.RawMessage) error {
	head := map[string]any{"corrId": "c", "version": "2026-04-01"}
	if n, ok := data["__now"]; ok {
		head["resonate:debug_time"] = n
		delete(data, "__now")
	}
	body, _ := json.Marshal(map[string]any{"kind": kind, "head": head, "data": data})
	resp, err := http.Post(url, "application/json", bytes.NewReader(body))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	if out != nil {
		*out = json.RawMessage(raw)
	}
	return nil
}

func writeNDJSON(path string, events []event) {
	f, err := os.Create(path)
	if err != nil {
		panic(err)
	}
	defer f.Close()
	for _, e := range events {
		line, _ := json.Marshal(map[string]any{
			"kind": e.Kind, "now": e.Now, "req": e.Req, "res": e.Res})
		fmt.Fprintln(f, string(line))
	}
}

func writeHistory(path string, events []event) {
	f, err := os.Create(path)
	if err != nil {
		panic(err)
	}
	defer f.Close()
	for _, e := range events {
		line, _ := json.Marshal(historyRow{Kind: e.Kind, Now: e.Now, Req: e.Req,
			Res: e.Res, Call: e.Call, Return: e.Return, Client: e.Client})
		fmt.Fprintln(f, string(line))
	}
}

type overlapStats struct{ pairs, max int }

// countOverlaps reports whether the run was ACTUALLY concurrent. A
// harness that intends to produce overlap and does not is the easiest way
// to run a concurrency experiment that proves nothing, so it is measured
// rather than assumed.
func countOverlaps(events []event) overlapStats {
	var st overlapStats
	for i := range events {
		live := 1
		for j := range events {
			if i == j {
				continue
			}
			if events[i].Call < events[j].Return && events[j].Call < events[i].Return {
				if j > i {
					st.pairs++
				}
				live++
			}
		}
		if live > st.max {
			st.max = live
		}
	}
	return st
}
