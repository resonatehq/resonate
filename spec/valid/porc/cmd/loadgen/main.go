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
)

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

	if err := post(*url, "debug.start", map[string]any{}, nil); err != nil {
		fmt.Fprintln(os.Stderr, "loadgen: debug.start failed:", err)
		fmt.Fprintln(os.Stderr, "         start the server with RESONATE_DEBUG=true")
		os.Exit(2)
	}
	_ = post(*url, "debug.reset", map[string]any{}, nil)

	clk := &clock{now: 1000}
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

				op, kind := nextOp(r, cid, int(i))
				now := clk.next(*batch)
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
				events = append(events, event{Kind: kind, Now: now, Req: reqJSON,
					Res: res, Call: call, Return: ret, Client: cid})
				mu.Unlock()
			}
		}(c)
	}
	wg.Wait()

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
