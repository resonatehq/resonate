// Command scenarios drives a resonate server through the workflow shapes
// in `work/`, recording every request and response so the trace can be fed
// to the checkers in this repository.
//
//	scenarios simple-run    -runs 20 -parallel 4 -loops 2..5
//	scenarios fan-out       -runs 10 -parallel 8 -fanout 3..6 -depth 1..2 -contention 0.5
//	scenarios simple-sleep  -runs 10 -sleep 5..50ms
//	scenarios simple-rpc    -runs 20 -parallel 4 -loops 1..3
//
// Then:
//
//	lake exe checktrace < trace.ndjson                  # the Lean checker
//	go run ./valid/porc/cmd/lincheck  < trace.ndjson     # sequential
//	go run ./valid/porc/cmd/conccheck < trace.history    # searches orders
//
// Every numeric flag is an INTERVAL — `3`, `2..5`, `5..50ms` — drawn per
// invocation, because a scenario pinned to one shape produces one shape of
// trace and the existing captures are already a single shape repeated.
package main

import (
	stdctx "context"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	resonate "github.com/resonatehq/resonate/impl/sdk/go"
)

type stdCtx = stdctx.Context

func backgroundCtx() stdCtx { return stdctx.Background() }

// Range is an inclusive interval parsed from `n` or `lo..hi`.
type Range struct{ Lo, Hi int }

func (r *Range) String() string {
	if r.Lo == r.Hi {
		return strconv.Itoa(r.Lo)
	}
	return fmt.Sprintf("%d..%d", r.Lo, r.Hi)
}

func (r *Range) Set(s string) error {
	s = strings.TrimSuffix(strings.TrimSpace(s), "ms")
	lo, hi, found := strings.Cut(s, "..")
	a, err := strconv.Atoi(strings.TrimSpace(lo))
	if err != nil {
		return fmt.Errorf("not a number or interval: %q", s)
	}
	if !found {
		r.Lo, r.Hi = a, a
		return nil
	}
	b, err := strconv.Atoi(strings.TrimSpace(strings.TrimSuffix(hi, "ms")))
	if err != nil {
		return fmt.Errorf("not an interval: %q", s)
	}
	if b < a {
		return fmt.Errorf("interval is backwards: %q", s)
	}
	r.Lo, r.Hi = a, b
	return nil
}

type rng struct {
	mu sync.Mutex
	r  *rand.Rand
}

func (g *rng) pick(v Range) int {
	if v.Hi <= v.Lo {
		return v.Lo
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	return v.Lo + g.r.Intn(v.Hi-v.Lo+1)
}

func (g *rng) float() float64 {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.r.Float64()
}

// Config is every knob, so the scenario table can read them uniformly.
type Config struct {
	Loops, Fanout, Sleep, Depth Range
}

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	name := os.Args[1]
	if name == "-h" || name == "--help" || name == "help" {
		usage()
		return
	}
	sc, err := findScenario(name)
	if err != nil {
		fmt.Fprintln(os.Stderr, "scenarios:", err)
		os.Exit(2)
	}

	fs := flag.NewFlagSet(name, flag.ExitOnError)
	cfg := &Config{
		Loops: Range{2, 2}, Fanout: Range{3, 3}, Sleep: Range{10, 10}, Depth: Range{1, 1},
	}
	fs.Var(&cfg.Loops, "loops", "sequential iterations, e.g. 3 or 2..5")
	fs.Var(&cfg.Fanout, "fanout", "concurrent children, e.g. 4 or 2..8")
	fs.Var(&cfg.Sleep, "sleep", "durable sleep in ms, e.g. 10 or 5..50ms")
	fs.Var(&cfg.Depth, "depth", "fan-out recursion depth, e.g. 1 or 1..2")

	var (
		url        = fs.String("url", "http://127.0.0.1:8001", "resonate address; a comma-separated list sends each request to one at random (servers sharing a store)")
		prefix     = fs.String("prefix", "", "prepended to every origin, so invocations against one server never share ids (default under skulld: the slot and the start time)")
		reset      = fs.Bool("reset", true, "send debug.reset first (off when several invocations share a store)")
		runs       = fs.Int("runs", 10, "total workflow invocations")
		parallel   = fs.Int("parallel", 4, "fake clients running concurrently")
		contention = fs.Float64("contention", 0, "chance [0,1] a run reuses another client's origin")
		out        = fs.String("out", "trace", "output prefix: writes .ndjson and .history")
		seed       = fs.Int64("seed", 1, "rng seed")
		batch      = fs.Uint64("batch", 4, "requests sharing one debug instant")
		debug      = fs.Bool("debug-time", true, "stamp resonate:debug_time (needs RESONATE_DEBUG=true and debug.start)")
		timeout    = fs.Duration("timeout", 60*time.Second, "per-run timeout")
		transport  = fs.String("transport", "poll", "how workers receive work: poll (SSE, as the SDK ships) or push (the server POSTs to each client)")
		pushHost   = fs.String("push-host", "127.0.0.1", "push: the host the server reaches clients at")
		pushPort   = fs.Int("push-port", 0, "push: client i listens on push-port+i; 0 takes any free port")
		clock      = fs.String("clock", "logical", "debug instants: logical (from 1000) or wall (Unix ms, as the SDK's deadlines are); wall also ticks the server's clock forward")
		tickEvery  = fs.Duration("tick", 5*time.Millisecond, "clock wall: how often the server's clock is moved to the recorder's")
	)
	fs.Usage = func() {
		fmt.Fprintf(os.Stderr, "usage: scenarios %s [flags]\n\n  %s\n\nflags:\n", name, sc.help)
		fs.PrintDefaults()
	}
	_ = fs.Parse(os.Args[2:])

	if *parallel < 1 {
		*parallel = 1
	}
	g := &rng{r: rand.New(rand.NewSource(*seed))}
	rec := NewRecorder(1000, *batch, *debug)
	if *clock == "wall" {
		rec.UseWallClock()
	}
	urls := strings.Split(*url, ",")

	// Under skulld: stream every event to the host as it is recorded, and
	// keep this invocation's ids apart from the ones before it in the run.
	slot, underSkull := os.LookupEnv("SKULL_SLOT")
	if *prefix == "" && underSkull {
		*prefix = fmt.Sprintf("s%s-%x-", slot, time.Now().UnixNano()&0xffffffff)
	}
	invocation := strings.TrimSuffix(*prefix, "-")
	if invocation == "" {
		invocation = name
	}
	if sink := dialSkull(); sink != nil {
		rec.sink = func(e Event) { sink.trace(invocation, e) }
		fmt.Printf("streaming the trace to skulld as invocation %s\n", invocation)
	}

	if *debug {
		for _, u := range urls {
			if err := debugStart(u, *reset); err != nil {
				fmt.Fprintln(os.Stderr, "scenarios: debug.start failed:", err)
				fmt.Fprintln(os.Stderr, "  start the server with RESONATE_DEBUG=true, or pass -debug-time=false")
				os.Exit(2)
			}
		}
	}

	// Origins. Each client owns one, which is what keeps every awaits-edge
	// inside a partition — the property both checkers' partitioning rests
	// on. `-contention` deliberately breaks that for a fraction of runs, so
	// the traffic includes clients fighting over one workflow id.
	origins := make([]string, *parallel)
	for i := range origins {
		origins[i] = fmt.Sprintf("%sc%d", *prefix, i)
	}

	fmt.Printf("%s: %d runs, %d clients, contention %.2f\n", name, *runs, *parallel, *contention)
	fmt.Printf("  loops=%s fanout=%s sleep=%sms depth=%s\n",
		cfg.Loops.String(), cfg.Fanout.String(), cfg.Sleep.String(), cfg.Depth.String())

	var (
		wg        sync.WaitGroup
		counter   = make(chan int, *runs)
		okN, errN int
		mu        sync.Mutex
	)
	for i := 0; i < *runs; i++ {
		counter <- i
	}
	close(counter)

	if *debug && *clock == "wall" {
		tickCtx, stopTicking := stdctx.WithCancel(stdctx.Background())
		defer stopTicking()
		go debugTicker(tickCtx, urls, rec, *tickEvery)
	}

	start := time.Now()
	for c := 0; c < *parallel; c++ {
		wg.Add(1)
		go func(cid int) {
			defer wg.Done()
			client := origins[cid]
			var network resonate.Network
			switch *transport {
			case "push":
				port := 0
				if *pushPort > 0 {
					port = *pushPort + cid
				}
				push, err := NewPushNetwork(urls, client, *pushHost, port)
				if err != nil {
					fmt.Fprintf(os.Stderr, "client %s: %v\n", client, err)
					return
				}
				network = rec.Wrap(push, client)
			default:
				network = rec.Network(urls[0], client)
			}
			r, err := resonate.New(resonate.Config{Network: network})
			if err != nil {
				fmt.Fprintf(os.Stderr, "client %s: %v\n", client, err)
				return
			}
			defer func() { _ = r.Stop() }()

			invoke, err := register(r)
			if err != nil {
				fmt.Fprintf(os.Stderr, "client %s: register: %v\n", client, err)
				return
			}

			for i := range counter {
				// Contention: with probability p, use ANOTHER client's
				// origin, so two clients race on the same workflow id. That
				// is the case the checkers should still explain, and the
				// one a per-client harness never produces.
				origin := client
				if *contention > 0 && g.float() < *contention {
					origin = origins[g.pick(Range{0, *parallel - 1})]
				}
				// Dashes, not dots. The server rejects a `resonate:origin`
				// containing '.', and the SDK derives origin from the root
				// promise id. Dots then reappear where they belong: the SDK
				// names children `<root>.<n>`, so the checkers' partition key
				// — everything before the first '.' — resolves to the root
				// workflow, and one workflow is one partition.
				id := fmt.Sprintf("%s-%s-%d", origin, name, i)

				done := make(chan error, 1)
				go func() { done <- invoke[name](id, sc.args(cfg, g)) }()
				select {
				case err := <-done:
					mu.Lock()
					if err != nil {
						errN++
						if errN <= 3 {
							fmt.Fprintf(os.Stderr, "  run %s: %v\n", id, err)
						}
					} else {
						okN++
					}
					mu.Unlock()
				case <-time.After(*timeout):
					mu.Lock()
					errN++
					mu.Unlock()
				}
			}
		}(c)
	}
	wg.Wait()
	elapsed := time.Since(start).Round(time.Millisecond)

	if *out == "" {
		fmt.Printf("\n%d runs ok, %d failed, %v, %d events\n", okN, errN, elapsed, rec.Len())
		return
	}
	if err := rec.WriteNDJSON(*out + ".ndjson"); err != nil {
		fmt.Fprintln(os.Stderr, "scenarios: write ndjson:", err)
		os.Exit(1)
	}
	if err := rec.WriteHistory(*out + ".history"); err != nil {
		fmt.Fprintln(os.Stderr, "scenarios: write history:", err)
		os.Exit(1)
	}

	pairs, maxc := rec.Overlaps()
	kinds, statuses := rec.Summary()
	fmt.Printf("\n%d runs ok, %d failed, %v\n", okN, errN, elapsed)
	fmt.Printf("  recorded %d protocol events\n", rec.Len())
	fmt.Printf("  overlapping pairs: %d   max concurrency: %d\n", pairs, maxc)
	fmt.Printf("  kinds:    %s\n", sortedKinds(kinds))
	fmt.Printf("  statuses: %v\n", statuses)
	if sk := rec.Skipped(); len(sk) > 0 {
		fmt.Printf("  DROPPED (no checker can decode these): %s\n", sortedKinds(sk))
		fmt.Printf("    the trace is INCOMPLETE — see recordable() in record.go\n")
	}
	fmt.Printf("  wrote %s.ndjson (for checktrace / lincheck)\n", *out)
	fmt.Printf("  wrote %s.history (for conccheck)\n", *out)
	if maxc < 2 {
		fmt.Println("  NOTE: nothing overlapped — raise -parallel or -batch for a concurrent history")
	}
	if rec.Len() == 0 {
		fmt.Println("  NOTE: no events recorded; every run failed")
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, "usage: scenarios <scenario> [flags]")
	fmt.Fprintln(os.Stderr, "\nscenarios:")
	for _, s := range scenarios {
		fmt.Fprintf(os.Stderr, "  %-14s %s\n", s.name, s.help)
	}
	fmt.Fprintln(os.Stderr, "\nrun `scenarios <scenario> -h` for flags.")
	fmt.Fprintln(os.Stderr, "\nEvery numeric flag accepts an interval: 3, 2..5, 5..50ms.")
}
