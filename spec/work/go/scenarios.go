package main

import (
	"fmt"
	"time"

	resonate "github.com/resonatehq/resonate-sdk-go"
)

// The four scenarios, as durable functions.
//
// Every parameter is an INTERVAL — `Range` below — drawn per invocation.
// A scenario pinned to one shape produces one shape of trace; the point of
// this binary is to feed the checkers traffic they have not seen, and the
// recorded captures in valid/traces are already a single shape repeated
// 200 times.

// Args is the payload every scenario takes. One struct for all four keeps
// registration and dispatch uniform, and the JSON is small enough that
// carrying unused fields costs nothing.
type Args struct {
	Loops  int `json:"loops"`  // simple-run, simple-rpc: sequential steps
	Fanout int `json:"fanout"` // fan-out: concurrent children
	SleepM int `json:"sleepMs"`
	Depth  int `json:"depth"` // fan-out: recursion depth
}

// step is the leaf every scenario ultimately calls: pure, fast, and
// deterministic, so the trace records structure rather than noise.
func step(_ *resonate.Context, n int) (int, error) { return n * 2, nil }

// simpleRun: `loops` sequential LOCAL calls. `ctx.Run` keeps the child on
// this worker, so the trace shows one root promise and a chain of local
// children — the smallest shape that still exercises suspend/resume.
func simpleRun(ctx *resonate.Context, a Args) (int, error) {
	total := 0
	for i := 0; i < a.Loops; i++ {
		f, err := ctx.Run(step, i)
		if err != nil {
			return 0, err
		}
		var v int
		if err := f.Await(&v); err != nil {
			return 0, err
		}
		total += v
	}
	return total, nil
}

// simpleRPC: `loops` sequential REMOTE calls. `ctx.RPC` dispatches through
// the server, so each child is a targeted promise with its own task —
// which is what puts task.acquire/suspend/fulfill in the trace.
func simpleRPC(ctx *resonate.Context, a Args) (int, error) {
	total := 0
	for i := 0; i < a.Loops; i++ {
		f, err := ctx.RPC("step-remote", i)
		if err != nil {
			return 0, err
		}
		var v int
		if err := f.Await(&v); err != nil {
			return 0, err
		}
		total += v
	}
	return total, nil
}

// stepRemote is simpleRPC's callee, registered under its own name so the
// server can route to it.
func stepRemote(_ *resonate.Context, n int) (int, error) { return n * 2, nil }

// simpleSleep: durable sleep, `loops` times. A sleep is a promise with a
// deadline and no worker — the one scenario that makes the server's
// timeout path, and therefore the checkers' τ recovery, do real work.
func simpleSleep(ctx *resonate.Context, a Args) (int, error) {
	for i := 0; i < a.Loops; i++ {
		f, err := ctx.Sleep(time.Duration(a.SleepM) * time.Millisecond)
		if err != nil {
			return 0, err
		}
		if err := f.Await(nil); err != nil {
			return 0, err
		}
	}
	return a.Loops, nil
}

// fanOut: `fanout` children started BEFORE any is awaited, so they are
// genuinely concurrent, then joined. Recurses to `depth`.
//
// This is the scenario that matters for the checkers: a suspended parent
// with several outstanding callbacks is precisely the state where the
// resume schedule is unobservable and has to be reconstructed.
func fanOut(ctx *resonate.Context, a Args) (int, error) {
	futures := make([]*resonate.Future, 0, a.Fanout)
	for i := 0; i < a.Fanout; i++ {
		var (
			f   *resonate.Future
			err error
		)
		if a.Depth > 1 {
			f, err = ctx.Run(fanOut, Args{Fanout: a.Fanout, Depth: a.Depth - 1})
		} else {
			f, err = ctx.Run(step, i)
		}
		if err != nil {
			return 0, err
		}
		futures = append(futures, f)
	}
	total := 0
	for _, f := range futures {
		var v int
		if err := f.Await(&v); err != nil {
			return 0, err
		}
		total += v
	}
	return total, nil
}

// ---------------------------------------------------------------- registry

type scenario struct {
	name string
	help string
	// args builds the payload for one invocation from the drawn ranges.
	args func(cfg *Config, r *rng) Args
}

var scenarios = []scenario{
	{"simple-run", "sequential local ctx.Run calls",
		func(c *Config, r *rng) Args { return Args{Loops: r.pick(c.Loops)} }},
	{"simple-rpc", "sequential remote ctx.RPC calls",
		func(c *Config, r *rng) Args { return Args{Loops: r.pick(c.Loops)} }},
	{"simple-sleep", "durable ctx.Sleep in a loop",
		func(c *Config, r *rng) Args {
			return Args{Loops: r.pick(c.Loops), SleepM: r.pick(c.Sleep)}
		}},
	{"fan-out", "concurrent children, joined; recurses to -depth",
		func(c *Config, r *rng) Args {
			return Args{Fanout: r.pick(c.Fanout), Depth: r.pick(c.Depth)}
		}},
}

func scenarioNames() []string {
	out := make([]string, len(scenarios))
	for i, s := range scenarios {
		out[i] = s.name
	}
	return out
}

func findScenario(name string) (scenario, error) {
	for _, s := range scenarios {
		if s.name == name {
			return s, nil
		}
	}
	return scenario{}, fmt.Errorf("unknown scenario %q; want one of %v", name, scenarioNames())
}

// register wires all four into one Resonate client. Every client registers
// every function, because with contention two clients may end up running
// each other's workflows.
func register(r *resonate.Resonate) (map[string]func(string, Args) error, error) {
	runFn, err := resonate.Register(r, "simple-run", simpleRun)
	if err != nil {
		return nil, err
	}
	rpcFn, err := resonate.Register(r, "simple-rpc", simpleRPC)
	if err != nil {
		return nil, err
	}
	sleepFn, err := resonate.Register(r, "simple-sleep", simpleSleep)
	if err != nil {
		return nil, err
	}
	fanFn, err := resonate.Register(r, "fan-out", fanOut)
	if err != nil {
		return nil, err
	}
	if _, err := resonate.Register(r, "step-remote", stepRemote); err != nil {
		return nil, err
	}
	if _, err := resonate.Register(r, "step", step); err != nil {
		return nil, err
	}

	// One shape for all four: start, then await. `TypedHandle.Result`
	// returns (R, error), so the result is discarded here — the trace is
	// the deliverable, not the arithmetic.
	invoke := func(f *resonate.RegisteredFunc[Args, int]) func(string, Args) error {
		return func(id string, a Args) error {
			h, err := f.Run(backgroundCtx(), id, a)
			if err != nil {
				return err
			}
			_, err = h.Result(backgroundCtx())
			return err
		}
	}
	return map[string]func(string, Args) error{
		"simple-run":   invoke(runFn),
		"simple-rpc":   invoke(rpcFn),
		"simple-sleep": invoke(sleepFn),
		"fan-out":      invoke(fanFn),
	}, nil
}
