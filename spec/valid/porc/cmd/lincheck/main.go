package main

import (
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/anishathalye/porcupine"
	model "github.com/resonatehq/resonate-specification/valid/porc"
)

func main() {
	concurrent := flag.Int("concurrent", 1, "operations per overlapping window (1 = sequential history)")
	timeout := flag.Duration("timeout", 60*time.Second, "per-discipline check timeout")
	quiet := flag.Bool("quiet", false, "suppress the witness")
	partition := flag.Bool("partition", true, "check each resonate:origin independently (verified sound first)")
	cone := flag.Bool("cone", true, "restrict each gap's internal step closure to the cone of influence of the next observation (the full closure with -cone=false)")
	flag.Parse()
	model.Cone = *cone

	ops, resps, err := model.LoadTrace(os.Stdin)
	if err != nil {
		fmt.Fprintln(os.Stderr, "lincheck:", err)
		os.Exit(2)
	}
	if len(ops) == 0 {
		fmt.Fprintln(os.Stderr, "lincheck: empty trace on stdin")
		os.Exit(2)
	}
	if *partition {
		if err := model.CheckPartitionable(ops); err != nil {
			fmt.Fprintln(os.Stderr, "lincheck: cannot partition:", err)
			fmt.Fprintln(os.Stderr, "           re-run with -partition=false")
			os.Exit(2)
		}
	}
	partitioned = *partition
	pending := 0
	for _, r := range resps {
		if model.PendingOp(r) {
			pending++
		}
	}
	fmt.Printf("loaded %d events   history: %s   partition: %v\n",
		len(ops), historyDesc(*concurrent), *partition)
	if pending > 0 {

		fmt.Printf("pending: %d of %d ops answered 500 — each may or may not have applied; the verdict leaves them free\n",
			pending, len(ops))
	}
	fmt.Println()

	failed, inconclusive := false, false
	for _, d := range []model.Discipline{model.Projected, model.Materialized} {
		var sat model.Saturation
		nm := model.NondeterministicModel(d, &sat, *partition)
		hist := buildHistory(ops, resps, *concurrent)

		start := time.Now()
		res, _ := porcupine.CheckOperationsVerbose(nm.ToModel(), hist, *timeout)
		elapsed := time.Since(start)

		label := fmt.Sprintf("%-18s", d.String())
		switch {
		case res == porcupine.Ok && !sat.OK():
			fmt.Printf("%s INCONCLUSIVE  linearizable, but an internal step closure hit the fuel bound  %v\n", label, elapsed)
			inconclusive = true
		case res == porcupine.Ok:
			fmt.Printf("%s LINEARIZABLE  %v\n", label, elapsed)
			if !*quiet {
				printWitness(d, ops, resps)
			}
		case res == porcupine.Illegal && !sat.OK():
			fmt.Printf("%s INCONCLUSIVE  not linearizable, but a closure was cut short — no conclusion  %v\n", label, elapsed)
			inconclusive = true
		case res == porcupine.Illegal:
			fmt.Printf("%s NOT LINEARIZABLE  %v\n", label, elapsed)
			reportFirstFailure(d, ops, resps)
			failed = true
		default:
			fmt.Printf("%s TIMEOUT after %v\n", label, elapsed)
			inconclusive = true
		}
	}

	switch {
	case failed:
		os.Exit(1)
	case inconclusive:
		os.Exit(3)
	}
}

func historyDesc(c int) string {
	if c <= 1 {
		return "sequential (each call returns before the next begins)"
	}
	return fmt.Sprintf("%d-way overlap — porcupine must find an order", c)
}

func buildHistory(ops []model.Op, resps []model.Response, concurrent int) []porcupine.Operation {
	if concurrent < 1 {
		concurrent = 1
	}
	hist := make([]porcupine.Operation, len(ops))
	for i := range ops {
		call := int64(i) * 2
		ret := call + 1
		if concurrent > 1 {
			ret = call + int64(2*concurrent)
		}
		hist[i] = porcupine.Operation{
			ClientId: i % concurrent,
			Input:    ops[i],
			Call:     call,
			Output:   resps[i],
			Return:   ret,
		}
	}
	return hist
}

var partitioned = true

func replay(d model.Discipline, ops []model.Op, resps []model.Response, part bool) ([]string, int, bool) {
	if part {
		return model.ReplayPartitioned(d, ops, resps)
	}
	return model.Replay(d, ops, resps)
}

func printWitness(d model.Discipline, ops []model.Op, resps []model.Response) {
	w, _, ok := replay(d, ops, resps, partitioned)
	if !ok {
		fmt.Println("     (no sequential witness — the history linearizes only out of order)")
		return
	}
	if len(w) == 0 {
		fmt.Println("     witness: no internal steps needed")
		return
	}
	fmt.Printf("     witness: %d internal internal-step firings the server never reported\n", len(w))
	for i, r := range w {
		if i == 8 {
			fmt.Printf("       … and %d more\n", len(w)-8)
			break
		}
		fmt.Printf("       %s\n", r)
	}
}

func reportFirstFailure(d model.Discipline, ops []model.Op, resps []model.Response) {
	_, at, ok := replay(d, ops, resps, partitioned)
	if ok {
		fmt.Println("     (sequentially explainable — the failure is an ordering one)")
		return
	}
	fmt.Printf("     first unexplainable event: %d  %v\n", at, ops[at])
	fmt.Printf("       observed: status %d", resps[at].Status)
	if p := resps[at].Promise; p != nil {
		fmt.Printf("  promise %v", p.State)
		if p.SettledAt != nil {
			fmt.Printf(" settledAt %d", *p.SettledAt)
		}
	}
	if t := resps[at].Task; t != nil {
		fmt.Printf("  task %v v%d", t.State, t.Version)
	}
	fmt.Println()
}
