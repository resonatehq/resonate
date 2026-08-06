// Command fuzz is a differential fuzzer between the two trace checkers in
// this repository: the Lean one in `valid/` (concrete machine, hand-rolled
// subset construction) and the Go one here (abstract machine, porcupine's
// power-set construction).
//
//	go run ./cmd/fuzz -n 500
//
// Nothing is shared between them but the specification and the file
// format, so a disagreement means one of them is wrong — and prints the
// trace that proves it.
//
// Two properties are checked per generated trace:
//
//	VALID    a trace produced by running a script is explainable by
//	         construction (the script IS the execution), so BOTH checkers
//	         must accept it.
//	MUTANT   corrupt one response and both must refute. Agreement on
//	         WHETHER is what matters; they need not agree on where.
//
// The second is what stops a vacuous pass: two checkers that accept
// everything would sail through the first property forever.
package main

import (
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"time"

	model "github.com/resonatehq/resonate-specification/porcupine"
)

type verdict int

const (
	accept verdict = iota
	refute
	decline
	broken
)

func (v verdict) String() string {
	return [...]string{"ACCEPT", "REFUTE", "DECLINE", "ERROR"}[v]
}

func main() {
	n := flag.Int("n", 200, "traces to generate")
	steps := flag.Int("steps", 40, "script steps per trace")
	seed0 := flag.Int64("seed", 1, "base seed")
	lean := flag.String("lean", "", "path to the Lean checktrace binary (default: ../.lake/build/bin/checktrace)")
	fuel := flag.Int("fuel", 64, "fuel passed to the Lean checker")
	cap_ := flag.Int("cap", 200000, "candidate cap passed to the Lean checker")
	jumpy := flag.Bool("jumpy", true, "allow large clock jumps (reaches the timeout rules; raises the Lean decline rate)")
	keep := flag.String("keep", "", "directory to write disagreeing traces to")
	flag.Parse()

	leanBin := *lean
	if leanBin == "" {
		leanBin = "../.lake/build/bin/checktrace"
	}
	if _, err := os.Stat(leanBin); err != nil {
		fmt.Fprintf(os.Stderr, "fuzz: Lean checker not found at %s\n", leanBin)
		fmt.Fprintln(os.Stderr, "      build it with: (cd .. && lake build checktrace)")
		os.Exit(2)
	}

	var (
		disagree  int
		checked   int
		mutants   int
		fired     = map[string]int{}
		statuses  = map[int]int{}
		leanCount = map[verdict]int{}
		goCount   = map[verdict]int{}
		mLean     = map[verdict]int{}
		mGo       = map[verdict]int{}
	)
	start := time.Now()

	for i := 0; i < *n; i++ {
		seed := *seed0 + int64(i)
		g := model.NewGen(seed, "o")
		g.Jumpy = *jumpy
		script := g.Script(*steps)
		ops, resps, ruleHits := model.Run(model.Materialized, script)
		if len(ops) == 0 {
			continue
		}
		for k, v := range ruleHits {
			fired[k] += v
		}
		for _, r := range resps {
			statuses[r.Status]++
		}

		// property 1 — a recorded run is explainable by construction
		nd := emit(ops, resps)
		gv := goVerdict(ops, resps)
		lv := leanVerdict(leanBin, nd, *cap_, *fuel)
		goCount[gv]++
		leanCount[lv]++
		checked++
		if !agree(gv, lv) || gv == refute || lv == refute {
			disagree++
			report(*keep, seed, "VALID", nd, gv, lv, ops, resps)
		}

		// property 2 — a corrupted run must be refused by both
		if mops, mresps, ok := mutate(seed, ops, resps); ok {
			mutants++
			mnd := emit(mops, mresps)
			mg := goVerdict(mops, mresps)
			ml := leanVerdict(leanBin, mnd, *cap_, *fuel)
			mGo[mg]++
			mLean[ml]++
			if !agree(mg, ml) {
				disagree++
				report(*keep, seed, "MUTANT", mnd, mg, ml, mops, mresps)
			}
		}
	}

	fmt.Printf("\n%d traces, %d mutants, %v\n", checked, mutants, time.Since(start).Round(time.Millisecond))
	fmt.Printf("  go   %v\n  lean %v\n", counts(goCount), counts(leanCount))
	if d := leanCount[decline]; d > 0 {
		fmt.Printf("  NOTE: the Lean checker DECLINED %d/%d (%d%%). A decline agrees with\n"+
			"        anything, so those comparisons are vacuous — the real sample size\n"+
			"        is %d, not %d.\n", d, checked, 100*d/checked, checked-d, checked)
	}
	fmt.Printf("  mutants:  go %v | lean %v\n", counts(mGo), counts(mLean))
	if mGo[accept] > 0 || mLean[accept] > 0 {
		fmt.Printf("  NOTE: %d/%d mutants were ACCEPTED. A corrupted response that both\n"+
			"        checkers still explain is not a bug — the model may genuinely\n"+
			"        admit it — but a high rate means the mutation is too weak to\n"+
			"        discriminate.\n", max(mGo[accept], mLean[accept]), mutants)
	}
	fmt.Printf("  rule firings that changed state: %v\n", sortedCounts(fired))
	fmt.Printf("  response statuses generated:     %v\n", sortedInts(statuses))
	if disagree > 0 {
		fmt.Printf("\n  *** %d DISAGREEMENTS ***\n", disagree)
		os.Exit(1)
	}
	fmt.Println("\n  no disagreements")
}

// agree treats DECLINE as agreeing with anything: a checker that declined
// made no claim, so there is nothing to contradict.
func agree(a, b verdict) bool {
	if a == broken || b == broken {
		return false
	}
	if a == decline || b == decline {
		return true
	}
	return a == b
}

func goVerdict(ops []model.Op, resps []model.Response) verdict {
	if _, _, ok := model.Replay(model.Materialized, ops, resps); ok {
		return accept
	}
	return refute
}

func leanVerdict(bin, ndjson string, cap, fuel int) verdict {
	cmd := exec.Command(bin, fmt.Sprint(cap), fmt.Sprint(fuel))
	cmd.Stdin = strings.NewReader(ndjson)
	out, err := cmd.CombinedOutput()
	if err == nil {
		return accept
	}
	if ee, ok := err.(*exec.ExitError); ok {
		switch ee.ExitCode() {
		case 1:
			return refute
		case 3:
			return decline
		}
	}
	fmt.Fprintf(os.Stderr, "lean checker error: %v\n%s\n", err, out)
	return broken
}

func emit(ops []model.Op, resps []model.Response) string { return model.Emit(ops, resps) }

// mutate corrupts exactly one response. Status flips are the coarsest
// channel and the easiest for both checkers to catch; record flips are
// the interesting ones, since they test that both compare the records
// rather than just the code.
func mutate(seed int64, ops []model.Op, resps []model.Response) ([]model.Op, []model.Response, bool) {
	out := append([]model.Response(nil), resps...)
	i := int(seed) % len(out)
	r := out[i]
	switch {
	case r.Task != nil:
		t := *r.Task
		t.Version++
		out[i] = model.Response{Status: r.Status, Promise: r.Promise, Task: &t}
	case r.Promise != nil:
		p := *r.Promise
		if p.State == model.Pending {
			p.State = model.Resolved
		} else {
			p.State = model.Pending
			p.SettledAt = nil
		}
		out[i] = model.Response{Status: r.Status, Promise: &p}
	case r.Status == 200:
		out[i] = model.Response{Status: 404}
	default:
		out[i] = model.Response{Status: 200}
	}
	return ops, out, true
}

func report(dir string, seed int64, kind, ndjson string, gv, lv verdict, ops []model.Op, resps []model.Response) {
	fmt.Printf("\nDISAGREEMENT  seed=%d  %s   go=%v  lean=%v\n", seed, kind, gv, lv)
	_, at, ok := model.Replay(model.Materialized, ops, resps)
	if !ok {
		fmt.Printf("  go stops at event %d: %v (status %d)\n", at, ops[at], resps[at].Status)
	}
	if dir == "" {
		lines := strings.Split(strings.TrimRight(ndjson, "\n"), "\n")
		for i, l := range lines {
			if i >= 12 {
				fmt.Printf("  … %d more events\n", len(lines)-12)
				break
			}
			fmt.Println("  " + l)
		}
		return
	}
	_ = os.MkdirAll(dir, 0o755)
	path := filepath.Join(dir, fmt.Sprintf("seed-%d-%s.ndjson", seed, strings.ToLower(kind)))
	if err := os.WriteFile(path, []byte(ndjson), 0o644); err != nil {
		fmt.Println("  (could not write:", err, ")")
		return
	}
	fmt.Println("  written to", path)
}

func counts(m map[verdict]int) string {
	var f []string
	for _, v := range []verdict{accept, refute, decline, broken} {
		if m[v] > 0 {
			f = append(f, fmt.Sprintf("%v=%d", v, m[v]))
		}
	}
	return strings.Join(f, " ")
}

func sortedCounts(m map[string]int) string {
	ks := make([]string, 0, len(m))
	for k := range m {
		ks = append(ks, k)
	}
	sort.Strings(ks)
	var f []string
	for _, k := range ks {
		f = append(f, fmt.Sprintf("%s=%d", k, m[k]))
	}
	if len(f) == 0 {
		return "(none — the corpus never reached a rule)"
	}
	return strings.Join(f, " ")
}

func sortedInts(m map[int]int) string {
	ks := make([]int, 0, len(m))
	for k := range m {
		ks = append(ks, k)
	}
	sort.Ints(ks)
	var f []string
	for _, k := range ks {
		f = append(f, fmt.Sprintf("%d=%d", k, m[k]))
	}
	return strings.Join(f, " ")
}
