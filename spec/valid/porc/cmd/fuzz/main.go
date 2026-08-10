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

	model "github.com/resonatehq/resonate-specification/valid/porc"
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
	maxMut := flag.Int("mutants", 40, "corpus mode: mutants sampled per trace (0 = every event)")
	corpus := flag.String("corpus", "", "check RECORDED traces (comma-separated .ndjson) instead of generated ones")
	keep := flag.String("keep", "", "directory to write disagreeing traces to")
	goonly := flag.Bool("goonly", false, "skip the Lean checker (every Lean verdict becomes DECLINE — the differential halves are vacuous, the Go properties still bite)")
	pend := flag.Bool("pending", true, "also fuzz the pending-op (500) semantics — Go checker only; the Lean checker does not know pending ops")
	coneCheck := flag.Bool("conecheck", true, "also require the cone-of-influence reduction and the full closure to agree on every verdict")
	flag.Parse()

	leanBin := *lean
	if leanBin == "" {
		leanBin = "../.lake/build/bin/checktrace"
	}
	if *goonly {
		// A decline agrees with anything and is counted as vacuous, which is
		// exactly the honest accounting for a checker that never ran.
		leanCheck = func(string, string, int, int) verdict { return decline }
	} else if _, err := os.Stat(leanBin); err != nil {
		fmt.Fprintf(os.Stderr, "fuzz: Lean checker not found at %s\n", leanBin)
		fmt.Fprintln(os.Stderr, "      build it with: (cd .. && lake build checktrace), or run -goonly")
		os.Exit(2)
	}

	if *corpus != "" {
		os.Exit(runCorpus(strings.Split(*corpus, ","), leanBin, *cap_, *fuel, *maxMut, *keep))
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
		// pending-op properties (Go checker only — the Lean checker does not
		// know pending ops, by design)
		pendWeaken int
		pendMask   int
		pendFail   int
		// cone-vs-full-closure agreement
		coneChecked int
		coneFail    int
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
		lv := leanCheck(leanBin, nd, *cap_, *fuel)
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
			ml := leanCheck(leanBin, mnd, *cap_, *fuel)
			mGo[mg]++
			mLean[ml]++
			if !agree(mg, ml) {
				disagree++
				report(*keep, seed, "MUTANT", mnd, mg, ml, mops, mresps)
			}
		}

		if *coneCheck {
			// property 5 — the cone is an OPTIMIZATION, not a semantics:
			// reduced and full closure must reach the same verdict on the
			// valid trace and on its mutant alike. (gv/mg above were computed
			// under the default, cone on.)
			coneChecked++
			if full := verdictWithCone(false, ops, resps); full != gv {
				coneFail++
				disagree++
				report(*keep, seed, "CONE-VALID", nd, gv, full, ops, resps)
			}
			if mops, mresps, ok := mutate(seed, ops, resps); ok {
				mg := goVerdict(mops, mresps)
				if full := verdictWithCone(false, mops, mresps); full != mg {
					coneFail++
					disagree++
					report(*keep, seed, "CONE-MUTANT", emit(mops, mresps), mg, full, mops, mresps)
				}
			}
		}

		if *pend {
			// property 3 — pendingization is EVIDENCE-WEAKENING. Replacing
			// any response with a 500 turns the op pending; the "applied,
			// response unobserved" branch subsumes the original evidence, so
			// a valid trace must stay accepted. A refutation here is a bug
			// in the pending semantics, never in the trace.
			wresps := pendingize(resps, pickIdxs(seed, len(resps), 1+int(seed%3))...)
			pendWeaken++
			if v := goVerdict(ops, wresps); v != accept {
				pendFail++
				disagree++
				report(*keep, seed, "PENDING-WEAKEN", emit(ops, wresps), v, decline, ops, wresps)
			}

			// property 4 — pendingizing the CORRUPTED response discards the
			// corruption: the mutation lived entirely in the response channel
			// the 500 replaces, so the refuted mutant must flip back to
			// accepted.
			if mops, mresps, ok := mutate(seed, ops, resps); ok {
				at := int(seed) % len(mresps)
				mresps = pendingize(mresps, at)
				pendMask++
				if v := goVerdict(mops, mresps); v != accept {
					pendFail++
					disagree++
					report(*keep, seed, "PENDING-MASK", emit(mops, mresps), v, decline, mops, mresps)
				}
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
	if *pend {
		fmt.Printf("  pending:  weaken=%d mask=%d violations=%d (Go only; a violation is a bug in the pending semantics)\n",
			pendWeaken, pendMask, pendFail)
	}
	if *coneCheck {
		fmt.Printf("  cone:     %d traces (+mutants) compared against the full closure, %d disagreements\n",
			coneChecked, coneFail)
	}
	fmt.Printf("  rule firings that changed state: %v\n", sortedCounts(fired))
	fmt.Printf("  response statuses generated:     %v\n", sortedInts(statuses))
	if disagree > 0 {
		fmt.Printf("\n  *** %d DISAGREEMENTS ***\n", disagree)
		os.Exit(1)
	}
	fmt.Println("\n  no disagreements")
}

// runCorpus applies the differential check to RECORDED traces.
//
// Generated traces exercise what the generator thought to write. A capture
// exercises what the SDK and server actually do — which is how `task.fence`
// turned out to be 60% of a real trace and absent from both checkers. So
// the same two properties are run over real traffic:
//
//	the trace as recorded    both checkers must agree (and a real capture
//	                         from a conforming server should be ACCEPTed)
//	one response corrupted   both must agree, and should REFUTE
//
// Every event is mutated in turn rather than one per file, because a
// capture is a single sample and one mutation would test one handler.
func runCorpus(paths []string, leanBin string, cap, fuel, maxMut int, keep string) int {
	disagree := 0
	for _, path := range paths {
		path = strings.TrimSpace(path)
		f, err := os.Open(path)
		if err != nil {
			fmt.Fprintln(os.Stderr, "fuzz:", err)
			return 2
		}
		ops, resps, err := model.LoadTrace(f)
		f.Close()
		if err != nil {
			fmt.Fprintf(os.Stderr, "fuzz: %s: %v\n", path, err)
			return 2
		}
		kinds := map[string]int{}
		for _, o := range ops {
			kinds[o.Kind]++
		}

		gv := goVerdict(ops, resps)
		lv := leanCheck(leanBin, model.Emit(ops, resps), cap, fuel)
		base := "agree"
		if !agree(gv, lv) {
			base = "*** DISAGREE ***"
			disagree++
		}
		fmt.Printf("%s\n  %d events   go=%v lean=%v   %s\n",
			filepath.Base(path), len(ops), gv, lv, base)
		fmt.Printf("  kinds: %s\n", sortedCounts(kinds))

		// Every event mutated in turn, or a spread sample. Each mutant
		// costs a Lean subprocess (~2s), so exhausting a 2200-event capture
		// is an hour; `-mutants 0` does it anyway when that is what you
		// want.
		stride := 1
		if maxMut > 0 && len(resps) > maxMut {
			stride = (len(resps) + maxMut - 1) / maxMut
		}
		var mAgree, mDis, mAcceptedAnyway, mTried int
		for i := range resps {
			if i%stride != 0 {
				continue
			}
			mTried++
			mops, mresps := mutateAt(i, ops, resps)
			mg := goVerdict(mops, mresps)
			ml := leanCheck(leanBin, model.Emit(mops, mresps), cap, fuel)
			if agree(mg, ml) {
				mAgree++
				if mg == accept {
					mAcceptedAnyway++
				}
			} else {
				mDis++
				disagree++
				report(keep, int64(i), "CORPUS-MUTANT", model.Emit(mops, mresps), mg, ml, mops, mresps)
			}
		}
		fmt.Printf("  mutants: %d/%d sampled — %d agree, %d DISAGREE", mTried, len(resps), mAgree, mDis)
		if mAcceptedAnyway > 0 {
			fmt.Printf("   (%d mutants both checkers still explain — the model admits them)", mAcceptedAnyway)
		}
		fmt.Println()
		fmt.Println()
	}
	if disagree > 0 {
		fmt.Printf("*** %d DISAGREEMENTS ***\n", disagree)
		return 1
	}
	fmt.Println("no disagreements")
	return 0
}

// mutateAt corrupts event i, preferring the record channel over the status
// channel because records are where the two checkers could differ subtly.
func mutateAt(i int, ops []model.Op, resps []model.Response) ([]model.Op, []model.Response) {
	out := append([]model.Response(nil), resps...)
	r := out[i]
	switch {
	case r.Task != nil:
		t := *r.Task
		t.Version++
		out[i] = model.Response{Status: r.Status, Promise: r.Promise, Task: &t, Inner: r.Inner}
	case r.Promise != nil:
		p := *r.Promise
		if p.State == model.Pending {
			p.State = model.Resolved
		} else {
			p.State = model.Pending
			p.SettledAt = nil
		}
		out[i] = model.Response{Status: r.Status, Promise: &p, Inner: r.Inner}
	case r.Inner != nil:
		in := *r.Inner
		in.Status = 404
		in.Promise = nil
		out[i] = model.Response{Status: r.Status, Inner: &in}
	case r.Status == 200:
		out[i] = model.Response{Status: 404}
	default:
		out[i] = model.Response{Status: 200}
	}
	return ops, out
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

// goVerdict replays the trace against the Go model.
//
// Partitioned WHEN THAT IS SOUND, and only then: `CheckPartitionable`
// scans for any event linking two `resonate:origin`s — a suspend, a
// callback or a fence reaching across — and the split is used only if it
// finds none. A recorded run from work/go is a dozen independent
// workflows interleaved across four clients, and replaying it as one
// state makes the candidate set the PRODUCT of the per-workflow sets: the
// first corpus run over four such captures reached 8.7 GB and had still
// not finished a single file.
//
// The verdict is the same either way. That is the point of verifying the
// split rather than assuming it.
func goVerdict(ops []model.Op, resps []model.Response) verdict {
	replay := model.Replay
	if err := model.CheckPartitionable(ops); err == nil {
		replay = model.ReplayPartitioned
	}
	if _, _, ok := replay(model.Materialized, ops, resps); ok {
		return accept
	}
	return refute
}

// leanCheck is the Lean half, replaceable so -goonly can stub it out with
// a permanent DECLINE instead of threading a flag through every call site.
var leanCheck = leanVerdict

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

// verdictWithCone runs goVerdict under an explicit cone setting, restoring
// the default after. The fuzzer is sequential, so flipping the package
// toggle is race-free.
func verdictWithCone(cone bool, ops []model.Op, resps []model.Response) verdict {
	prev := model.Cone
	model.Cone = cone
	defer func() { model.Cone = prev }()
	return goVerdict(ops, resps)
}

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

// pendingize replaces the responses at the given indices with a bare 500 —
// the pending op. The op's identity (kind, request, instant) is untouched;
// only its evidence is discarded.
func pendingize(resps []model.Response, idxs ...int) []model.Response {
	out := append([]model.Response(nil), resps...)
	for _, i := range idxs {
		out[i] = model.Response{Status: 500}
	}
	return out
}

// pickIdxs derives k distinct indices from the seed — deterministic, so a
// failure reproduces from its seed alone.
func pickIdxs(seed int64, n, k int) []int {
	if k > n {
		k = n
	}
	seen := map[int]bool{}
	out := make([]int, 0, k)
	x := uint64(seed)*2654435761 + 1
	for len(out) < k {
		x = x*6364136223846793005 + 1442695040888963407
		i := int(x % uint64(n))
		if !seen[i] {
			seen[i] = true
			out = append(out, i)
		}
	}
	return out
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
