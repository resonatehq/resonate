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
	jumpy := flag.Bool("jumpy", true, "allow large clock jumps (reaches the timeout internal steps; raises the Lean decline rate)")
	maxMut := flag.Int("mutants", 40, "corpus mode: mutants sampled per trace (0 = every event)")
	corpus := flag.String("corpus", "", "check RECORDED traces (comma-separated .ndjson) instead of generated ones")
	keep := flag.String("keep", "", "directory to write disagreeing traces to")
	goonly := flag.Bool("goonly", false, "skip the Lean checker (every Lean verdict becomes DECLINE — the differential halves are vacuous, the Go properties still bite)")
	pend := flag.Bool("pending", true, "also fuzz the pending-op (500) semantics — Go checker only; the Lean checker does not know pending ops")
	coneCheck := flag.Bool("conecheck", true, "also require the cone-of-influence reduction and the full closure to agree on every verdict")
	tagCheck := flag.Bool("tagrule", true, "also require the tag rules to hold on generated traffic: a targeted timer is refused, a timer born past its deadline resolves")
	affCheck := flag.Bool("affects", true, "also require every firing's REACHABLE writes to be declared in its `affects` — the cone's soundness obligation, arming included")
	affCap := flag.Int("affectscap", 2000, "node cap for the arming search; a state whose search hits it is counted, not silently passed")
	flag.Parse()

	leanBin := *lean
	if leanBin == "" {
		leanBin = "../.lake/build/bin/checktrace"
	}
	if *goonly {

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

		pendWeaken int
		pendMask   int
		pendFail   int

		coneChecked int
		coneFail    int
		affChecked  int
		affFail     int
		affTrunc    int
		tagRefused  int
		tagBornDead int
		tagFail     int
	)
	start := time.Now()

	for i := 0; i < *n; i++ {
		seed := *seed0 + int64(i)
		g := model.NewGen(seed, "o")
		g.Jumpy = *jumpy
		script := g.Script(*steps)
		ops, resps, internalStepHits := model.Run(model.Materialized, script)
		if len(ops) == 0 {
			continue
		}
		for k, v := range internalStepHits {
			fired[k] += v
		}
		for _, r := range resps {
			statuses[r.Status]++
		}

		if *tagCheck {
			for k, o := range ops {
				r := resps[k]
				switch {
				case o.Kind == "promise.create" && o.Tags.TimerTargeted():
					tagRefused++
					if r.Status != 400 {
						tagFail++
						fmt.Printf("TAG RULE seed=%d: targeted timer promise.create answered %d, want 400\n", seed, r.Status)
					}
				case o.Kind == "task.create" && o.Action != nil && o.Action.Tags.TimerTargeted():
					tagRefused++
					if r.Status != 400 {
						tagFail++
						fmt.Printf("TAG RULE seed=%d: targeted timer task.create answered %d, want 400\n", seed, r.Status)
					}
				case o.Kind == "promise.create" && r.Promise != nil &&
					r.Promise.Tags.IsTimer() && r.Promise.TimeoutAt <= o.Now:

					tagBornDead++
					if r.Status != 200 || r.Promise.State != model.Resolved {
						tagFail++
						fmt.Printf("TAG RULE seed=%d: timer past its deadline read %v, want RESOLVED\n",
							seed, r.Promise.State)
					}
				}
			}
		}

		if *affCheck {
			for _, st := range model.States(model.Materialized, script) {
				affChecked++
				bad, complete := model.ArmingSound(st.S, st.Now, *affCap)
				if !complete {
					affTrunc++
				}
				if bad != "" {
					affFail++
					fmt.Printf("AFFECTS UNSOUND seed=%d: %s\n", seed, bad)
					break
				}
			}
		}

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

		if *affCheck {
			fmt.Printf("  affects:  %d states checked, %d firings writing outside their declared set\n",
				affChecked, affFail)
		}
		if *coneCheck {

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

			wresps := pendingize(resps, pickIdxs(seed, len(resps), 1+int(seed%3))...)
			pendWeaken++
			if v := goVerdict(ops, wresps); v != accept {
				pendFail++
				disagree++
				report(*keep, seed, "PENDING-WEAKEN", emit(ops, wresps), v, decline, ops, wresps)
			}

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
	if *tagCheck {
		fmt.Printf("  tagrule:  %d targeted timers refused, %d timers born past their deadline, %d violations\n",
			tagRefused, tagBornDead, tagFail)
	}
	if *affCheck {
		fmt.Printf("  affects:  %d states checked, %d firings reaching outside their declared set, %d searches truncated at the cap\n",
			affChecked, affFail, affTrunc)
	}
	if *coneCheck {
		fmt.Printf("  cone:     %d traces (+mutants) compared against the full closure, %d disagreements\n",
			coneChecked, coneFail)
	}
	fmt.Printf("  internal-step firings that changed state: %v\n", sortedCounts(fired))
	fmt.Printf("  response statuses generated:     %v\n", sortedInts(statuses))
	if disagree > 0 {
		fmt.Printf("\n  *** %d DISAGREEMENTS ***\n", disagree)
		os.Exit(1)
	}
	fmt.Println("\n  no disagreements")
}

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
	replay := model.Replay
	if err := model.CheckPartitionable(ops); err == nil {
		replay = model.ReplayPartitioned
	}
	if _, _, ok := replay(model.Materialized, ops, resps); ok {
		return accept
	}
	return refute
}

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

func verdictWithCone(cone bool, ops []model.Op, resps []model.Response) verdict {
	prev := model.Cone
	model.Cone = cone
	defer func() { model.Cone = prev }()
	return goVerdict(ops, resps)
}

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

func pendingize(resps []model.Response, idxs ...int) []model.Response {
	out := append([]model.Response(nil), resps...)
	for _, i := range idxs {
		out[i] = model.Response{Status: 500}
	}
	return out
}

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
		return "(none — the corpus never reached an internal step)"
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
