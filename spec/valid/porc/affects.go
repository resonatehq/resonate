package model

import "sort"

// The cone of influence keeps a firing only when its `affects` set meets
// what the request touches, so `affects` carries a SOUNDNESS obligation:
//
//	if a firing can change anything a future response could report,
//	its id is in `affects`.
//
// Get that wrong and the reduction drops a firing the trace needed, and
// the checker refutes a correct server. It has been wrong once before —
// R1 arms R4 for every awaiter of the promise it settles, and the first
// version of the Lean cone missed that chain.
//
// Half of the obligation is mechanically checkable, and this file checks
// it. The half about ARMING — which internal steps a firing newly enables — stays
// hand-reasoned, because it is a claim about the future rather than about
// this write. What is checkable is the write half: run the firing and look
// at what moved.
//
// The projection below is not "what changed in the state" but "what
// changed that a response would carry", and it is deliberately the same
// projection `matches` compares against a capture: the promise record
// `samePromiseRecord` reads, and the task fields `matches` reads. A firing
// that moves only callbacks, listeners, `expiresAt`, `retryAt` or the
// outbox is invisible on the channel this checker compares — which is
// exactly what an empty `affects` asserts about R3 and R6.

// promiseFacet is the part of a promise a response can carry —
// `PromiseObject.toRecord` in spec/02-abstract/state.lean.
type promiseFacet struct {
	state              PromiseState
	timeoutAt          uint64
	createdAt          uint64
	settledAt          string
	param, value, tags string
}

func facetOf(p *Promise) promiseFacet {
	f := promiseFacet{state: p.State, timeoutAt: p.TimeoutAt, createdAt: p.CreatedAt,
		param: string(p.Param), value: string(p.Value), tags: p.Tags.key()}
	f.settledAt = u64s(p.SettledAt)
	return f
}

// taskFacet is the part of a task a response can carry — `TaskObject.toRecord`.
// `resumes` is a COUNT there, not the list, so two different resume lists of
// equal length are indistinguishable and must not count as a write.
type taskFacet struct {
	state    TaskState
	version  uint64
	resumes  int
	ttl, pid string
}

func taskFacetOf(t *Task) taskFacet {
	f := taskFacet{state: t.State, version: t.Version, resumes: len(t.Resumes)}
	f.ttl = u64s(t.TTL)
	if t.PID != nil {
		f.pid = *t.PID
	}
	return f
}

// observableWrites fires against a copy and returns the ids whose
// response-projected state moved.
func observableWrites(s *ServerState, fire func(*ServerState)) []string {
	before := map[string]promiseFacet{}
	beforeT := map[string]taskFacet{}
	for _, o := range s.Objects {
		before[o.ID] = facetOf(o.Promise)
		if o.Task != nil {
			beforeT[o.ID] = taskFacetOf(o.Task)
		}
	}

	u := s.clone()
	fire(u)

	moved := map[string]bool{}
	for _, o := range u.Objects {
		if f, ok := before[o.ID]; !ok || f != facetOf(o.Promise) {
			moved[o.ID] = true
		}
		if o.Task != nil {
			if f, ok := beforeT[o.ID]; !ok || f != taskFacetOf(o.Task) {
				moved[o.ID] = true
			}
		}
	}
	out := make([]string, 0, len(moved))
	for id := range moved {
		out = append(out, id)
	}
	sort.Strings(out)
	return out
}

// AffectsSound checks the write half of the cone's obligation at one state:
// every id a firing observably writes is declared in its `affects`. Returns
// the offending internal-step name, or "" when every firing is covered.
func AffectsSound(s *ServerState, now uint64) string {
	for _, f := range enabledFirings(s, now) {
		declared := map[string]bool{}
		for _, id := range f.affects {
			declared[id] = true
		}
		for _, id := range observableWrites(s, f.fire) {
			if !declared[id] {
				return f.name + " writes " + id + ", not in affects"
			}
		}
	}
	return ""
}

// ---------------------------------------------------------------- arming

// The other half of the obligation, and the one that has been wrong.
//
// `relevantInternalSteps` computes the cone over `enabledFirings(s, now)` — the
// firings enabled AT THIS STATE. A firing that only becomes enabled after
// another fires is not in that list and no fixpoint over it can pull the
// firing in. R1 is the standing case: settling a promise is what ENABLES
// R4 for each awaiter on its ledger, and at the moment the cone is
// computed those R4s do not exist to be found. R1 therefore has to carry
// the awaiters in its own `affects`, which is why its set is
// `callbacks + p.ID` and not the `{p.ID}` it writes. Miss that and a
// request touching the awaiter drops R1, never reaches the R4 it would
// have armed, and a correct server is refuted. That is the bug the Lean
// cone shipped once.
//
// Stated as an obligation over reachability rather than over one write:
//
//	if firing f, followed by any number of further firings, can change
//	what a request touching k would see, then k is in f.affects.
//
// which is checkable by exploring: fire f, then keep firing what f ARMED,
// and watch which ids move.
//
// Only what it armed. A firing already enabled at s is a candidate in the
// fixpoint on its own merits — if it writes k then k is in ITS affects, by
// the write half above, so a request touching k pulls it in without any
// help from f. Following those too makes the check reject sound tables:
// it reported R3 removing a listener as "reaching" a task write, when all
// that happened was an unrelated firing, enabled the whole time, doing its
// own job. What f owes is only the firings that did not exist to be found.
//
// Termination is by state key — the clock is fixed at `now`, so the
// reachable set is finite — with a node cap so a pathological state cannot
// hang the fuzzer. `complete` reports whether the cap was hit, because a
// check that quietly truncates its own search is worse than no check.
func reachableWrites(s *ServerState, now uint64, first func(*ServerState), cap int) (out []string, complete bool) {
	base := map[string]promiseFacet{}
	baseT := map[string]taskFacet{}
	for _, o := range s.Objects {
		base[o.ID] = facetOf(o.Promise)
		if o.Task != nil {
			baseT[o.ID] = taskFacetOf(o.Task)
		}
	}

	moved := map[string]bool{}
	note := func(u *ServerState) {
		for _, o := range u.Objects {
			if f, ok := base[o.ID]; !ok || f != facetOf(o.Promise) {
				moved[o.ID] = true
			}
			if o.Task != nil {
				if f, ok := baseT[o.ID]; !ok || f != taskFacetOf(o.Task) {
					moved[o.ID] = true
				}
			}
		}
	}

	// the firings already available before f — not f's responsibility
	preEnabled := map[string]bool{}
	for _, g := range enabledFirings(s, now) {
		preEnabled[g.name] = true
	}

	head := s.clone()
	first(head)
	note(head)

	seen := map[string]bool{head.Key(): true}
	frontier := []*ServerState{head}
	complete = true
	for len(frontier) > 0 {
		cur := frontier[0]
		frontier = frontier[1:]
		for _, g := range enabledFirings(cur, now) {
			if preEnabled[g.name] {
				continue // enabled all along; it answers for its own writes
			}
			next := cur.clone()
			g.fire(next)
			k := next.Key()
			if seen[k] {
				continue
			}
			if len(seen) >= cap {
				complete = false
				frontier = nil
				break
			}
			seen[k] = true
			note(next)
			frontier = append(frontier, next)
		}
	}

	out = make([]string, 0, len(moved))
	for id := range moved {
		out = append(out, id)
	}
	sort.Strings(out)
	return out, complete
}

// armingViolation checks one firing's reachability obligation. Exported
// shape takes the firing so a test can hand it a deliberately understated
// `affects` and confirm the check bites.
func armingViolation(s *ServerState, now uint64, f firing, cap int) string {
	declared := map[string]bool{}
	for _, id := range f.affects {
		declared[id] = true
	}
	reach, complete := reachableWrites(s, now, f.fire, cap)
	if !complete {
		return "" // truncated search proves nothing; counted separately
	}
	for _, id := range reach {
		if !declared[id] {
			return f.name + " can reach a write to " + id + ", not in affects"
		}
	}
	return ""
}

// ArmingSound checks every enabled firing's reachability obligation.
// Returns the offending internal step, and whether every search ran to exhaustion.
func ArmingSound(s *ServerState, now uint64, cap int) (bad string, complete bool) {
	complete = true
	for _, f := range enabledFirings(s, now) {
		reach, ok := reachableWrites(s, now, f.fire, cap)
		if !ok {
			complete = false
			continue
		}
		declared := map[string]bool{}
		for _, id := range f.affects {
			declared[id] = true
		}
		for _, id := range reach {
			if !declared[id] {
				return f.name + " can reach a write to " + id + ", not in affects", complete
			}
		}
	}
	return "", complete
}
