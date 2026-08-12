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
// it. The half about ARMING — which rules a firing newly enables — stays
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
	for _, p := range s.Promises {
		before[p.ID] = facetOf(p)
	}
	for _, t := range s.Tasks {
		beforeT[t.ID] = taskFacetOf(t)
	}

	u := s.clone()
	fire(u)

	moved := map[string]bool{}
	for _, p := range u.Promises {
		if f, ok := before[p.ID]; !ok || f != facetOf(p) {
			moved[p.ID] = true
		}
	}
	for _, t := range u.Tasks {
		if f, ok := beforeT[t.ID]; !ok || f != taskFacetOf(t) {
			moved[t.ID] = true
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
// the offending rule name, or "" when every firing is covered.
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
