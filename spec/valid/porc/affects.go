package model

import "sort"

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
				continue
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

func armingViolation(s *ServerState, now uint64, f firing, cap int) string {
	declared := map[string]bool{}
	for _, id := range f.affects {
		declared[id] = true
	}
	reach, complete := reachableWrites(s, now, f.fire, cap)
	if !complete {
		return ""
	}
	for _, id := range reach {
		if !declared[id] {
			return f.name + " can reach a write to " + id + ", not in affects"
		}
	}
	return ""
}

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
