package model

import (
	"fmt"
	"strings"

	"github.com/anishathalye/porcupine"
)

// Partitioning.
//
// Porcupine checks each partition independently, so a history is
// linearizable iff every partition is. That is only true when the
// partitions cannot interact — and in THIS machine objects are not
// independent in general: `task.suspend x awaiting a` links `a` and `x`,
// and a settle on `a` wakes `x`.
//
// What makes partitioning sound here is an internal step of resonate's, not of the
// specification's: an awaiter and its awaited must share
// `resonate:origin`, enforced with
//
//	400 "Awaiter and awaited must belong to the same origin"
//
// (found while capturing traces — it is one of two places resonate is
// stricter than `01-protocol/validation.lean`). Ids are `origin.suffix`,
// so partitioning on the origin prefix keeps every awaits-edge inside one
// partition.
//
// That argument is a hypothesis about the input, so it is CHECKED rather
// than assumed: `CheckPartitionable` rejects a trace with a cross-origin
// suspend, and `-partition=false` turns the whole thing off. A checker
// whose soundness rests on an unverified property of its input is not a
// checker.

// originOf is the partition key: everything before the first '.', which
// is `resonate:origin` for ids resonate accepts.
func originOf(id string) string {
	if i := strings.IndexByte(id, '.'); i >= 0 {
		return id[:i]
	}
	return id
}

// partitionKey is the partition an operation belongs to.
//
// For almost every kind that is `originOf(o.ID)`. `task.heartbeat` is the
// exception: it carries no top-level id at all — its subject is the list
// of `(id, version)` refs in the request — so keying on `o.ID` put every
// heartbeat in the `""` partition, away from the tasks whose leases it
// extends.
//
// That is not a cosmetic misfiling. The lease extension is then never
// applied in the partition that owns the task, `leaseTimeoutAt` stays at its
// old value, and R5 `leaseExpiry` becomes enabled EARLIER than the
// specification allows. Since firing an internal step is a choice, an
// enabled-too-early internal step only ADDS candidates — so the partitioned replay
// accepts everything the whole-state replay does and possibly more. A
// false accept, which is the one direction a checker must never fail in.
func partitionKey(o Op) string {
	if o.Kind == "task.heartbeat" && len(o.Refs) > 0 {
		return originOf(o.Refs[0].ID)
	}
	return originOf(o.ID)
}

// CheckPartitionable verifies the property partitioning depends on: every
// awaits-edge stays within one origin. Returns the offending event if not.
func CheckPartitionable(ops []Op) error {
	for i, o := range ops {
		// A fence links the task and the promise its action targets, so it
		// is an awaits-edge for partitioning purposes just as a suspend is.
		// A heartbeat extends the lease of every task it names, so it
		// belongs to all their partitions at once. One origin is fine —
		// `partitionKey` routes it there. More than one is not
		// representable, and guessing would silently drop the extension
		// for every task outside the chosen partition.
		if o.Kind == "task.heartbeat" {
			for _, ref := range o.Refs {
				if originOf(ref.ID) != originOf(o.Refs[0].ID) {
					return fmt.Errorf("event %d: task.heartbeat names %s and %s across origins; partitioning would be unsound",
						i, o.Refs[0].ID, ref.ID)
				}
			}
			continue
		}
		if o.Kind == "task.fence" {
			if o.Action != nil && originOf(o.Action.ID) != originOf(o.ID) {
				return fmt.Errorf("event %d: task.fence %s acts on %s across origins; partitioning would be unsound",
					i, o.ID, o.Action.ID)
			}
			continue
		}
		if o.Kind != "task.suspend" && o.Kind != "promise.register_callback" {
			continue
		}
		want := originOf(o.ID)
		if o.Kind == "promise.register_callback" {
			if originOf(o.Awaiter) != want {
				return fmt.Errorf("event %d: %s links origins %q and %q; partitioning would be unsound",
					i, o.Kind, want, originOf(o.Awaiter))
			}
			continue
		}
		for _, a := range o.Awaited {
			if originOf(a) != want {
				return fmt.Errorf("event %d: task.suspend %s awaits %s across origins; partitioning would be unsound",
					i, o.ID, a)
			}
		}
	}
	return nil
}

// partitionOps groups operations by origin, preserving order within each.
func partitionOps(history []porcupine.Operation) [][]porcupine.Operation {
	groups := map[string][]porcupine.Operation{}
	var order []string
	for _, op := range history {
		k := partitionKey(op.Input.(Op))
		if _, seen := groups[k]; !seen {
			order = append(order, k)
		}
		groups[k] = append(groups[k], op)
	}
	out := make([][]porcupine.Operation, 0, len(order))
	for _, k := range order {
		out = append(out, groups[k])
	}
	return out
}

func partitionEvents(history []porcupine.Event) [][]porcupine.Event {
	// An Event carries its Input only on a call; a return has to be routed
	// to the same partition as its matching call.
	byID := map[int]string{}
	groups := map[string][]porcupine.Event{}
	var order []string
	for _, e := range history {
		var k string
		if e.Kind == porcupine.CallEvent {
			k = partitionKey(e.Value.(Op))
			byID[e.Id] = k
		} else {
			k = byID[e.Id]
		}
		if _, seen := groups[k]; !seen {
			order = append(order, k)
		}
		groups[k] = append(groups[k], e)
	}
	out := make([][]porcupine.Event, 0, len(order))
	for _, k := range order {
		out = append(out, groups[k])
	}
	return out
}

// ReplayPartitioned runs Replay per origin and stitches the results.
//
// The linearizability check partitions, so the witness pass must too, or
// it becomes the slow half: `enabledInternalSteps` and `Key` are both linear in
// the number of live objects, and on a 2200 event capture that is 600 of
// them for every step. Same soundness argument, same `CheckPartitionable`
// precondition.
func ReplayPartitioned(d Discipline, ops []Op, resps []Response) (witness []string, failedAt int, ok bool) {
	type group struct {
		ops   []Op
		resps []Response
		index []int // back to the position in the whole trace
	}
	groups := map[string]*group{}
	var order []string
	for i, o := range ops {
		k := partitionKey(o)
		g, seen := groups[k]
		if !seen {
			g = &group{}
			groups[k] = g
			order = append(order, k)
		}
		g.ops = append(g.ops, o)
		g.resps = append(g.resps, resps[i])
		g.index = append(g.index, i)
	}
	first := -1
	for _, k := range order {
		g := groups[k]
		w, at, gok := Replay(d, g.ops, g.resps)
		if !gok {
			if first < 0 || g.index[at] < first {
				first = g.index[at]
			}
			continue
		}
		witness = append(witness, w...)
	}
	if first >= 0 {
		return nil, first, false
	}
	return witness, -1, true
}
