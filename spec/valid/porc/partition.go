package model

import (
	"fmt"
	"strings"

	"github.com/anishathalye/porcupine"
)

func originOf(id string) string {
	if i := strings.IndexByte(id, '.'); i >= 0 {
		return id[:i]
	}
	return id
}

func partitionKey(o Op) string {
	if o.Kind == "task.heartbeat" && len(o.Refs) > 0 {
		return originOf(o.Refs[0].ID)
	}
	return originOf(o.ID)
}

func CheckPartitionable(ops []Op) error {
	for i, o := range ops {

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

func ReplayPartitioned(d Discipline, ops []Op, resps []Response) (witness []string, failedAt int, ok bool) {
	type group struct {
		ops   []Op
		resps []Response
		index []int
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
