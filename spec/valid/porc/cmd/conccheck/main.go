package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"sort"
	"time"

	"github.com/anishathalye/porcupine"
	model "github.com/resonatehq/resonate-specification/valid/porc"
)

type row struct {
	Kind   string          `json:"kind"`
	Now    uint64          `json:"now"`
	Req    json.RawMessage `json:"req"`
	Res    json.RawMessage `json:"res"`
	Call   int64           `json:"call"`
	Return int64           `json:"return"`
	Client int             `json:"client"`
}

func main() {
	timeout := flag.Duration("timeout", 120*time.Second, "per-discipline timeout")
	partition := flag.Bool("partition", true, "check each origin independently")
	cone := flag.Bool("cone", true, "restrict each gap's internal step closure to the cone of influence of the next observation (the full closure with -cone=false)")
	flag.Parse()
	model.Cone = *cone

	rows, err := readRows(os.Stdin)
	if err != nil {
		fmt.Fprintln(os.Stderr, "conccheck:", err)
		os.Exit(2)
	}
	if len(rows) == 0 {
		fmt.Fprintln(os.Stderr, "conccheck: empty history on stdin")
		os.Exit(2)
	}

	ops, resps, err := model.LoadTrace(ndjsonOf(rows))
	if err != nil {
		fmt.Fprintln(os.Stderr, "conccheck: decode:", err)
		os.Exit(2)
	}
	if *partition {
		if err := model.CheckPartitionable(ops); err != nil {
			fmt.Fprintln(os.Stderr, "conccheck: cannot partition:", err)
			os.Exit(2)
		}
	}

	pairs, maxConc := overlaps(rows)
	fmt.Printf("loaded %d events   overlapping pairs: %d   max concurrency: %d\n",
		len(rows), pairs, maxConc)
	if maxConc < 2 {
		fmt.Println("  WARNING: nothing overlapped — this history is sequential, so")
		fmt.Println("           porcupine has no ordering freedom to search over.")
	}
	pending := 0
	for _, r := range resps {
		if model.PendingOp(r) {
			pending++
		}
	}
	if pending > 0 {
		fmt.Printf("pending: %d of %d ops answered 500 — each may or may not have applied; the verdict leaves them free\n",
			pending, len(ops))
	}
	fmt.Println()

	hist := make([]porcupine.Operation, len(rows))
	for i, r := range rows {
		hist[i] = porcupine.Operation{ClientId: r.Client, Input: ops[i],
			Call: r.Call, Output: resps[i], Return: r.Return}
	}

	bad := false
	for _, d := range []model.Discipline{model.Projected, model.Materialized} {
		var sat model.Saturation
		nm := model.NondeterministicModel(d, &sat, *partition)
		start := time.Now()
		res, _ := porcupine.CheckOperationsVerbose(nm.ToModel(), hist, *timeout)
		el := time.Since(start).Round(time.Millisecond)

		label := fmt.Sprintf("%-18s", d.String())
		switch {
		case !sat.OK():
			fmt.Printf("%s INCONCLUSIVE  an internal step closure hit the fuel bound  %v\n", label, el)
		case res == porcupine.Ok:
			fmt.Printf("%s LINEARIZABLE  (some order works)  %v\n", label, el)
		case res == porcupine.Illegal:
			fmt.Printf("%s NOT LINEARIZABLE  (NO order works)  %v\n", label, el)
			bad = true
		default:
			fmt.Printf("%s TIMEOUT after %v\n", label, el)
		}

		_, at, ok := model.ReplayPartitioned(d, ops, resps)
		if ok {
			fmt.Printf("                   recorded order alone also explains it\n")
		} else {
			fmt.Printf("                   recorded order alone does NOT: stops at event %d (%v)\n",
				at, ops[at])
			fmt.Printf("                   → the Lean checker will refute this file; that is a\n")
			fmt.Printf("                     statement about the ORDER, not about the server\n")
		}
	}
	if bad {
		os.Exit(1)
	}
}

func readRows(f *os.File) ([]row, error) {
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 1<<20), 1<<24)
	var out []row
	for sc.Scan() {
		if len(sc.Bytes()) == 0 {
			continue
		}
		var r row
		if err := json.Unmarshal(sc.Bytes(), &r); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	sort.SliceStable(out, func(i, j int) bool {
		if out[i].Now != out[j].Now {
			return out[i].Now < out[j].Now
		}
		return out[i].Return < out[j].Return
	})
	return out, sc.Err()
}

func ndjsonOf(rows []row) io.Reader {
	var b bytes.Buffer
	for _, r := range rows {
		line, _ := json.Marshal(map[string]any{
			"kind": r.Kind, "now": r.Now, "req": r.Req, "res": r.Res})
		b.Write(line)
		b.WriteByte('\n')
	}
	return &b
}

func overlaps(rows []row) (pairs, max int) {
	for i := range rows {
		live := 1
		for j := range rows {
			if i == j {
				continue
			}
			if rows[i].Call < rows[j].Return && rows[j].Call < rows[i].Return {
				if j > i {
					pairs++
				}
				live++
			}
		}
		if live > max {
			max = live
		}
	}
	return pairs, max
}
