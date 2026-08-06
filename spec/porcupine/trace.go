package model

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"strings"
)

// Loading a recorded run.
//
// The format is resonate's own wire envelope, one event per line, exactly
// as valid/traces/capture.py tees it — so a trace file is teed traffic,
// not a shape invented here. The same files feed the Lean checker, which
// is the point: two independent implementations, one input.

type wireEvent struct {
	Kind string          `json:"kind"`
	Now  uint64          `json:"now"`
	Req  json.RawMessage `json:"req"`
	Res  wireRes         `json:"res"`
}

type wireRes struct {
	Head struct {
		Status int `json:"status"`
	} `json:"head"`
	Data json.RawMessage `json:"data"`
}

type wirePromise struct {
	State     string  `json:"state"`
	SettledAt *uint64 `json:"settledAt"`
}

type wireTask struct {
	State   string `json:"state"`
	Version uint64 `json:"version"`
}

var promiseStates = map[string]PromiseState{
	"pending": Pending, "resolved": Resolved, "rejected": Rejected,
	"rejected_canceled": RejectedCanceled, "rejected_timedout": RejectedTimedout,
}

// resonate's wire names for task states. The model's own names differ
// (`init` is `TaskPending` here, matching the Lean's `.pending`), which is
// exactly the sort of mismatch that silently passes if you never look — so
// the mapping is explicit and an unknown name is an error, not a zero.
var taskStates = map[string]TaskState{
	"init": TaskPending, "pending": TaskPending,
	"claimed": TaskAcquired, "acquired": TaskAcquired,
	"suspended": TaskSuspended, "halted": TaskHalted,
	"completed": TaskFulfilled, "fulfilled": TaskFulfilled,
}

// LoadTrace reads NDJSON into ops and the responses that were observed.
func LoadTrace(r io.Reader) ([]Op, []Response, error) {
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 0, 1<<20), 1<<24)
	var ops []Op
	var resps []Response
	line := 0
	for sc.Scan() {
		text := strings.TrimSpace(sc.Text())
		if text == "" {
			continue
		}
		line++
		var e wireEvent
		if err := json.Unmarshal([]byte(text), &e); err != nil {
			return nil, nil, fmt.Errorf("line %d: %w", line, err)
		}
		if strings.HasPrefix(e.Kind, "schedule.") {
			return nil, nil, fmt.Errorf(
				"line %d: trace mentions %s; `occurrences` and `nextCron` are opaque in the "+
					"specification, so there is no calendar to check against (see valid/schedules.lean)",
				line, e.Kind)
		}
		op, err := decodeReq(e)
		if err != nil {
			return nil, nil, fmt.Errorf("line %d: %w", line, err)
		}
		res, err := decodeRes(e)
		if err != nil {
			return nil, nil, fmt.Errorf("line %d: %w", line, err)
		}
		ops = append(ops, op)
		resps = append(resps, res)
	}
	return ops, resps, sc.Err()
}

func decodeReq(e wireEvent) (Op, error) {
	op := Op{Kind: e.Kind, Now: e.Now}
	var r struct {
		ID        string            `json:"id"`
		TimeoutAt uint64            `json:"timeoutAt"`
		Tags      map[string]string `json:"tags"`
		Version   uint64            `json:"version"`
		PID       string            `json:"pid"`
		TTL       uint64            `json:"ttl"`
		State     string            `json:"state"`
		Actions   []struct {
			Data struct {
				Awaited string `json:"awaited"`
				Awaiter string `json:"awaiter"`
			} `json:"data"`
		} `json:"actions"`
		Action struct {
			Data struct {
				ID    string `json:"id"`
				State string `json:"state"`
			} `json:"data"`
		} `json:"action"`
		Awaited string `json:"awaited"`
		Awaiter string `json:"awaiter"`
	}
	if err := json.Unmarshal(e.Req, &r); err != nil {
		return op, err
	}
	op.ID, op.TimeoutAt, op.Version, op.PID, op.TTL = r.ID, r.TimeoutAt, r.Version, r.PID, r.TTL
	op.Tags = Tags(r.Tags)
	if op.Tags == nil {
		op.Tags = Tags{}
	}
	switch e.Kind {
	case "promise.settle":
		st, ok := promiseStates[r.State]
		if !ok {
			return op, fmt.Errorf("unknown promise state %q", r.State)
		}
		op.State = st
	case "task.fulfill":
		st, ok := promiseStates[r.Action.Data.State]
		if !ok {
			return op, fmt.Errorf("unknown promise state %q", r.Action.Data.State)
		}
		op.State = st
	case "task.suspend":
		for _, a := range r.Actions {
			op.Awaited = append(op.Awaited, a.Data.Awaited)
		}
	case "promise.register_callback":
		op.ID, op.Awaiter = r.Awaited, r.Awaiter
	}
	return op, nil
}

func decodeRes(e wireEvent) (Response, error) {
	res := Response{Status: e.Res.Head.Status}
	if res.Status/100 != 2 && res.Status != 300 {
		// a non-2xx carries an error string, not a record
		return res, nil
	}
	var d struct {
		Promise json.RawMessage `json:"promise"`
		Task    json.RawMessage `json:"task"`
	}
	if len(e.Res.Data) == 0 {
		return res, nil
	}
	if err := json.Unmarshal(e.Res.Data, &d); err != nil {
		return res, nil // data is not an object (e.g. a bare string)
	}
	if len(d.Promise) > 0 {
		var p wirePromise
		if err := json.Unmarshal(d.Promise, &p); err == nil && p.State != "" {
			st, ok := promiseStates[p.State]
			if !ok {
				return res, fmt.Errorf("unknown promise state %q", p.State)
			}
			res.PromiseState = &st
			res.SettledAt = p.SettledAt
		}
	}
	if len(d.Task) > 0 {
		var t wireTask
		if err := json.Unmarshal(d.Task, &t); err == nil && t.State != "" {
			st, ok := taskStates[t.State]
			if !ok {
				return res, fmt.Errorf("unknown task state %q", t.State)
			}
			v := t.Version
			res.TaskState = &st
			res.TaskVersion = &v
		}
	}
	return res, nil
}
