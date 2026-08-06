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
	ID        string            `json:"id"`
	State     string            `json:"state"`
	TimeoutAt uint64            `json:"timeoutAt"`
	CreatedAt uint64            `json:"createdAt"`
	SettledAt *uint64           `json:"settledAt"`
	Tags      map[string]string `json:"tags"`
	Param     json.RawMessage   `json:"param"`
	Value     json.RawMessage   `json:"value"`
}

type wireTask struct {
	ID      string  `json:"id"`
	State   string  `json:"state"`
	Version uint64  `json:"version"`
	Resumes int     `json:"resumes"`
	TTL     *uint64 `json:"ttl"`
	PID     *string `json:"pid"`
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
		if strings.HasPrefix(e.Kind, "schedule.") && e.Kind != "schedule.search" {
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
			Kind string `json:"kind"`
			Data struct {
				ID        string            `json:"id"`
				TimeoutAt uint64            `json:"timeoutAt"`
				Tags      map[string]string `json:"tags"`
				State     string            `json:"state"`
				Param     json.RawMessage   `json:"param"`
				Value     json.RawMessage   `json:"value"`
			} `json:"data"`
		} `json:"action"`
		Awaited string          `json:"awaited"`
		Awaiter string          `json:"awaiter"`
		Address string          `json:"address"`
		Param   json.RawMessage `json:"param"`
		Value   json.RawMessage `json:"value"`
		Tasks   []struct {
			ID      string `json:"id"`
			Version uint64 `json:"version"`
		} `json:"tasks"`
	}
	if err := json.Unmarshal(e.Req, &r); err != nil {
		return op, err
	}
	op.ID, op.TimeoutAt, op.Version, op.PID, op.TTL = r.ID, r.TimeoutAt, r.Version, r.PID, r.TTL
	op.Tags = Tags(r.Tags)
	op.Param, op.Value = r.Param, r.Value
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
		op.Value = r.Action.Data.Value
	case "task.suspend":
		for _, a := range r.Actions {
			op.Awaited = append(op.Awaited, a.Data.Awaited)
		}
	case "promise.register_callback":
		op.ID, op.Awaiter = r.Awaited, r.Awaiter
	case "task.heartbeat":
		for _, t := range r.Tasks {
			op.Refs = append(op.Refs, TaskRef{ID: t.ID, Version: t.Version})
		}
	case "task.create":
		// Same `{kind, head, data}` envelope as task.fence and task.suspend
		// carry, so the shared `Action` struct decodes it directly.
		op.Action = &FenceAction{Kind: "promise.create", ID: r.Action.Data.ID,
			TimeoutAt: r.Action.Data.TimeoutAt, Tags: Tags(r.Action.Data.Tags),
			Param: r.Action.Data.Param}
		// task.create carries no top-level `id`; the object it creates is
		// the action's. Without this `op.ID` stays empty, `originOf("")`
		// puts every creation in its own partition, and the promise is
		// invisible to the partition that then reads it.
		op.ID = r.Action.Data.ID
		if op.Action.Tags == nil {
			op.Action.Tags = Tags{}
		}
	case "task.fence":
		// The action envelope is `{kind, head, data}`, like task.suspend's
		// and task.fulfill's. `State` is only meaningful for a settle.
		fa := &FenceAction{Kind: r.Action.Kind, ID: r.Action.Data.ID,
			TimeoutAt: r.Action.Data.TimeoutAt, Tags: Tags(r.Action.Data.Tags),
			Param: r.Action.Data.Param, Value: r.Action.Data.Value}
		if fa.Tags == nil {
			fa.Tags = Tags{}
		}
		if r.Action.Kind == "promise.settle" {
			st, ok := promiseStates[r.Action.Data.State]
			if !ok {
				return op, fmt.Errorf("task.fence: unknown promise state %q", r.Action.Data.State)
			}
			fa.State = st
		}
		op.Action = fa
	case "promise.register_listener":
		// The request field is `awaited`, not `id`, and the address is in
		// `address`. Missing this made every recorded listener decode as
		// `PromiseRegisterListener("", "")` — 400 for a bad address where
		// the server had said 404 for a missing promise.
		//
		// The differential fuzzer could not see it: there the Go side uses
		// the generated `Op` values directly and only the LEAN side reads
		// the file, so a bug in this decoder is invisible by construction.
		// It took traffic recorded from a real server to surface.
		op.ID, op.PID = r.Awaited, r.Address
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
		Action  *struct {
			Kind string `json:"kind"`
			Head struct {
				Status int `json:"status"`
			} `json:"head"`
			Data json.RawMessage `json:"data"`
		} `json:"action"`
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
			res.Promise = &Promise{ID: p.ID, State: st, TimeoutAt: p.TimeoutAt,
				CreatedAt: p.CreatedAt, SettledAt: p.SettledAt,
				Tags: Tags(p.Tags), Param: p.Param, Value: p.Value}
		}
	}
	if d.Action != nil {
		inner := &InnerResponse{Kind: d.Action.Kind, Status: d.Action.Head.Status}
		var ad struct {
			Promise json.RawMessage `json:"promise"`
		}
		if err := json.Unmarshal(d.Action.Data, &ad); err == nil && len(ad.Promise) > 0 {
			var p wirePromise
			if err := json.Unmarshal(ad.Promise, &p); err == nil && p.State != "" {
				st, ok := promiseStates[p.State]
				if !ok {
					return res, fmt.Errorf("task.fence action: unknown promise state %q", p.State)
				}
				inner.Promise = &Promise{ID: p.ID, State: st, TimeoutAt: p.TimeoutAt,
					CreatedAt: p.CreatedAt, SettledAt: p.SettledAt,
					Tags: Tags(p.Tags), Param: p.Param, Value: p.Value}
			}
		}
		res.Inner = inner
	}
	if len(d.Task) > 0 {
		var t wireTask
		if err := json.Unmarshal(d.Task, &t); err == nil && t.State != "" {
			st, ok := taskStates[t.State]
			if !ok {
				return res, fmt.Errorf("unknown task state %q", t.State)
			}
			res.Task = &Task{ID: t.ID, State: st, Version: t.Version,
				TTL: t.TTL, PID: t.PID, Resumes: make([]string, t.Resumes)}
		}
	}
	return res, nil
}
