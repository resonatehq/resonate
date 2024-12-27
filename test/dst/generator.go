package dst

import (
	"fmt"
	"math"
	"math/rand" // nosemgrep
	"strconv"

	"github.com/resonatehq/resonate/internal/kernel/t_api"
	"github.com/resonatehq/resonate/pkg/idempotency"
	"github.com/resonatehq/resonate/pkg/promise"
)

type Generator struct {
	ticks              int64
	timeElapsedPerTick int64
	timeoutTicks       int64 // max ticks in the future to set promise timeout
	idSet              []string
	idemotencyKeySet   []*idempotency.Key
	headersSet         []map[string]string
	dataSet            [][]byte
	tagsSet            []map[string]string
	requests           map[t_api.Kind][]*t_api.Request
	generators         []RequestGenerator
}

type RequestGenerator func(*rand.Rand, int64) *t_api.Request

func NewGenerator(r *rand.Rand, config *Config) *Generator {
	idSet := make([]string, config.Ids)
	for i := 0; i < config.Ids; i++ {
		idSet[i] = strconv.Itoa(i)
	}

	idempotencyKeySet := []*idempotency.Key{}
	for i := 0; i < config.IdempotencyKeys; i++ {
		s := strconv.Itoa(i)
		idempotencyKey := idempotency.Key(s)
		idempotencyKeySet = append(idempotencyKeySet, &idempotencyKey, nil) // half of all idempotencyKeys are nil
	}

	headersSet := []map[string]string{}
	for i := 0; i < config.Headers; i++ {
		headers := map[string]string{}
		for j := 0; j < r.Intn(3)+1; j++ {
			headers[strconv.Itoa(j)] = fmt.Sprintf("%d.%d", i, j)
		}

		headersSet = append(headersSet, headers, nil) // half of all headers are nil
	}

	dataSet := [][]byte{}
	for i := 0; i < config.Data; i++ {
		dataSet = append(dataSet, []byte(strconv.Itoa(i)), nil) // half of all values are nil
	}

	tagsSet := []map[string]string{}
	for i := 0; i < config.Tags; i++ {
		tags := map[string]string{}
		for j := 0; j < r.Intn(4); j++ {
			tags[strconv.Itoa(j)] = fmt.Sprintf("%d.%d", i, j)
		}

		if r.Intn(2) == 0 {
			tags["resonate:timeout"] = "true" // transition to resolved on timeout
		}

		if r.Intn(2) == 0 {
			tags["resonate:invoke"] = "dst" // create a task
		}

		tagsSet = append(tagsSet, tags, nil) // half of all tags are nil
	}

	return &Generator{
		ticks:              config.Ticks,
		timeElapsedPerTick: config.TimeElapsedPerTick,
		timeoutTicks:       config.TimeoutTicks,
		idSet:              idSet,
		idemotencyKeySet:   idempotencyKeySet,
		headersSet:         headersSet,
		dataSet:            dataSet,
		tagsSet:            tagsSet,
		requests:           map[t_api.Kind][]*t_api.Request{},
		generators:         []RequestGenerator{},
	}
}

func (g *Generator) AddGenerator(kind t_api.Kind, generator RequestGenerator) {
	g.generators = append(g.generators, generator)
}

func (g *Generator) AddRequest(req *t_api.Request) {
	g.requests[req.Kind] = append(g.requests[req.Kind], req)
}

func (g *Generator) Generate(r *rand.Rand, t int64, n int) []*t_api.Request {
	reqs := make([]*t_api.Request, n)

	for i := 0; i < n; i++ {
		// some generators require "canned requests" and return nil if none
		// are available, loop until we get a non-nil request
		for reqs[i] == nil {
			f := g.generators[r.Intn(len(g.generators))]
			reqs[i] = f(r, t)
		}
	}

	return reqs
}

// PROMISES

func (g *Generator) GenerateReadPromise(r *rand.Rand, t int64) *t_api.Request {
	id := g.promiseId(r)

	return &t_api.Request{
		Kind: t_api.ReadPromise,
		ReadPromise: &t_api.ReadPromiseRequest{
			Id: id,
		},
	}
}

func (g *Generator) GenerateSearchPromises(r *rand.Rand, t int64) *t_api.Request {
	// grab a cursor if one is available
	if req := g.pop(r, t_api.SearchPromises); req != nil {
		return req
	}

	id := g.promiseSearch(r)
	limit := RangeIntn(r, 1, 11)
	tags := g.tagsSet[r.Intn(len(g.tagsSet))]
	states := []promise.State{}

	// states
	for i := 0; i < r.Intn(5); i++ {
		switch r.Intn(5) {
		case 0:
			states = append(states, promise.Pending)
		case 1:
			states = append(states, promise.Resolved)
		case 2:
			states = append(states, promise.Rejected)
		case 3:
			states = append(states, promise.Timedout)
		case 4:
			states = append(states, promise.Canceled)
		}
	}

	return &t_api.Request{
		Kind: t_api.SearchPromises,
		SearchPromises: &t_api.SearchPromisesRequest{
			Id:     id,
			States: states,
			Tags:   tags,
			Limit:  limit,
		},
	}
}

func (g *Generator) GenerateCreatePromise(r *rand.Rand, t int64) *t_api.Request {
	id := g.promiseId(r)
	idempotencyKey := g.idemotencyKeySet[r.Intn(len(g.idemotencyKeySet))]
	strict := r.Intn(2) == 0
	headers := g.headersSet[r.Intn(len(g.headersSet))]
	data := g.dataSet[r.Intn(len(g.dataSet))]
	timeout := RangeInt63n(r, t, t+(g.timeoutTicks*g.timeElapsedPerTick))
	tags := g.tagsSet[r.Intn(len(g.tagsSet))]

	return &t_api.Request{
		Kind: t_api.CreatePromise,
		CreatePromise: &t_api.CreatePromiseRequest{
			Id:             id,
			IdempotencyKey: idempotencyKey,
			Strict:         strict,
			Param:          promise.Value{Headers: headers, Data: data},
			Timeout:        timeout,
			Tags:           tags,
		},
	}
}

func (g *Generator) GenerateCreatePromiseAndTask(r *rand.Rand, t int64) *t_api.Request {
	req := g.GenerateCreatePromise(r, t)
	if req.CreatePromise.Tags == nil {
		req.CreatePromise.Tags = map[string]string{"resonate:invoke": "dst"}
	} else {
		req.CreatePromise.Tags["resonate:invoke"] = "dst"
	}

	return &t_api.Request{
		Kind: t_api.CreatePromiseAndTask,
		CreatePromiseAndTask: &t_api.CreatePromiseAndTaskRequest{
			Promise: req.CreatePromise,
			Task: &t_api.CreateTaskRequest{
				PromiseId: req.CreatePromise.Id,
				ProcessId: req.CreatePromise.Id,
				Ttl:       RangeIntn(r, 1000, 5000),
				Timeout:   req.CreatePromise.Timeout,
			},
		},
	}
}

func (g *Generator) GenerateCompletePromise(r *rand.Rand, t int64) *t_api.Request {
	id := g.promiseId(r)
	idempotencyKey := g.idemotencyKeySet[r.Intn(len(g.idemotencyKeySet))]
	strict := r.Intn(2) == 0
	state := promise.State(math.Exp2(float64(r.Intn(3) + 1)))
	headers := g.headersSet[r.Intn(len(g.headersSet))]
	data := g.dataSet[r.Intn(len(g.dataSet))]

	return &t_api.Request{
		Kind: t_api.CompletePromise,
		CompletePromise: &t_api.CompletePromiseRequest{
			Id:             id,
			IdempotencyKey: idempotencyKey,
			Strict:         strict,
			State:          state,
			Value:          promise.Value{Headers: headers, Data: data},
		},
	}
}

// CALLBACKS

func (g *Generator) GenerateCreateCallback(r *rand.Rand, t int64) *t_api.Request {
	promiseId := g.promiseId(r)
	rootPromiseId := g.promiseId(r)
	timeout := RangeInt63n(r, t, g.ticks*g.timeElapsedPerTick)
	id := g.callbackId(r)

	return &t_api.Request{
		Kind: t_api.CreateCallback,
		CreateCallback: &t_api.CreateCallbackRequest{
			Id:            id,
			PromiseId:     promiseId,
			RootPromiseId: rootPromiseId,
			Timeout:       timeout,
			Recv:          []byte(`"dst"`), // ignored in dst, use hardcoded value
		},
	}
}

// SCHEDULES

func (g *Generator) GenerateReadSchedule(r *rand.Rand, t int64) *t_api.Request {
	id := g.scheduleId(r)

	return &t_api.Request{
		Kind: t_api.ReadSchedule,
		ReadSchedule: &t_api.ReadScheduleRequest{
			Id: id,
		},
	}
}

func (g *Generator) GenerateSearchSchedules(r *rand.Rand, t int64) *t_api.Request {
	// grab a cursor if one is available
	if req := g.pop(r, t_api.SearchSchedules); req != nil {
		return req
	}

	id := g.scheduleSearch(r)
	limit := RangeIntn(r, 1, 11)
	tags := g.tagsSet[r.Intn(len(g.tagsSet))]

	return &t_api.Request{
		Kind: t_api.SearchSchedules,
		SearchSchedules: &t_api.SearchSchedulesRequest{
			Id:    id,
			Tags:  tags,
			Limit: limit,
		},
	}
}

func (g *Generator) GenerateCreateSchedule(r *rand.Rand, t int64) *t_api.Request {
	id := g.scheduleId(r)
	cron := fmt.Sprintf("%d * * * *", r.Intn(60))
	tags := g.tagsSet[r.Intn(len(g.tagsSet))]
	idempotencyKey := g.idemotencyKeySet[r.Intn(len(g.idemotencyKeySet))]

	promiseTimeout := RangeInt63n(r, t, g.ticks*g.timeElapsedPerTick)
	promiseHeaders := g.headersSet[r.Intn(len(g.headersSet))]
	promiseData := g.dataSet[r.Intn(len(g.dataSet))]
	promiseTags := g.tagsSet[r.Intn(len(g.tagsSet))]

	return &t_api.Request{
		Kind: t_api.CreateSchedule,
		CreateSchedule: &t_api.CreateScheduleRequest{
			Id:             id,
			Description:    "",
			Cron:           cron,
			Tags:           tags,
			PromiseId:      fmt.Sprintf("%s.{{.timestamp}}", id),
			PromiseTimeout: promiseTimeout,
			PromiseParam:   promise.Value{Headers: promiseHeaders, Data: promiseData},
			PromiseTags:    promiseTags,
			IdempotencyKey: idempotencyKey,
		},
	}
}

func (g *Generator) GenerateDeleteSchedule(r *rand.Rand, t int64) *t_api.Request {
	id := g.scheduleId(r)

	return &t_api.Request{
		Kind: t_api.DeleteSchedule,
		DeleteSchedule: &t_api.DeleteScheduleRequest{
			Id: id,
		},
	}
}

// LOCKS

func (g *Generator) GenerateAcquireLock(r *rand.Rand, t int64) *t_api.Request {
	resourceId := g.idSet[r.Intn(len(g.idSet))]
	executionId := g.idSet[r.Intn(len(g.idSet))]
	processId := g.idSet[r.Intn(len(g.idSet))]
	ttl := RangeInt63n(r, 0, max(1, (g.ticks*g.timeElapsedPerTick)/100))

	return &t_api.Request{
		Kind: t_api.AcquireLock,
		AcquireLock: &t_api.AcquireLockRequest{
			ResourceId:  resourceId,
			ExecutionId: executionId,
			ProcessId:   processId,
			Ttl:         ttl,
		},
	}
}

func (g *Generator) GenerateReleaseLock(r *rand.Rand, t int64) *t_api.Request {
	resourceId := g.idSet[r.Intn(len(g.idSet))]
	executionId := g.idSet[r.Intn(len(g.idSet))]

	return &t_api.Request{
		Kind: t_api.ReleaseLock,
		ReleaseLock: &t_api.ReleaseLockRequest{
			ResourceId:  resourceId,
			ExecutionId: executionId,
		},
	}
}

func (g *Generator) GenerateHeartbeatLocks(r *rand.Rand, t int64) *t_api.Request {
	processId := g.idSet[r.Intn(len(g.idSet))]

	return &t_api.Request{
		Kind: t_api.HeartbeatLocks,
		HeartbeatLocks: &t_api.HeartbeatLocksRequest{
			ProcessId: processId,
		},
	}
}

// TASKS

func (g *Generator) GenerateClaimTask(r *rand.Rand, t int64) *t_api.Request {
	req := g.pop(r, t_api.ClaimTask)

	if req != nil {
		g.nextTasks(r, req.ClaimTask.Id, req.ClaimTask.ProcessId, req.ClaimTask.Counter)
	}

	return req
}

func (g *Generator) GenerateCompleteTask(r *rand.Rand, t int64) *t_api.Request {
	return g.pop(r, t_api.CompleteTask)
}

func (g *Generator) GenerateHeartbeatTasks(r *rand.Rand, t int64) *t_api.Request {
	return g.pop(r, t_api.HeartbeatTasks)
}

// Helpers

func (g *Generator) promiseId(r *rand.Rand) string {
	return "p" + g.idSet[r.Intn(len(g.idSet))]
}

func (g *Generator) scheduleId(r *rand.Rand) string {
	return "s" + g.idSet[r.Intn(len(g.idSet))]
}

func (g *Generator) callbackId(r *rand.Rand) string {
	return "cb" + g.idSet[r.Intn(len(g.idSet))]
}

func (g *Generator) promiseSearch(r *rand.Rand) string {
	return "p" + g.search(r)
}

func (g *Generator) scheduleSearch(r *rand.Rand) string {
	return "s" + g.search(r)
}

func (g *Generator) search(r *rand.Rand) string {
	switch r.Intn(2) {
	case 0:
		return fmt.Sprintf("*%d", r.Intn(10))
	default:
		return fmt.Sprintf("%d*", r.Intn(10))
	}
}

func (g *Generator) pop(r *rand.Rand, kind t_api.Kind) *t_api.Request {
	reqs := g.requests[kind]

	if len(reqs) == 0 {
		return nil
	}

	i := r.Intn(len(reqs))
	req := reqs[i]
	g.requests[kind] = append(reqs[:i], reqs[i+1:]...)

	return req
}

func (g *Generator) nextTasks(r *rand.Rand, id string, pid string, counter int) {
	// seed the "next" requests,
	// sometimes we deliberately do nothing
	for i := 0; i < r.Intn(3); i++ {
		switch r.Intn(4) {
		case 0:
			g.AddRequest(&t_api.Request{
				Kind: t_api.ClaimTask,
				ClaimTask: &t_api.ClaimTaskRequest{
					Id:        id,
					ProcessId: pid,
					Counter:   counter,
					Ttl:       RangeIntn(r, 1000, 5000),
				},
			})
		case 1:
			g.AddRequest(&t_api.Request{
				Kind: t_api.CompleteTask,
				CompleteTask: &t_api.CompleteTaskRequest{
					Id:      id,
					Counter: counter,
				},
			})
		case 2:
			g.AddRequest(&t_api.Request{
				Kind: t_api.HeartbeatTasks,
				HeartbeatTasks: &t_api.HeartbeatTasksRequest{
					ProcessId: pid,
				},
			})
		case 3:
			// do nothing
		}
	}
}
