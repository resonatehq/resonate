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
	searchSet          []string
	callback           uint64
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

		tagsSet = append(tagsSet, tags, nil) // half of all tags are nil
	}

	searchSet := make([]string, config.Searches*2)
	for i := 0; i < config.Searches*2; i = i + 2 {
		searchSet[i+0] = fmt.Sprintf("*%d", i)
		searchSet[i+1] = fmt.Sprintf("%d*", i)
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
		searchSet:          searchSet,
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

			// k := RangeMap(r, g.generators)

			// if len(g.requests[k]) == 0 {
			// 	reqs[i] = g.generators[k](r, t)
			// } else {
			// 	j := r.Intn(len(g.requests[k]))
			// 	reqs[i] = g.requests[k][j]
			// 	g.requests[k] = append(g.requests[k][:j], g.requests[k][j+1:]...)
			// }
		}
	}

	return reqs
}

// PROMISES

func (g *Generator) GenerateReadPromise(r *rand.Rand, t int64) *t_api.Request {
	id := g.idSet[r.Intn(len(g.idSet))]

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

	id := g.searchSet[r.Intn(len(g.searchSet))]
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
	id := g.idSet[r.Intn(len(g.idSet))]

	idempotencyKey := g.idemotencyKeySet[r.Intn(len(g.idemotencyKeySet))]
	data := g.dataSet[r.Intn(len(g.dataSet))]
	headers := g.headersSet[r.Intn(len(g.headersSet))]
	tags := g.tagsSet[r.Intn(len(g.tagsSet))]
	timeout := RangeInt63n(r, t, t+(g.timeoutTicks*g.timeElapsedPerTick))
	strict := r.Intn(2) == 0

	return &t_api.Request{
		Kind: t_api.CreatePromise,
		CreatePromise: &t_api.CreatePromiseRequest{
			Id: id,
			Param: promise.Value{
				Headers: headers,
				Data:    data,
			},
			Timeout:        timeout,
			IdempotencyKey: idempotencyKey,
			Tags:           tags,
			Strict:         strict,
		},
	}
}

func (g *Generator) GenerateCompletePromise(r *rand.Rand, t int64) *t_api.Request {
	id := g.idSet[r.Intn(len(g.idSet))]
	idempotencyKey := g.idemotencyKeySet[r.Intn(len(g.idemotencyKeySet))]
	data := g.dataSet[r.Intn(len(g.dataSet))]
	headers := g.headersSet[r.Intn(len(g.headersSet))]
	strict := r.Intn(2) == 0
	state := promise.State(math.Exp2(float64(r.Intn(3) + 1)))

	return &t_api.Request{
		Kind: t_api.CompletePromise,
		CompletePromise: &t_api.CompletePromiseRequest{
			Id:    id,
			State: state,
			Value: promise.Value{
				Headers: headers,
				Data:    data,
			},
			IdempotencyKey: idempotencyKey,
			Strict:         strict,
		},
	}
}

// CALLBACKS

func (g *Generator) GenerateCreateCallback(r *rand.Rand, t int64) *t_api.Request {
	promiseId := g.idSet[r.Intn(len(g.idSet))]
	rootPromiseId := g.idSet[r.Intn(len(g.idSet))]
	timeout := RangeInt63n(r, t, g.ticks*g.timeElapsedPerTick)
	g.callback++

	return &t_api.Request{
		Kind: t_api.CreateCallback,
		CreateCallback: &t_api.CreateCallbackRequest{
			PromiseId:     promiseId,
			RootPromiseId: rootPromiseId,
			// RecvType:      "http",
			// RecvData:      nil,
			Timeout: timeout,
		},
	}
}

// SCHEDULES

func (g *Generator) GenerateReadSchedule(r *rand.Rand, t int64) *t_api.Request {
	id := g.idSet[r.Intn(len(g.idSet))]

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

	id := g.searchSet[r.Intn(len(g.searchSet))]
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
	id := g.idSet[r.Intn(len(g.idSet))]
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
	id := g.idSet[r.Intn(len(g.idSet))]

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
	expiryInMilliseconds := RangeInt63n(r, 0, max(1, (g.ticks*g.timeElapsedPerTick)/100))

	return &t_api.Request{
		Kind: t_api.AcquireLock,
		AcquireLock: &t_api.AcquireLockRequest{
			ResourceId:           resourceId,
			ExecutionId:          executionId,
			ProcessId:            processId,
			ExpiryInMilliseconds: expiryInMilliseconds,
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
		switch r.Intn(3) {
		case 0:
			g.AddRequest(&t_api.Request{
				Kind: t_api.ClaimTask,
				ClaimTask: &t_api.ClaimTaskRequest{
					Id:        id,
					ProcessId: pid,
					Counter:   counter,
					Frequency: RangeIntn(r, 1000, 5000),
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
		}
	}
}
