package dst

import (
	"fmt"
	"maps"
	"math"
	"math/rand" // nosemgrep
	"strconv"

	cmdUtil "github.com/resonatehq/resonate/cmd/util"
	"github.com/resonatehq/resonate/internal/kernel/t_api"
	"github.com/resonatehq/resonate/internal/util"
	"github.com/resonatehq/resonate/pkg/message"
	"github.com/resonatehq/resonate/pkg/promise"
)

type Generator struct {
	ticks              int64
	timeElapsedPerTick int64
	timeoutTicks       int64 // max ticks in the future to set promise timeout
	idSet              []string
	headersSet         []map[string]string
	dataSet            [][]byte
	tagsSet            []map[string]string
	requests           map[t_api.Kind][]*t_api.Request
	generators         []RequestGenerator
}

type RequestGenerator func(*rand.Rand, int64) *t_api.Request

func NewGenerator(r *rand.Rand, config *Config) *Generator {
	idSet := make([]string, config.Ids)
	width := len(strconv.Itoa(config.Ids))

	for i := 0; i < config.Ids; i++ {
		// pad ids with leading zeros to ensure ids are the same length
		// this helps with lexigraphical sorting across different databases
		idSet[i] = fmt.Sprintf(fmt.Sprintf("%%0%dd", width), i)
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
	g.requests[req.Kind()] = append(g.requests[req.Kind()], req)
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
		Metadata: map[string]string{"partitionId": id},
		Payload: &t_api.ReadPromiseRequest{
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
	limit := cmdUtil.RangeIntn(r, 1, 11)
	tags := g.tags(r)
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
		Payload: &t_api.SearchPromisesRequest{
			Id:     id,
			States: states,
			Tags:   tags,
			Limit:  limit,
		},
	}
}

func (g *Generator) GenerateCreatePromise(r *rand.Rand, t int64) *t_api.Request {
	id := g.promiseId(r)
	headers := g.headers(r)
	data := g.dataSet[r.Intn(len(g.dataSet))]
	timeout := cmdUtil.RangeInt63n(r, t, t+(g.timeoutTicks*g.timeElapsedPerTick))
	tags := g.tags(r)

	return &t_api.Request{
		Metadata: map[string]string{"partitionId": id},
		Payload: &t_api.CreatePromiseRequest{
			Id:      id,
			Param:   promise.Value{Headers: headers, Data: data},
			Timeout: timeout,
			Tags:    tags,
		},
	}
}

func (g *Generator) GenerateCreatePromiseAndTask(r *rand.Rand, t int64) *t_api.Request {
	req := g.GenerateCreatePromise(r, t)
	createPromiseReq := req.Payload.(*t_api.CreatePromiseRequest)
	if createPromiseReq.Tags == nil {
		createPromiseReq.Tags = map[string]string{"resonate:invoke": "dst"}
	} else {
		createPromiseReq.Tags["resonate:invoke"] = "dst"
	}

	return &t_api.Request{
		Metadata: map[string]string{"partitionId": createPromiseReq.Id},
		Payload: &t_api.CreatePromiseAndTaskRequest{
			Promise: createPromiseReq,
			Task: &t_api.CreateTaskRequest{
				PromiseId: createPromiseReq.Id,
				ProcessId: createPromiseReq.Id,
				Ttl:       cmdUtil.Choose(r, 1000, 2000, 3000, 4000, 5000, int64(math.MaxInt64)),
				Timeout:   createPromiseReq.Timeout,
			},
		},
	}
}

func (g *Generator) GenerateCompletePromise(r *rand.Rand, t int64) *t_api.Request {
	id := g.promiseId(r)
	state := promise.State(math.Exp2(float64(r.Intn(3) + 1)))
	headers := g.headers(r)
	data := g.dataSet[r.Intn(len(g.dataSet))]

	return &t_api.Request{
		Metadata: map[string]string{"partitionId": id},
		Payload: &t_api.CompletePromiseRequest{
			Id:    id,
			State: state,
			Value: promise.Value{Headers: headers, Data: data},
		},
	}
}

// CALLBACKS

func (g *Generator) GenerateCreateCallback(r *rand.Rand, t int64) *t_api.Request {
	promiseId := g.promiseId(r)
	rootPromiseId := g.promiseId(r)
	timeout := cmdUtil.RangeInt63n(r, t, g.ticks*g.timeElapsedPerTick)

	switch r.Intn(2) {
	case 0:
		return &t_api.Request{
			Metadata: map[string]string{"partitionId": promiseId},
			Payload: &t_api.CreateCallbackRequest{
				Id:        fmt.Sprintf("__resume:%s:%s", rootPromiseId, promiseId),
				PromiseId: promiseId,
				Recv:      []byte(`"dst"`), // ignored in dst, use hardcoded value
				Mesg:      &message.Mesg{Type: "resume", Root: rootPromiseId, Leaf: promiseId},
				Timeout:   timeout,
			},
		}
	default:
		return &t_api.Request{
			Metadata: map[string]string{"partitionId": promiseId},
			Payload: &t_api.CreateCallbackRequest{
				Id:        fmt.Sprintf("__notify:%s:%s", promiseId, g.callbackId(r)),
				PromiseId: promiseId,
				Recv:      []byte(`"dst"`), // ignored in dst, use hardcoded value
				Mesg:      &message.Mesg{Type: "notify", Root: promiseId},
				Timeout:   timeout,
			},
		}
	}
}

// SCHEDULES

func (g *Generator) GenerateReadSchedule(r *rand.Rand, t int64) *t_api.Request {
	id := g.scheduleId(r)

	return &t_api.Request{
		Metadata: map[string]string{"partitionId": id},
		Payload: &t_api.ReadScheduleRequest{
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
	limit := cmdUtil.RangeIntn(r, 1, 11)
	tags := g.tags(r)

	return &t_api.Request{
		Payload: &t_api.SearchSchedulesRequest{
			Id:    id,
			Tags:  tags,
			Limit: limit,
		},
	}
}

func (g *Generator) GenerateCreateSchedule(r *rand.Rand, t int64) *t_api.Request {
	id := g.scheduleId(r)
	cron := fmt.Sprintf("%d * * * *", r.Intn(60))
	tags := g.tags(r)

	promiseTimeout := cmdUtil.RangeInt63n(r, t, g.ticks*g.timeElapsedPerTick)
	promiseHeaders := g.headers(r)
	promiseData := g.dataSet[r.Intn(len(g.dataSet))]
	promiseTags := g.tags(r)

	return &t_api.Request{
		Metadata: map[string]string{"partitionId": id},
		Payload: &t_api.CreateScheduleRequest{
			Id:             id,
			Description:    "",
			Cron:           cron,
			Tags:           tags,
			PromiseId:      fmt.Sprintf("%s.{{.timestamp}}", id),
			PromiseTimeout: promiseTimeout,
			PromiseParam:   promise.Value{Headers: promiseHeaders, Data: promiseData},
			PromiseTags:    promiseTags,
		},
	}
}

func (g *Generator) GenerateDeleteSchedule(r *rand.Rand, t int64) *t_api.Request {
	id := g.scheduleId(r)

	return &t_api.Request{
		Metadata: map[string]string{"partitionId": id},
		Payload: &t_api.DeleteScheduleRequest{
			Id: id,
		},
	}
}

// TASKS

func (g *Generator) GenerateClaimTask(r *rand.Rand, t int64) *t_api.Request {
	req := g.pop(r, t_api.ClaimTask)

	if req != nil {
		claimTaskReq := req.Payload.(*t_api.ClaimTaskRequest)
		partitionId, ok := req.Metadata["partitionId"]
		util.Assert(ok, "req metadata must contain partitionId")

		g.nextTasks(r,
			claimTaskReq.Id,
			claimTaskReq.ProcessId,
			claimTaskReq.Counter,
			partitionId)
	}

	return req
}

func (g *Generator) GenerateCompleteTask(r *rand.Rand, t int64) *t_api.Request {
	return g.pop(r, t_api.CompleteTask)
}

func (g *Generator) GenerateDropTask(r *rand.Rand, t int64) *t_api.Request {
	return g.pop(r, t_api.DropTask)
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

func (g *Generator) nextTasks(r *rand.Rand, id string, pid string, counter int, partitionId string) {
	// seed the "next" requests,
	// sometimes we deliberately do nothing
	for i := 0; i < r.Intn(3); i++ {
		switch r.Intn(5) {
		case 0:
			g.AddRequest(&t_api.Request{
				Metadata: map[string]string{"partitionId": partitionId},
				Payload: &t_api.ClaimTaskRequest{
					Id:        id,
					ProcessId: pid,
					Counter:   counter,
					Ttl:       cmdUtil.Choose(r, 1000, 2000, 3000, 4000, 5000, int64(math.MaxInt64)),
				},
			})
		case 1:
			g.AddRequest(&t_api.Request{
				Metadata: map[string]string{"partitionId": partitionId},
				Payload: &t_api.CompleteTaskRequest{
					Id:      id,
					Counter: counter,
				},
			})
		case 2:
			g.AddRequest(&t_api.Request{
				Metadata: map[string]string{"partitionId": partitionId},
				Payload: &t_api.DropTaskRequest{
					Id:      id,
					Counter: counter,
				},
			})
		case 3:
			g.AddRequest(&t_api.Request{
				Metadata: map[string]string{"partitionId": partitionId},
				Payload: &t_api.HeartbeatTasksRequest{
					ProcessId: pid,
				},
			})
		case 4:
			// do nothing
		}
	}
}

func (g *Generator) tags(r *rand.Rand) map[string]string {
	tags := g.tagsSet[r.Intn(len(g.tagsSet))]
	return maps.Clone(tags)
}

func (g *Generator) headers(r *rand.Rand) map[string]string {
	headers := g.headersSet[r.Intn(len(g.headersSet))]
	return maps.Clone(headers)
}
