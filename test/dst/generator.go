package dst

import (
	"fmt"
	"math/rand" // nosemgrep
	"strconv"

	"github.com/resonatehq/resonate/internal/kernel/t_api"
	"github.com/resonatehq/resonate/pkg/idempotency"
	"github.com/resonatehq/resonate/pkg/promise"
	"github.com/resonatehq/resonate/pkg/subscription"
)

type Generator struct {
	ticks              int64
	timeElapsedPerTick int64
	idSet              []string
	idemotencyKeySet   []*idempotency.Key
	headersSet         []map[string]string
	dataSet            [][]byte
	tagsSet            []map[string]string
	urlSet             []string
	retrySet           []int
	requests           []RequestGenerator
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

		tagsSet = append(tagsSet, tags, nil) // half of all tags are nil
	}

	retrySet := make([]int, config.Retries)
	for i := 0; i < config.Retries; i++ {
		retrySet[i] = i
	}

	urlSet := make([]string, config.Urls)
	for i := 0; i < config.Urls; i++ {
		urlSet[i] = fmt.Sprintf("https://resonatehq.io/%d", i)
	}

	return &Generator{
		ticks:              config.Ticks,
		timeElapsedPerTick: config.TimeElapsedPerTick,
		idSet:              idSet,
		idemotencyKeySet:   idempotencyKeySet,
		headersSet:         headersSet,
		dataSet:            dataSet,
		tagsSet:            tagsSet,
		urlSet:             urlSet,
		retrySet:           retrySet,
	}
}

func (g *Generator) AddRequest(request RequestGenerator) {
	g.requests = append(g.requests, request)
}

func (g *Generator) Generate(r *rand.Rand, t int64, n int, cursors []*t_api.Request) []*t_api.Request {
	reqs := []*t_api.Request{}

	for i := 0; i < n; i++ {
		bound := len(g.requests)
		if len(cursors) > 0 {
			bound = bound + 1
		}

		switch j := r.Intn(bound); j {
		case len(g.requests):
			reqs = append(reqs, cursors[r.Intn(len(cursors))])
		default:
			f := g.requests[j]
			reqs = append(reqs, f(r, t))
		}

	}

	return reqs
}

// PROMISE

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
	limit := RangeIntn(r, 1, 11)
	states := []promise.State{}

	var id string
	switch r.Intn(2) {
	case 0:
		id = fmt.Sprintf("*%d", r.Intn(10))
	default:
		id = fmt.Sprintf("%d*", r.Intn(10))
	}

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

	// tags
	tags := g.tagsSet[r.Intn(len(g.tagsSet))]

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
	timeout := RangeInt63n(r, t, g.ticks*g.timeElapsedPerTick)
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

func (g *Generator) GenerateCancelPromise(r *rand.Rand, t int64) *t_api.Request {
	id := g.idSet[r.Intn(len(g.idSet))]
	idempotencyKey := g.idemotencyKeySet[r.Intn(len(g.idemotencyKeySet))]
	data := g.dataSet[r.Intn(len(g.dataSet))]
	headers := g.headersSet[r.Intn(len(g.headersSet))]
	strict := r.Intn(2) == 0

	return &t_api.Request{
		Kind: t_api.CancelPromise,
		CancelPromise: &t_api.CancelPromiseRequest{
			Id: id,
			Value: promise.Value{
				Headers: headers,
				Data:    data,
			},
			IdempotencyKey: idempotencyKey,
			Strict:         strict,
		},
	}
}

func (g *Generator) GenerateResolvePromise(r *rand.Rand, t int64) *t_api.Request {
	id := g.idSet[r.Intn(len(g.idSet))]
	idempotencyKey := g.idemotencyKeySet[r.Intn(len(g.idemotencyKeySet))]
	data := g.dataSet[r.Intn(len(g.dataSet))]
	headers := g.headersSet[r.Intn(len(g.headersSet))]
	strict := r.Intn(2) == 0

	return &t_api.Request{
		Kind: t_api.ResolvePromise,
		ResolvePromise: &t_api.ResolvePromiseRequest{
			Id: id,
			Value: promise.Value{
				Headers: headers,
				Data:    data,
			},
			IdempotencyKey: idempotencyKey,
			Strict:         strict,
		},
	}
}

func (g *Generator) GenerateRejectPromise(r *rand.Rand, t int64) *t_api.Request {
	id := g.idSet[r.Intn(len(g.idSet))]
	idempotencyKey := g.idemotencyKeySet[r.Intn(len(g.idemotencyKeySet))]
	data := g.dataSet[r.Intn(len(g.dataSet))]
	headers := g.headersSet[r.Intn(len(g.headersSet))]
	strict := r.Intn(2) == 0

	return &t_api.Request{
		Kind: t_api.RejectPromise,
		RejectPromise: &t_api.RejectPromiseRequest{
			Id: id,
			Value: promise.Value{
				Headers: headers,
				Data:    data,
			},
			IdempotencyKey: idempotencyKey,
			Strict:         strict,
		},
	}
}

// SCHEDULE

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
	limit := RangeIntn(r, 1, 11)

	var id string
	switch r.Intn(2) {
	case 0:
		id = fmt.Sprintf("*%d", r.Intn(10))
	default:
		id = fmt.Sprintf("%d*", r.Intn(10))
	}

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
	cron := fmt.Sprintf("%d * * * *", RangeIntn(r, 10, 60))
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

func (g *Generator) GenerateReadSubscriptions(r *rand.Rand, t int64) *t_api.Request {
	limit := r.Intn(10)
	id := g.idSet[r.Intn(len(g.idSet))]

	return &t_api.Request{
		Kind: t_api.ReadSubscriptions,
		ReadSubscriptions: &t_api.ReadSubscriptionsRequest{
			PromiseId: id,
			Limit:     limit,
		},
	}
}

func (g *Generator) GenerateCreateSubscription(r *rand.Rand, t int64) *t_api.Request {
	id := g.idSet[r.Intn(len(g.idSet))]
	promiseId := g.idSet[r.Intn(len(g.idSet))]
	url := g.urlSet[r.Intn(len(g.urlSet))]
	delay := g.retrySet[r.Intn(len(g.retrySet))]
	attempts := RangeIntn(r, 1, 4)

	return &t_api.Request{
		Kind: t_api.CreateSubscription,
		CreateSubscription: &t_api.CreateSubscriptionRequest{
			Id:        id,
			PromiseId: promiseId,
			Url:       url,
			RetryPolicy: &subscription.RetryPolicy{
				Delay:    int64(delay),
				Attempts: int64(attempts),
			},
		},
	}
}

func (g *Generator) GenerateDeleteSubscription(r *rand.Rand, t int64) *t_api.Request {
	id := g.idSet[r.Intn(len(g.idSet))]
	promiseId := g.idSet[r.Intn(len(g.idSet))]

	return &t_api.Request{
		Kind: t_api.DeleteSubscription,
		DeleteSubscription: &t_api.DeleteSubscriptionRequest{
			Id:        id,
			PromiseId: promiseId,
		},
	}
}
