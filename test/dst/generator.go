package dst

import (
	"fmt"
	"math/rand" // nosemgrep
	"strconv"

	"github.com/resonatehq/resonate/internal/kernel/types"
	"github.com/resonatehq/resonate/pkg/promise"
	"github.com/resonatehq/resonate/pkg/subscription"
)

type Generator struct {
	ticks      int64
	idSet      []string
	ikeySet    []*promise.IdempotencyKey
	dataSet    [][]byte
	headersSet []map[string]string
	tagsSet    []map[string]string
	urlSet     []string
	retrySet   []int
	requests   []RequestGenerator
}

type RequestGenerator func(*rand.Rand, int64) *types.Request

func NewGenerator(r *rand.Rand, config *Config) *Generator {
	idSet := make([]string, config.Ids)
	for i := 0; i < config.Ids; i++ {
		idSet[i] = strconv.Itoa(i)
	}

	ikeySet := []*promise.IdempotencyKey{}
	for i := 0; i < config.Ikeys; i++ {
		s := strconv.Itoa(i)
		ikey := promise.IdempotencyKey(s)
		ikeySet = append(ikeySet, &ikey, nil) // half of all ikeys are nil
	}

	dataSet := [][]byte{}
	for i := 0; i < config.Data; i++ {
		dataSet = append(dataSet, []byte(strconv.Itoa(i)), nil) // half of all values are nil
	}

	headersSet := []map[string]string{}
	for i := 0; i < config.Headers; i++ {
		headers := map[string]string{}
		for j := 0; j < r.Intn(3)+1; j++ {
			headers[strconv.Itoa(j)] = fmt.Sprintf("%d.%d", i, j)
		}

		headersSet = append(headersSet, headers, nil) // half of all headers are nil
	}

	tagsSet := []map[string]string{}
	for i := 0; i < config.Tags; i++ {
		tags := map[string]string{}
		for j := 0; j < r.Intn(3)+1; j++ {
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
		ticks:      config.Ticks,
		idSet:      idSet,
		ikeySet:    ikeySet,
		dataSet:    dataSet,
		headersSet: headersSet,
		tagsSet:    tagsSet,
		urlSet:     urlSet,
		retrySet:   retrySet,
	}
}

func (g *Generator) AddRequest(request RequestGenerator) {
	g.requests = append(g.requests, request)
}

func (g *Generator) Generate(r *rand.Rand, t int64, n int, cursors []*types.Request) []*types.Request {
	reqs := []*types.Request{}

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

func (g *Generator) GenerateReadPromise(r *rand.Rand, t int64) *types.Request {
	id := g.idSet[r.Intn(len(g.idSet))]

	return &types.Request{
		Kind: types.ReadPromise,
		ReadPromise: &types.ReadPromiseRequest{
			Id: id,
		},
	}
}

func (g *Generator) GenerateSearchPromises(r *rand.Rand, t int64) *types.Request {
	limit := r.Intn(10)
	states := []promise.State{}

	var query string
	switch r.Intn(2) {
	case 0:
		query = fmt.Sprintf("*%d", r.Intn(10))
	default:
		query = fmt.Sprintf("%d*", r.Intn(10))
	}

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

	return &types.Request{
		Kind: types.SearchPromises,
		SearchPromises: &types.SearchPromisesRequest{
			Q:      query,
			States: states,
			Limit:  limit,
		},
	}
}

func (g *Generator) GenerateCreatePromise(r *rand.Rand, t int64) *types.Request {
	id := g.idSet[r.Intn(len(g.idSet))]
	ikey := g.ikeySet[r.Intn(len(g.ikeySet))]
	data := g.dataSet[r.Intn(len(g.dataSet))]
	headers := g.headersSet[r.Intn(len(g.headersSet))]
	tags := g.tagsSet[r.Intn(len(g.tagsSet))]
	timeout := RangeInt63n(r, t, g.ticks)
	strict := r.Intn(2) == 0

	return &types.Request{
		Kind: types.CreatePromise,
		CreatePromise: &types.CreatePromiseRequest{
			Id: id,
			Param: promise.Value{
				Headers: headers,
				Data:    data,
			},
			Timeout:       timeout,
			IdemptencyKey: ikey,
			Tags:          tags,
			Strict:        strict,
		},
	}
}

func (g *Generator) GenerateCancelPromise(r *rand.Rand, t int64) *types.Request {
	id := g.idSet[r.Intn(len(g.idSet))]
	ikey := g.ikeySet[r.Intn(len(g.ikeySet))]
	data := g.dataSet[r.Intn(len(g.dataSet))]
	headers := g.headersSet[r.Intn(len(g.headersSet))]
	strict := r.Intn(2) == 0

	return &types.Request{
		Kind: types.CancelPromise,
		CancelPromise: &types.CancelPromiseRequest{
			Id: id,
			Value: promise.Value{
				Headers: headers,
				Data:    data,
			},
			IdemptencyKey: ikey,
			Strict:        strict,
		},
	}
}

func (g *Generator) GenerateResolvePromise(r *rand.Rand, t int64) *types.Request {
	id := g.idSet[r.Intn(len(g.idSet))]
	ikey := g.ikeySet[r.Intn(len(g.ikeySet))]
	data := g.dataSet[r.Intn(len(g.dataSet))]
	headers := g.headersSet[r.Intn(len(g.headersSet))]
	strict := r.Intn(2) == 0

	return &types.Request{
		Kind: types.ResolvePromise,
		ResolvePromise: &types.ResolvePromiseRequest{
			Id: id,
			Value: promise.Value{
				Headers: headers,
				Data:    data,
			},
			IdemptencyKey: ikey,
			Strict:        strict,
		},
	}
}

func (g *Generator) GenerateRejectPromise(r *rand.Rand, t int64) *types.Request {
	id := g.idSet[r.Intn(len(g.idSet))]
	ikey := g.ikeySet[r.Intn(len(g.ikeySet))]
	data := g.dataSet[r.Intn(len(g.dataSet))]
	headers := g.headersSet[r.Intn(len(g.headersSet))]
	strict := r.Intn(2) == 0

	return &types.Request{
		Kind: types.RejectPromise,
		RejectPromise: &types.RejectPromiseRequest{
			Id: id,
			Value: promise.Value{
				Headers: headers,
				Data:    data,
			},
			IdemptencyKey: ikey,
			Strict:        strict,
		},
	}
}

func (g *Generator) GenerateReadSubscriptions(r *rand.Rand, t int64) *types.Request {
	limit := r.Intn(10)
	id := g.idSet[r.Intn(len(g.idSet))]

	return &types.Request{
		Kind: types.ReadSubscriptions,
		ReadSubscriptions: &types.ReadSubscriptionsRequest{
			PromiseId: id,
			Limit:     limit,
		},
	}
}

func (g *Generator) GenerateCreateSubscription(r *rand.Rand, t int64) *types.Request {
	id := g.idSet[r.Intn(len(g.idSet))]
	promiseId := g.idSet[r.Intn(len(g.idSet))]
	url := g.urlSet[r.Intn(len(g.urlSet))]
	delay := g.retrySet[r.Intn(len(g.retrySet))]
	attempts := RangeIntn(r, 1, 4)

	return &types.Request{
		Kind: types.CreateSubscription,
		CreateSubscription: &types.CreateSubscriptionRequest{
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

func (g *Generator) GenerateDeleteSubscription(r *rand.Rand, t int64) *types.Request {
	id := g.idSet[r.Intn(len(g.idSet))]
	promiseId := g.idSet[r.Intn(len(g.idSet))]

	return &types.Request{
		Kind: types.DeleteSubscription,
		DeleteSubscription: &types.DeleteSubscriptionRequest{
			Id:        id,
			PromiseId: promiseId,
		},
	}
}
