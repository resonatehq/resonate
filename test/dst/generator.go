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
	ikeySet    []*promise.Ikey
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

	ikeySet := []*promise.Ikey{}
	for i := 0; i < config.Ikeys; i++ {
		s := strconv.Itoa(i)
		ikey := promise.Ikey(s)
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

func (g *Generator) Generate(r *rand.Rand, t int64, n int) []*types.Request {
	reqs := []*types.Request{}

	for i := 0; i < n; i++ {
		f := g.requests[r.Intn(len(g.requests))]
		reqs = append(reqs, f(r, t))
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
	return &types.Request{
		Kind: types.SearchPromises,
		SearchPromises: &types.SearchPromisesRequest{
			Q: "*",
			States: []promise.State{
				promise.Pending,
				promise.Resolved,
				promise.Rejected,
				promise.Timedout,
				promise.Canceled,
			},
			Limit: 5,
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

	return &types.Request{
		Kind: types.CreatePromise,
		CreatePromise: &types.CreatePromiseRequest{
			Id:      id,
			Timeout: timeout,
			Param: promise.Value{
				Headers: headers,
				Ikey:    ikey,
				Data:    data,
			},
			Tags: tags,
		},
	}
}

func (g *Generator) GenerateCancelPromise(r *rand.Rand, t int64) *types.Request {
	id := g.idSet[r.Intn(len(g.idSet))]
	ikey := g.ikeySet[r.Intn(len(g.ikeySet))]
	data := g.dataSet[r.Intn(len(g.dataSet))]
	headers := g.headersSet[r.Intn(len(g.headersSet))]

	return &types.Request{
		Kind: types.CancelPromise,
		CancelPromise: &types.CancelPromiseRequest{
			Id: id,
			Value: promise.Value{
				Headers: headers,
				Ikey:    ikey,
				Data:    data,
			},
		},
	}
}

func (g *Generator) GenerateResolvePromise(r *rand.Rand, t int64) *types.Request {
	id := g.idSet[r.Intn(len(g.idSet))]
	ikey := g.ikeySet[r.Intn(len(g.ikeySet))]
	data := g.dataSet[r.Intn(len(g.dataSet))]
	headers := g.headersSet[r.Intn(len(g.headersSet))]

	return &types.Request{
		Kind: types.ResolvePromise,
		ResolvePromise: &types.ResolvePromiseRequest{
			Id: id,
			Value: promise.Value{
				Headers: headers,
				Ikey:    ikey,
				Data:    data,
			},
		},
	}
}

func (g *Generator) GenerateRejectPromise(r *rand.Rand, t int64) *types.Request {
	id := g.idSet[r.Intn(len(g.idSet))]
	ikey := g.ikeySet[r.Intn(len(g.ikeySet))]
	data := g.dataSet[r.Intn(len(g.dataSet))]
	headers := g.headersSet[r.Intn(len(g.headersSet))]

	return &types.Request{
		Kind: types.RejectPromise,
		RejectPromise: &types.RejectPromiseRequest{
			Id: id,
			Value: promise.Value{
				Headers: headers,
				Ikey:    ikey,
				Data:    data,
			},
		},
	}
}

func (g *Generator) GenerateCompletePromise(r *rand.Rand, t int64) *types.Request {
	id := g.idSet[r.Intn(len(g.idSet))]
	ikey := g.ikeySet[r.Intn(len(g.ikeySet))]
	data := g.dataSet[r.Intn(len(g.dataSet))]
	headers := g.headersSet[r.Intn(len(g.headersSet))]

	var state promise.State
	switch r.Intn(3) {
	case 0:
		state = promise.Canceled
	case 1:
		state = promise.Resolved
	case 2:
		state = promise.Rejected
	}

	return &types.Request{
		Kind: types.CompletePromise,
		CompletePromise: &types.CompletePromiseRequest{
			Id: id,
			Value: promise.Value{
				Headers: headers,
				Ikey:    ikey,
				Data:    data,
			},
			State: state,
		},
	}
}

func (g *Generator) GenerateReadSubscriptions(r *rand.Rand, t int64) *types.Request {
	id := g.idSet[r.Intn(len(g.idSet))]

	return &types.Request{
		Kind: types.ReadSubscriptions,
		ReadSubscriptions: &types.ReadSubscriptionsRequest{
			PromiseId: id,
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
