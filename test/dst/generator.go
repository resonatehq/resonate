package dst

import (
	"fmt"
	"math/rand"
	"strconv"

	"github.com/resonatehq/resonate/internal/kernel/types"
	"github.com/resonatehq/resonate/pkg/promise"
	"github.com/resonatehq/resonate/pkg/subscription"
	"github.com/resonatehq/resonate/test"
)

type Generator struct {
	idSet           []string
	ikeySet         []*promise.Ikey
	dataSet         [][]byte
	headersSet      []map[string]string
	retrySet        []int
	subscriptionSet []string
	time            int64
	subscriptions   int64
	requests        []RequestGenerator
}

type RequestGenerator func(*rand.Rand, int64) *types.Request

func NewGenerator(r *rand.Rand, ids int, ikeys int, data int, headers int, retries int, subscriptions int, time int64) *Generator {
	idSet := make([]string, ids)
	for i := 0; i < ids; i++ {
		idSet[i] = strconv.Itoa(i)
	}

	ikeySet := []*promise.Ikey{}
	for i := 0; i < ikeys; i++ {
		s := strconv.Itoa(i)
		ikey := promise.Ikey(s)
		ikeySet = append(ikeySet, &ikey, nil) // half of all ikeys are nil
	}

	dataSet := [][]byte{}
	for i := 0; i < data; i++ {
		dataSet = append(dataSet, []byte(strconv.Itoa(i)), nil) // half of all values are nil
	}

	headersSet := []map[string]string{}
	for i := 0; i < headers; i++ {
		headers := map[string]string{}
		for j := 0; j < r.Intn(3)+1; j++ {
			headers[strconv.Itoa(j)] = fmt.Sprintf("%d.%d", i, j)
		}

		headersSet = append(headersSet, headers, nil) // half of all headers are nil
	}

	retrySet := make([]int, retries)
	for i := 0; i < retries; i++ {
		retrySet[i] = i
	}

	subscriptionSet := make([]string, subscriptions)
	for i := 0; i < subscriptions; i++ {
		subscriptionSet[i] = fmt.Sprintf("https://resonatehq.io/%d", i)
	}

	return &Generator{
		idSet:           idSet,
		ikeySet:         ikeySet,
		dataSet:         dataSet,
		headersSet:      headersSet,
		retrySet:        retrySet,
		subscriptionSet: subscriptionSet,
		time:            time,
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
	q := fmt.Sprintf("%05d*", r.Intn(99999))

	return &types.Request{
		Kind: types.SearchPromises,
		SearchPromises: &types.SearchPromisesRequest{
			Q:     q,
			State: promise.Pending,
		},
	}
}

func (g *Generator) GenerateCreatePromise(r *rand.Rand, t int64) *types.Request {
	id := g.idSet[r.Intn(len(g.idSet))]
	ikey := g.ikeySet[r.Intn(len(g.ikeySet))]
	data := g.dataSet[r.Intn(len(g.dataSet))]
	headers := g.headersSet[r.Intn(len(g.headersSet))]
	timeout := test.RangeInt63n(r, t, g.time)

	url := g.subscriptionSet[r.Intn(len(g.subscriptionSet))]
	delay := g.retrySet[r.Intn(len(g.retrySet))]
	attempts := test.RangeIntn(r, 1, 4)
	g.subscriptions++

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
			Subscriptions: []*types.CreateSubscriptionRequest{{
				Url: url,
				RetryPolicy: &subscription.RetryPolicy{
					Delay:    int64(delay),
					Attempts: int64(attempts),
				},
			}},
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
	url := g.subscriptionSet[r.Intn(len(g.subscriptionSet))]
	delay := g.retrySet[r.Intn(len(g.retrySet))]
	attempts := test.RangeIntn(r, 1, 4)
	g.subscriptions++

	return &types.Request{
		Kind: types.CreateSubscription,
		CreateSubscription: &types.CreateSubscriptionRequest{
			PromiseId: id,
			Url:       url,
			RetryPolicy: &subscription.RetryPolicy{
				Delay:    int64(delay),
				Attempts: int64(attempts),
			},
		},
	}
}

func (g *Generator) GenerateDeleteSubscription(r *rand.Rand, t int64) *types.Request {
	promiseId := g.idSet[r.Intn(len(g.idSet))]
	id := r.Int63n(g.subscriptions + 1)

	return &types.Request{
		Kind: types.DeleteSubscription,
		DeleteSubscription: &types.DeleteSubscriptionRequest{
			PromiseId: promiseId,
			Id:        id,
		},
	}
}
