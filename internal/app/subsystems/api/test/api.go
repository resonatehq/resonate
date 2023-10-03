package test

import (
	"testing"
	"time"

	"github.com/resonatehq/resonate/internal/kernel/bus"
	"github.com/resonatehq/resonate/internal/kernel/types"
	"github.com/stretchr/testify/assert"
)

type API struct {
	t   *testing.T
	req *types.Request
	res *types.Response
}

func (a *API) Load(t *testing.T, req *types.Request, res *types.Response) {
	a.t = t
	a.req = req
	a.res = res
}

func (a *API) String() string {
	return "api:test"
}

func (a *API) Enqueue(sqe *bus.SQE[types.Request, types.Response]) {
	// assert
	assert.Equal(a.t, a.req, sqe.Submission)

	// immediately call callback
	go sqe.Callback(0, a.res, nil)
}

func (a *API) Dequeue(int, <-chan time.Time) []*bus.SQE[types.Request, types.Response] {
	return nil
}

func (a *API) Done() bool {
	return false
}
