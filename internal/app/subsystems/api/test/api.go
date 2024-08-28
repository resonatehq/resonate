package test

import (
	"testing"

	"github.com/resonatehq/resonate/internal/api"
	"github.com/resonatehq/resonate/internal/kernel/bus"
	"github.com/resonatehq/resonate/internal/kernel/t_api"
	"github.com/stretchr/testify/assert"
)

type API struct {
	t   *testing.T
	req *t_api.Request
	res *t_api.Response
}

func (a *API) Load(t *testing.T, req *t_api.Request, res *t_api.Response) {
	a.t = t
	a.req = req
	a.res = res
}

func (a *API) String() string {
	return "api:test"
}

func (a *API) AddSubsystem(subsystem api.Subsystem) {
	panic("not implemented")
}

func (a *API) Start() error {
	return nil
}

func (a *API) Stop() error {
	return nil
}

func (a *API) Shutdown() {}

func (a *API) Done() bool {
	return false
}

func (a *API) Errors() <-chan error {
	return nil
}

func (a *API) Signal(cancel <-chan interface{}) <-chan interface{} {
	panic("not implemented")
}

func (a *API) SQ() <-chan *bus.SQE[t_api.Request, t_api.Response] {
	panic("not implemented")
}

func (a *API) Enqueue(sqe *bus.SQE[t_api.Request, t_api.Response]) {
	// assert
	assert.Equal(a.t, a.req, sqe.Submission)

	// immediately call callback
	go sqe.Callback(a.res, nil)
}

func (a *API) Dequeue(int) []*bus.SQE[t_api.Request, t_api.Response] {
	panic("not implemented")
}
