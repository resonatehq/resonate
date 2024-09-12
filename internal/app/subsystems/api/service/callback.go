package service

import (
	"github.com/resonatehq/resonate/internal/kernel/bus"
	"github.com/resonatehq/resonate/internal/kernel/t_api"
	"github.com/resonatehq/resonate/internal/util"
)

// CREATE

func (s *Service) CreateCallback(header *Header, body *CreateCallbackBody) (*t_api.CreateCallbackResponse, *Error) {
	util.Assert(body.PromiseId != "", "callback.promiseId must be provided")
	util.Assert(body.Timeout != 0, "callback.timeout must be provided")
	util.Assert(body.Recv != nil, "callback.recv must be provided")

	cq := make(chan *bus.CQE[t_api.Request, t_api.Response], 1)

	s.api.Enqueue(&bus.SQE[t_api.Request, t_api.Response]{
		Id: header.Id(),
		Submission: &t_api.Request{
			Kind: t_api.CreateCallback,
			Tags: map[string]string{
				"id":       header.Id(),
				"name":     "CreateCallback",
				"protocol": s.protocol,
			},
			CreateCallback: &t_api.CreateCallbackRequest{
				PromiseId:     body.PromiseId,
				RootPromiseId: body.RootPromiseId,
				Timeout:       body.Timeout,
				Recv:          body.Recv,
			},
		},
		Callback: s.sendOrPanic(cq),
	})

	cqe := <-cq
	if cqe.Error != nil {
		return nil, ServerError(cqe.Error)
	}

	util.Assert(cqe.Completion.CreateCallback != nil, "response must not be nil")
	return cqe.Completion.CreateCallback, RequestError(cqe.Completion.Status())
}
