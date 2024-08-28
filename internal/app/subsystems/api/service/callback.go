package service

import (
	"errors"

	"github.com/resonatehq/resonate/internal/api"
	"github.com/resonatehq/resonate/internal/kernel/bus"
	"github.com/resonatehq/resonate/internal/kernel/t_api"
	"github.com/resonatehq/resonate/internal/util"
	"github.com/resonatehq/resonate/pkg/message"
)

// CREATE

func (s *Service) CreateCallback(header *Header, body *CreateCallbackBody) (*t_api.CreateCallbackResponse, error) {
	util.Assert(body.PromiseId != "", "callback.promiseId must be provided")
	util.Assert(body.Timeout != 0, "callback.timeout must be provided")
	util.Assert(body.Recv != "", "callback.recv must be provided")

	cq := make(chan *bus.CQE[t_api.Request, t_api.Response], 1)

	s.api.Enqueue(&bus.SQE[t_api.Request, t_api.Response]{
		Callback: s.sendOrPanic(cq),
		Submission: &t_api.Request{
			Kind: t_api.CreateCallback,
			Tags: s.tags(header.RequestId, "CreateCallback"),
			CreateCallback: &t_api.CreateCallbackRequest{
				PromiseId: body.PromiseId,
				Timeout:   body.Timeout,
				Message: &message.Message{
					Recv: body.Recv,
					Data: body.Data,
				},
			},
		},
	})

	cqe := <-cq
	if cqe.Error != nil {
		var resErr *t_api.ResonateError
		util.Assert(errors.As(cqe.Error, &resErr), "err must be a ResonateError")
		return nil, resErr
	}

	util.Assert(cqe.Completion.CreateCallback != nil, "response must not be nil")

	if api.IsRequestError(cqe.Completion.CreateCallback.Status) {
		return nil, api.HandleRequestError(cqe.Completion.CreateCallback.Status)
	}

	return cqe.Completion.CreateCallback, nil
}
