package service

import (
	"github.com/resonatehq/resonate/internal/kernel/bus"
	"github.com/resonatehq/resonate/internal/kernel/t_api"
	"github.com/resonatehq/resonate/internal/util"
)

// TODO: (experimental)
func (s *Service) SearchSubscriptions(id string, header *Header, params *SearchSubscriptionsParams) (*t_api.ReadSubscriptionsResponse, error) {
	cq := make(chan *bus.CQE[t_api.Request, t_api.Response], 1)

	// TODO: add cursor

	// validation
	limit := params.Limit
	if limit <= 0 || limit > 100 {
		limit = 100
	}

	s.api.Enqueue(&bus.SQE[t_api.Request, t_api.Response]{
		Metadata: s.metadata(header.RequestId, "read-subscriptions"),
		Submission: &t_api.Request{
			Kind: t_api.ReadSubscriptions,
			ReadSubscriptions: &t_api.ReadSubscriptionsRequest{
				// TODO: do we want query?
				PromiseId: params.PromiseId,
				Limit:     limit,
				// SortId:    nil,
			},
		},
		Callback: s.sendOrPanic(cq),
	})

	cqe := <-cq
	if cqe.Error != nil {
		return nil, cqe.Error
	}

	util.Assert(cqe.Completion.ReadSubscriptions != nil, "response must not be nil")
	return cqe.Completion.ReadSubscriptions, nil
}

func (s *Service) CreateSubscription(id string, header CreateSubscriptionHeader, body *CreateSubscriptionBody) (*t_api.CreateSubscriptionResponse, error) {
	cq := make(chan *bus.CQE[t_api.Request, t_api.Response], 1)

	s.api.Enqueue(&bus.SQE[t_api.Request, t_api.Response]{
		Metadata: s.metadata(header.RequestId, "create-subscription"),
		Submission: &t_api.Request{
			Kind: t_api.CreateSubscription,
			CreateSubscription: &t_api.CreateSubscriptionRequest{
				Id:          id,
				PromiseId:   body.PromiseId,
				Url:         body.Url,
				RetryPolicy: body.RetryPolicy,
			},
		},
		Callback: s.sendOrPanic(cq),
	})

	cqe := <-cq
	if cqe.Error != nil {
		return nil, cqe.Error
	}

	util.Assert(cqe.Completion.CreateSubscription != nil, "response must not be nil")
	return cqe.Completion.CreateSubscription, nil
}

// TODO: (experimental
func (s *Service) DeleteSubscription(id string, header *Header) (*t_api.DeleteSubscriptionResponse, error) {
	cq := make(chan *bus.CQE[t_api.Request, t_api.Response], 1)

	s.api.Enqueue(&bus.SQE[t_api.Request, t_api.Response]{
		Metadata: s.metadata(header.RequestId, "delete-subscription"),
		Submission: &t_api.Request{
			Kind: t_api.DeleteSubscription,
			DeleteSubscription: &t_api.DeleteSubscriptionRequest{
				Id: id,
				// TODO: should not require promise id
			},
		},
		Callback: s.sendOrPanic(cq),
	})

	cqe := <-cq
	if cqe.Error != nil {
		return nil, cqe.Error
	}

	util.Assert(cqe.Completion.DeleteSubscription != nil, "response must not be nil")
	return cqe.Completion.DeleteSubscription, nil
}
