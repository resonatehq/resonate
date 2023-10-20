package service

import (
	"strings"

	"github.com/google/uuid"
	"github.com/resonatehq/resonate/internal/api"
	"github.com/resonatehq/resonate/internal/kernel/bus"
	"github.com/resonatehq/resonate/internal/kernel/metadata"
	"github.com/resonatehq/resonate/internal/kernel/t_api"
	"github.com/resonatehq/resonate/internal/util"
	"github.com/resonatehq/resonate/pkg/promise"
)

func (e *ValidationError) Error() string { return e.msg }

type Service struct {
	api      api.API
	protocol string
}

func New(api api.API, protocol string) *Service {
	return &Service{
		api:      api,
		protocol: protocol,
	}
}

func (s *Service) metadata(id string, name string) *metadata.Metadata {
	if id == "" {
		id = uuid.New().String()
	}

	metadata := metadata.New(id)
	metadata.Tags.Set("name", name)
	metadata.Tags.Set("api", s.protocol)

	return metadata
}

// Read Promise

func (s *Service) ReadPromise(id string, header *Header) (*t_api.ReadPromiseResponse, error) {
	cq := make(chan *bus.CQE[t_api.Request, t_api.Response])
	defer close(cq)

	s.api.Enqueue(&bus.SQE[t_api.Request, t_api.Response]{
		Metadata: s.metadata(header.RequestId, "read-promise"),
		Submission: &t_api.Request{
			Kind: t_api.ReadPromise,
			ReadPromise: &t_api.ReadPromiseRequest{
				Id: id,
			},
		},
		Callback: s.sendOrPanic(cq),
	})

	cqe := <-cq
	if cqe.Error != nil {
		return nil, cqe.Error
	}

	util.Assert(cqe.Completion.ReadPromise != nil, "response must not be nil")
	return cqe.Completion.ReadPromise, nil

}

// Search Promise

func (s *Service) SearchPromises(header *Header, params *SearchPromiseParams) (*t_api.SearchPromisesResponse, error) {
	var searchPromises *t_api.SearchPromisesRequest
	if params.Cursor != "" {
		cursor, err := t_api.NewCursor[t_api.SearchPromisesRequest](params.Cursor)
		if err != nil {
			return nil, &ValidationError{msg: err.Error()}
		}
		searchPromises = cursor.Next
	} else {
		// validate
		if params.Q == "" {
			return nil, &ValidationError{msg: "query must be provided"}
		}

		var states []promise.State
		switch strings.ToLower(params.State) {
		case "":
			states = []promise.State{
				promise.Pending,
				promise.Resolved,
				promise.Rejected,
				promise.Timedout,
				promise.Canceled,
			}
		case "pending":
			states = []promise.State{
				promise.Pending,
			}
		case "resolved":
			states = []promise.State{
				promise.Resolved,
			}
		case "rejected":
			states = []promise.State{
				promise.Rejected,
				promise.Timedout,
				promise.Canceled,
			}
		default:
			return nil, &ValidationError{"state must be one of: pending, resolved, rejected"}
		}

		limit := params.Limit
		if limit <= 0 || limit > 100 {
			limit = 100
		}

		searchPromises = &t_api.SearchPromisesRequest{
			Q:      params.Q,
			States: states,
			Limit:  limit,
		}
	}

	cq := make(chan *bus.CQE[t_api.Request, t_api.Response])
	defer close(cq)

	s.api.Enqueue(&bus.SQE[t_api.Request, t_api.Response]{
		Metadata: s.metadata(header.RequestId, "search-promises"),
		Submission: &t_api.Request{
			Kind:           t_api.SearchPromises,
			SearchPromises: searchPromises,
		},
		Callback: s.sendOrPanic(cq),
	})

	cqe := <-cq
	if cqe.Error != nil {
		return nil, cqe.Error
	}

	util.Assert(cqe.Completion.SearchPromises != nil, "response must not be nil")
	return cqe.Completion.SearchPromises, nil
}

// Create Promise

func (s *Service) CreatePromise(id string, header *CreatePromiseHeader, body *CreatePromiseBody) (*t_api.CreatePromiseResponse, error) {
	cq := make(chan *bus.CQE[t_api.Request, t_api.Response])
	defer close(cq)

	s.api.Enqueue(&bus.SQE[t_api.Request, t_api.Response]{
		Metadata: s.metadata(header.RequestId, "create-promise"),
		Submission: &t_api.Request{
			Kind: t_api.CreatePromise,
			CreatePromise: &t_api.CreatePromiseRequest{
				Id:             id,
				IdempotencyKey: header.IdempotencyKey,
				Strict:         header.Strict,
				Param:          body.Param,
				Timeout:        body.Timeout,
				Tags:           body.Tags,
			},
		},
		Callback: s.sendOrPanic(cq),
	})

	cqe := <-cq
	if cqe.Error != nil {
		return nil, cqe.Error
	}

	util.Assert(cqe.Completion.CreatePromise != nil, "response must not be nil")
	return cqe.Completion.CreatePromise, nil
}

// Cancel Promise

func (s *Service) CancelPromise(id string, header *CancelPromiseHeader, body *CancelPromiseBody) (*t_api.CancelPromiseResponse, error) {
	cq := make(chan *bus.CQE[t_api.Request, t_api.Response])
	defer close(cq)

	s.api.Enqueue(&bus.SQE[t_api.Request, t_api.Response]{
		Metadata: s.metadata(header.RequestId, "cancel-promise"),
		Submission: &t_api.Request{
			Kind: t_api.CancelPromise,
			CancelPromise: &t_api.CancelPromiseRequest{
				Id:             id,
				IdempotencyKey: header.IdempotencyKey,
				Strict:         header.Strict,
				Value:          body.Value,
			},
		},
		Callback: s.sendOrPanic(cq),
	})

	cqe := <-cq
	if cqe.Error != nil {
		return nil, cqe.Error
	}

	util.Assert(cqe.Completion.CancelPromise != nil, "response must not be nil")
	return cqe.Completion.CancelPromise, nil
}

// Resolve Promise

func (s *Service) ResolvePromise(id string, header *ResolvePromiseHeader, body *ResolvePromiseBody) (*t_api.ResolvePromiseResponse, error) {
	cq := make(chan *bus.CQE[t_api.Request, t_api.Response])
	defer close(cq)

	s.api.Enqueue(&bus.SQE[t_api.Request, t_api.Response]{
		Metadata: s.metadata(header.RequestId, "resolve-promise"),
		Submission: &t_api.Request{
			Kind: t_api.ResolvePromise,
			ResolvePromise: &t_api.ResolvePromiseRequest{
				Id:             id,
				IdempotencyKey: header.IdempotencyKey,
				Strict:         header.Strict,
				Value:          body.Value,
			},
		},
		Callback: s.sendOrPanic(cq),
	})

	cqe := <-cq
	if cqe.Error != nil {
		return nil, cqe.Error
	}

	util.Assert(cqe.Completion.ResolvePromise != nil, "response must not be nil")
	return cqe.Completion.ResolvePromise, nil
}

// Reject Promise

func (s *Service) RejectPromise(id string, header *RejectPromiseHeader, body *RejectPromiseBody) (*t_api.RejectPromiseResponse, error) {
	cq := make(chan *bus.CQE[t_api.Request, t_api.Response])
	defer close(cq)

	s.api.Enqueue(&bus.SQE[t_api.Request, t_api.Response]{
		Metadata: s.metadata(header.RequestId, "reject-promise"),
		Submission: &t_api.Request{
			Kind: t_api.RejectPromise,
			RejectPromise: &t_api.RejectPromiseRequest{
				Id:             id,
				IdempotencyKey: header.IdempotencyKey,
				Strict:         header.Strict,
				Value:          body.Value,
			},
		},
		Callback: s.sendOrPanic(cq),
	})

	cqe := <-cq
	if cqe.Error != nil {
		return nil, cqe.Error
	}

	util.Assert(cqe.Completion.RejectPromise != nil, "response must not be nil")
	return cqe.Completion.RejectPromise, nil
}

func (s *Service) sendOrPanic(cq chan *bus.CQE[t_api.Request, t_api.Response]) func(*t_api.Response, error) {
	return func(completion *t_api.Response, err error) {
		cqe := &bus.CQE[t_api.Request, t_api.Response]{
			// Tags:       s.protocol(),
			Completion: completion,
			Error:      err,
		}

		select {
		case cq <- cqe:
		default:
			panic("response channel must not block")
		}
	}
}
