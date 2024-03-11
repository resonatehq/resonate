package service

import (
	"errors"
	"fmt"
	"strings"

	"github.com/resonatehq/resonate/internal/api"
	"github.com/resonatehq/resonate/internal/kernel/bus"
	"github.com/resonatehq/resonate/internal/kernel/t_api"
	"github.com/resonatehq/resonate/internal/util"
	"github.com/resonatehq/resonate/pkg/promise"
)

// Read Promise

func (s *Service) ReadPromise(id string, header *Header) (*t_api.ReadPromiseResponse, error) {
	cq := make(chan *bus.CQE[t_api.Request, t_api.Response], 1)

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
		var resErr *t_api.ResonateError
		util.Assert(errors.As(cqe.Error, &resErr), "err must be a ResonateError")
		return nil, api.HandleResonateError(resErr)
	}

	util.Assert(cqe.Completion.ReadPromise != nil, "response must not be nil") // WHY AM I GETTING NIL RESPONSES?

	// application level error - 3xx, 4xx
	if api.IsRequestError(cqe.Completion.ReadPromise.Status) {
		return nil, api.HandleRequestError(cqe.Completion.ReadPromise.Status)
	}

	// success
	return cqe.Completion.ReadPromise, nil
}

// Search Promise

func (s *Service) SearchPromises(header *Header, params *SearchPromisesParams) (*t_api.SearchPromisesResponse, error) {
	if params == nil {
		params = &SearchPromisesParams{}
	}
	var searchPromises *t_api.SearchPromisesRequest
	if params.Cursor != nil && *params.Cursor != "" {
		cursor, err := t_api.NewCursor[t_api.SearchPromisesRequest](*params.Cursor)
		if err != nil {
			return nil, api.HandleValidationError(err)
		}
		searchPromises = cursor.Next
	} else {
		// set default query
		if params.Id == nil {
			params.Id = util.ToPointer("*")
		}

		var states []promise.State
		if params.State == nil {
			states = []promise.State{
				promise.Pending,
				promise.Resolved,
				promise.Rejected,
				promise.Timedout,
				promise.Canceled,
			}
		} else {
			switch strings.ToLower(*params.State) {
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
				panic(fmt.Sprintf("invalid state: %s", *params.State))
			}
		}

		// set default limit
		if params.Limit == nil || *params.Limit == 0 {
			params.Limit = util.ToPointer(100)
		}

		searchPromises = &t_api.SearchPromisesRequest{
			Id:     *params.Id,
			States: states,
			Tags:   params.Tags,
			Limit:  *params.Limit,
		}
	}

	cq := make(chan *bus.CQE[t_api.Request, t_api.Response], 1)

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

	if api.IsRequestError(cqe.Completion.SearchPromises.Status) {
		return nil, api.HandleRequestError(cqe.Completion.SearchPromises.Status)
	}

	// success
	return cqe.Completion.SearchPromises, nil
}

// Create Promise

func (s *Service) CreatePromise(header *CreatePromiseHeader, body *promise.Promise) (*t_api.CreatePromiseResponse, error) {
	cq := make(chan *bus.CQE[t_api.Request, t_api.Response], 1)

	s.api.Enqueue(&bus.SQE[t_api.Request, t_api.Response]{
		Metadata: s.metadata(header.RequestId, "create-promise"),
		Submission: &t_api.Request{
			Kind: t_api.CreatePromise,
			CreatePromise: &t_api.CreatePromiseRequest{
				Id:             body.Id,
				IdempotencyKey: header.IdempotencyKey,
				Strict:         header.Strict,
				Param:          body.Param,
				Timeout:        body.Timeout, // required by connection
				Tags:           body.Tags,
			},
		},
		Callback: s.sendOrPanic(cq),
	})

	cqe := <-cq
	if cqe.Error != nil {
		var resErr *t_api.ResonateError
		util.Assert(errors.As(cqe.Error, &resErr), "err must be a ResonateError")
		return nil, api.HandleResonateError(resErr)
	}

	util.Assert(cqe.Completion.CreatePromise != nil, "response must not be nil")

	if api.IsRequestError(cqe.Completion.CreatePromise.Status) {
		return nil, api.HandleRequestError(cqe.Completion.CreatePromise.Status)
	}

	// success
	return cqe.Completion.CreatePromise, nil
}

// Cancel Promise

func (s *Service) CancelPromise(id string, header *CompletePromiseHeader, body *CompletePromiseBody) (*t_api.CompletePromiseResponse, error) {
	cq := make(chan *bus.CQE[t_api.Request, t_api.Response], 1)

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
		var resErr *t_api.ResonateError
		util.Assert(errors.As(cqe.Error, &resErr), "err must be a ResonateError")
		return nil, api.HandleResonateError(resErr)
	}

	util.Assert(cqe.Completion.CancelPromise != nil, "response must not be nil")

	if api.IsRequestError(cqe.Completion.CancelPromise.Status) {
		return nil, api.HandleRequestError(cqe.Completion.CancelPromise.Status)
	}

	// success
	return cqe.Completion.CancelPromise, nil
}

// Resolve Promise

func (s *Service) ResolvePromise(id string, header *CompletePromiseHeader, body *CompletePromiseBody) (*t_api.CompletePromiseResponse, error) {
	cq := make(chan *bus.CQE[t_api.Request, t_api.Response], 1)

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
		var resErr *t_api.ResonateError
		util.Assert(errors.As(cqe.Error, &resErr), "err must be a ResonateError")
		return nil, api.HandleResonateError(resErr)
	}

	util.Assert(cqe.Completion.ResolvePromise != nil, "response must not be nil")

	if api.IsRequestError(cqe.Completion.ResolvePromise.Status) {
		return nil, api.HandleRequestError(cqe.Completion.ResolvePromise.Status)
	}

	// success
	return cqe.Completion.ResolvePromise, nil
}

// Reject Promise

func (s *Service) RejectPromise(id string, header *CompletePromiseHeader, body *CompletePromiseBody) (*t_api.CompletePromiseResponse, error) {
	cq := make(chan *bus.CQE[t_api.Request, t_api.Response], 1)

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
		var resErr *t_api.ResonateError
		util.Assert(errors.As(cqe.Error, &resErr), "err must be a ResonateError")
		return nil, api.HandleResonateError(resErr)
	}

	util.Assert(cqe.Completion.RejectPromise != nil, "response must not be nil")

	if api.IsRequestError(cqe.Completion.RejectPromise.Status) {
		return nil, api.HandleRequestError(cqe.Completion.RejectPromise.Status)
	}

	// success
	return cqe.Completion.RejectPromise, nil
}
