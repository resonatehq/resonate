package service

import (
	"fmt"
	"strings"

	"github.com/resonatehq/resonate/internal/kernel/bus"
	"github.com/resonatehq/resonate/internal/kernel/t_api"
	"github.com/resonatehq/resonate/internal/util"
	"github.com/resonatehq/resonate/pkg/promise"
)

// Read Promise

func (s *Service) ReadPromise(id string, header *Header) (*t_api.ReadPromiseResponse, *Error) {
	cq := make(chan *bus.CQE[t_api.Request, t_api.Response], 1)

	s.api.Enqueue(&bus.SQE[t_api.Request, t_api.Response]{
		Callback: s.sendOrPanic(cq),
		Submission: &t_api.Request{
			Kind: t_api.ReadPromise,
			Tags: s.tags(header.RequestId, "ReadPromise"),
			ReadPromise: &t_api.ReadPromiseRequest{
				Id: id,
			},
		},
	})

	cqe := <-cq
	if cqe.Error != nil {
		return nil, ServerError(cqe.Error)
	}

	util.Assert(cqe.Completion.ReadPromise != nil, "response must not be nil")
	return cqe.Completion.ReadPromise, RequestError(cqe.Completion.Status())
}

// Search Promise

func (s *Service) SearchPromises(header *Header, params *SearchPromisesParams) (*t_api.SearchPromisesResponse, *Error) {
	if params == nil {
		params = &SearchPromisesParams{}
	}
	var searchPromises *t_api.SearchPromisesRequest
	if params.Cursor != nil && *params.Cursor != "" {
		cursor, err := t_api.NewCursor[t_api.SearchPromisesRequest](*params.Cursor)
		if err != nil {
			return nil, RequestValidationError(err)
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
		Callback: s.sendOrPanic(cq),
		Submission: &t_api.Request{
			Kind:           t_api.SearchPromises,
			Tags:           s.tags(header.RequestId, "SearchPromises"),
			SearchPromises: searchPromises,
		},
	})

	cqe := <-cq
	if cqe.Error != nil {
		return nil, ServerError(cqe.Error)
	}

	util.Assert(cqe.Completion.SearchPromises != nil, "response must not be nil")
	return cqe.Completion.SearchPromises, RequestError(cqe.Completion.Status())
}

// Create Promise

func (s *Service) CreatePromise(header *CreatePromiseHeader, body *promise.Promise) (*t_api.CreatePromiseResponse, *Error) {
	cq := make(chan *bus.CQE[t_api.Request, t_api.Response], 1)

	s.api.Enqueue(&bus.SQE[t_api.Request, t_api.Response]{
		Callback: s.sendOrPanic(cq),
		Submission: &t_api.Request{
			Kind: t_api.CreatePromise,
			Tags: s.tags(header.RequestId, "CreatePromise"),
			CreatePromise: &t_api.CreatePromiseRequest{
				Id:             body.Id,
				IdempotencyKey: header.IdempotencyKey,
				Strict:         header.Strict,
				Param:          body.Param,
				Timeout:        body.Timeout,
				Tags:           body.Tags,
			},
		},
	})

	cqe := <-cq
	if cqe.Error != nil {
		return nil, ServerError(cqe.Error)
	}

	util.Assert(cqe.Completion.CreatePromise != nil, "response must not be nil")
	return cqe.Completion.CreatePromise, RequestError(cqe.Completion.Status())
}

// Complete Promise
func (s *Service) CompletePromise(id string, state promise.State, header *CompletePromiseHeader, body *CompletePromiseBody) (*t_api.CompletePromiseResponse, *Error) {
	cq := make(chan *bus.CQE[t_api.Request, t_api.Response], 1)

	s.api.Enqueue(&bus.SQE[t_api.Request, t_api.Response]{
		Callback: s.sendOrPanic(cq),
		Submission: &t_api.Request{
			Kind: t_api.CompletePromise,
			Tags: s.tags(header.RequestId, "CompletePromise"),
			CompletePromise: &t_api.CompletePromiseRequest{
				Id:             id,
				IdempotencyKey: header.IdempotencyKey,
				Strict:         header.Strict,
				State:          state,
				Value:          body.Value,
			},
		},
	})

	cqe := <-cq
	if cqe.Error != nil {
		return nil, ServerError(cqe.Error)
	}

	util.Assert(cqe.Completion.CompletePromise != nil, "response must not be nil")
	return cqe.Completion.CompletePromise, RequestError(cqe.Completion.Status())
}
