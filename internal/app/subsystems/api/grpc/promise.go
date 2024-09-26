package grpc

import (
	"context"

	"github.com/resonatehq/resonate/internal/kernel/t_api"
	"github.com/resonatehq/resonate/internal/util"

	grpcApi "github.com/resonatehq/resonate/internal/app/subsystems/api/grpc/api"
	"github.com/resonatehq/resonate/pkg/idempotency"
	"github.com/resonatehq/resonate/pkg/promise"
	"google.golang.org/grpc/codes"
	grpcStatus "google.golang.org/grpc/status"
)

func (s *server) ReadPromise(c context.Context, r *grpcApi.ReadPromiseRequest) (*grpcApi.ReadPromiseResponse, error) {
	res, err := s.api.Process(r.RequestId, &t_api.Request{
		Kind: t_api.ReadPromise,
		ReadPromise: &t_api.ReadPromiseRequest{
			Id: r.Id,
		},
	})
	if err != nil {
		return nil, grpcStatus.Error(s.code(err.Code), err.Error())
	}

	util.Assert(res.ReadPromise != nil, "result must not be nil")
	return &grpcApi.ReadPromiseResponse{
		Promise: protoPromise(res.ReadPromise.Promise),
	}, nil
}

func (s *server) SearchPromises(c context.Context, r *grpcApi.SearchPromisesRequest) (*grpcApi.SearchPromisesResponse, error) {
	req, err := s.api.SearchPromises(r.Id, searchState(r.State), r.Tags, int(r.Limit), r.Cursor)
	if err != nil {
		return nil, grpcStatus.Error(s.code(err.Code), err.Error())
	}

	res, err := s.api.Process(r.RequestId, &t_api.Request{
		Kind:           t_api.SearchPromises,
		SearchPromises: req,
	})
	if err != nil {
		return nil, grpcStatus.Error(s.code(err.Code), err.Error())
	}

	util.Assert(res.SearchPromises != nil, "result must not be nil")

	promises := make([]*grpcApi.Promise, len(res.SearchPromises.Promises))
	for i, promise := range res.SearchPromises.Promises {
		promises[i] = protoPromise(promise)
	}

	var cursor string
	if res.SearchPromises.Cursor != nil {
		var err error
		cursor, err = res.SearchPromises.Cursor.Encode()
		if err != nil {
			return nil, grpcStatus.Error(codes.Internal, err.Error())
		}
	}

	return &grpcApi.SearchPromisesResponse{
		Promises: promises,
		Cursor:   cursor,
	}, nil
}

func (s *server) CreatePromise(c context.Context, r *grpcApi.CreatePromiseRequest) (*grpcApi.CreatePromiseResponse, error) {
	var idempotencyKey *idempotency.Key
	if r.IdempotencyKey != "" {
		idempotencyKey = util.ToPointer(idempotency.Key(r.IdempotencyKey))
	}

	var headers map[string]string
	if r.Param != nil {
		headers = r.Param.Headers
	}

	var data []byte
	if r.Param != nil {
		data = r.Param.Data
	}

	res, err := s.api.Process(r.RequestId, &t_api.Request{
		Kind: t_api.CreatePromise,
		CreatePromise: &t_api.CreatePromiseRequest{
			Id:             r.Id,
			IdempotencyKey: idempotencyKey,
			Strict:         r.Strict,
			Param:          promise.Value{Headers: headers, Data: data},
			Timeout:        r.Timeout,
			Tags:           r.Tags,
		},
	})
	if err != nil {
		return nil, grpcStatus.Error(s.code(err.Code), err.Error())
	}

	util.Assert(res.CreatePromise != nil, "result must not be nil")
	return &grpcApi.CreatePromiseResponse{
		Noop:    res.CreatePromise.Status == t_api.StatusOK,
		Promise: protoPromise(res.CreatePromise.Promise),
	}, nil
}

func (s *server) ResolvePromise(c context.Context, r *grpcApi.ResolvePromiseRequest) (*grpcApi.ResolvePromiseResponse, error) {
	var idempotencyKey *idempotency.Key
	if r.IdempotencyKey != "" {
		idempotencyKey = util.ToPointer(idempotency.Key(r.IdempotencyKey))
	}

	var headers map[string]string
	if r.Value != nil {
		headers = r.Value.Headers
	}

	var data []byte
	if r.Value != nil {
		data = r.Value.Data
	}

	res, err := s.api.Process(r.RequestId, &t_api.Request{
		Kind: t_api.CompletePromise,
		CompletePromise: &t_api.CompletePromiseRequest{
			Id:             r.Id,
			IdempotencyKey: idempotencyKey,
			Strict:         r.Strict,
			State:          promise.Resolved,
			Value:          promise.Value{Headers: headers, Data: data},
		},
	})
	if err != nil {
		return nil, grpcStatus.Error(s.code(err.Code), err.Error())
	}

	util.Assert(res.CompletePromise != nil, "result must not be nil")
	return &grpcApi.ResolvePromiseResponse{
		Noop:    res.CompletePromise.Status == t_api.StatusOK,
		Promise: protoPromise(res.CompletePromise.Promise),
	}, nil
}

func (s *server) RejectPromise(c context.Context, r *grpcApi.RejectPromiseRequest) (*grpcApi.RejectPromiseResponse, error) {
	var idempotencyKey *idempotency.Key
	if r.IdempotencyKey != "" {
		idempotencyKey = util.ToPointer(idempotency.Key(r.IdempotencyKey))
	}

	var headers map[string]string
	if r.Value != nil {
		headers = r.Value.Headers
	}

	var data []byte
	if r.Value != nil {
		data = r.Value.Data
	}

	res, err := s.api.Process(r.RequestId, &t_api.Request{
		Kind: t_api.CompletePromise,
		CompletePromise: &t_api.CompletePromiseRequest{
			Id:             r.Id,
			IdempotencyKey: idempotencyKey,
			Strict:         r.Strict,
			State:          promise.Rejected,
			Value:          promise.Value{Headers: headers, Data: data},
		},
	})
	if err != nil {
		return nil, grpcStatus.Error(s.code(err.Code), err.Error())
	}

	util.Assert(res.CompletePromise != nil, "result must not be nil")
	return &grpcApi.RejectPromiseResponse{
		Noop:    res.CompletePromise.Status == t_api.StatusOK,
		Promise: protoPromise(res.CompletePromise.Promise),
	}, nil
}

func (s *server) CancelPromise(c context.Context, r *grpcApi.CancelPromiseRequest) (*grpcApi.CancelPromiseResponse, error) {
	var idempotencyKey *idempotency.Key
	if r.IdempotencyKey != "" {
		idempotencyKey = util.ToPointer(idempotency.Key(r.IdempotencyKey))
	}

	var headers map[string]string
	if r.Value != nil {
		headers = r.Value.Headers
	}

	var data []byte
	if r.Value != nil {
		data = r.Value.Data
	}

	res, err := s.api.Process(r.RequestId, &t_api.Request{
		Kind: t_api.CompletePromise,
		CompletePromise: &t_api.CompletePromiseRequest{
			Id:             r.Id,
			IdempotencyKey: idempotencyKey,
			Strict:         r.Strict,
			State:          promise.Canceled,
			Value:          promise.Value{Headers: headers, Data: data},
		},
	})
	if err != nil {
		return nil, grpcStatus.Error(s.code(err.Code), err.Error())
	}

	util.Assert(res.CompletePromise != nil, "result must not be nil")
	return &grpcApi.CancelPromiseResponse{
		Noop:    res.CompletePromise.Status == t_api.StatusOK,
		Promise: protoPromise(res.CompletePromise.Promise),
	}, nil
}

// Helper functions

func protoPromise(promise *promise.Promise) *grpcApi.Promise {
	if promise == nil {
		return nil
	}

	return &grpcApi.Promise{
		Id:                        promise.Id,
		State:                     protoState(promise.State),
		Param:                     &grpcApi.Value{Headers: promise.Param.Headers, Data: promise.Param.Data},
		Value:                     &grpcApi.Value{Headers: promise.Value.Headers, Data: promise.Value.Data},
		Timeout:                   promise.Timeout,
		IdempotencyKeyForCreate:   string(util.SafeDeref(promise.IdempotencyKeyForCreate)),
		IdempotencyKeyForComplete: string(util.SafeDeref(promise.IdempotencyKeyForComplete)),
		CreatedOn:                 util.SafeDeref(promise.CreatedOn),
		CompletedOn:               util.SafeDeref(promise.CompletedOn),
		Tags:                      promise.Tags,
	}
}

func protoState(state promise.State) grpcApi.State {
	switch state {
	case promise.Pending:
		return grpcApi.State_PENDING
	case promise.Resolved:
		return grpcApi.State_RESOLVED
	case promise.Rejected:
		return grpcApi.State_REJECTED
	case promise.Timedout:
		return grpcApi.State_REJECTED_TIMEDOUT
	case promise.Canceled:
		return grpcApi.State_REJECTED_CANCELED
	default:
		panic("invalid state")
	}
}

func searchState(searchState grpcApi.SearchState) string {
	switch searchState {
	case grpcApi.SearchState_SEARCH_PENDING:
		return "pending"
	case grpcApi.SearchState_SEARCH_RESOLVED:
		return "resolved"
	case grpcApi.SearchState_SEARCH_REJECTED:
		return "rejected"
	default:
		return ""
	}
}
