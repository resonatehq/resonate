package grpc

import (
	"context"

	"github.com/resonatehq/resonate/internal/app/subsystems/api/service"
	"github.com/resonatehq/resonate/internal/kernel/t_api"
	"github.com/resonatehq/resonate/internal/util"

	grpcApi "github.com/resonatehq/resonate/internal/app/subsystems/api/grpc/api"
	"github.com/resonatehq/resonate/pkg/idempotency"
	"github.com/resonatehq/resonate/pkg/promise"
	"google.golang.org/grpc/codes"
	grpcStatus "google.golang.org/grpc/status"
)

func (s *server) ReadPromise(ctx context.Context, req *grpcApi.ReadPromiseRequest) (*grpcApi.ReadPromiseResponse, error) {
	header := &service.Header{
		RequestId: req.RequestId,
	}

	res, err := s.service.ReadPromise(req.Id, header)
	if err != nil {
		return nil, grpcStatus.Error(s.code(err.Code), err.Error())
	}

	return &grpcApi.ReadPromiseResponse{
		Promise: protoPromise(res.Promise),
	}, nil
}

func (s *server) SearchPromises(ctx context.Context, req *grpcApi.SearchPromisesRequest) (*grpcApi.SearchPromisesResponse, error) {
	header := &service.Header{
		RequestId: req.RequestId,
	}

	// TODO: for now, look at at protobuf validators
	// ref: https://github.com/protocolbuffers/protobuf/issues/1606
	// can't check if limit was set or not in proto3. see above issue
	// so can't check for 0.
	if req.Limit > 100 || req.Limit < 0 {
		return nil, grpcStatus.Error(codes.InvalidArgument, "field limit must be greater than 0 and less than or equal to 100")
	}

	params := &service.SearchPromisesParams{
		Id:     &req.Id,
		State:  searchState(req.State),
		Tags:   req.Tags,
		Limit:  util.ToPointer(int(req.Limit)),
		Cursor: &req.Cursor,
	}

	res, err := s.service.SearchPromises(header, params)
	if err != nil {
		return nil, grpcStatus.Error(s.code(err.Code), err.Error())
	}

	promises := make([]*grpcApi.Promise, len(res.Promises))
	for i, promise := range res.Promises {
		promises[i] = protoPromise(promise)
	}

	cursor := ""
	if res.Cursor != nil {
		var err error
		cursor, err = res.Cursor.Encode()
		if err != nil {
			return nil, grpcStatus.Error(codes.Internal, err.Error())
		}
	}

	return &grpcApi.SearchPromisesResponse{
		Cursor:   cursor,
		Promises: promises,
	}, nil
}

func (s *server) CreatePromise(ctx context.Context, req *grpcApi.CreatePromiseRequest) (*grpcApi.CreatePromiseResponse, error) {
	var idempotencyKey *idempotency.Key
	if req.IdempotencyKey != "" {
		idempotencyKey = util.ToPointer(idempotency.Key(req.IdempotencyKey))
	}

	var headers map[string]string
	if req.Param != nil {
		headers = req.Param.Headers
	}

	var data []byte
	if req.Param != nil {
		data = req.Param.Data
	}

	// TODO: for now, look at at protobuf validators
	if req.Timeout < 0 || req.Timeout == 0 {
		return nil, grpcStatus.Error(codes.InvalidArgument, "timeout must be greater than 0")
	}

	header := &service.CreatePromiseHeader{
		Header:         service.Header{RequestId: req.RequestId},
		Strict:         req.Strict,
		IdempotencyKey: idempotencyKey,
	}

	body := &promise.Promise{
		Id: req.Id,
		Param: promise.Value{
			Headers: headers,
			Data:    data,
		},
		Timeout: req.Timeout,
	}

	res, err := s.service.CreatePromise(header, body)
	if err != nil {
		return nil, grpcStatus.Error(s.code(err.Code), err.Error())
	}

	return &grpcApi.CreatePromiseResponse{
		Noop:    res.Status == t_api.StatusOK,
		Promise: protoPromise(res.Promise),
	}, nil
}

func (s *server) CancelPromise(ctx context.Context, req *grpcApi.CancelPromiseRequest) (*grpcApi.CancelPromiseResponse, error) {
	var idempotencyKey *idempotency.Key
	if req.IdempotencyKey != "" {
		idempotencyKey = util.ToPointer(idempotency.Key(req.IdempotencyKey))
	}

	var headers map[string]string
	if req.Value != nil {
		headers = req.Value.Headers
	}

	var data []byte
	if req.Value != nil {
		data = req.Value.Data
	}

	header := &service.CompletePromiseHeader{
		Header:         service.Header{RequestId: req.RequestId},
		Strict:         req.Strict,
		IdempotencyKey: idempotencyKey,
	}

	body := &service.CompletePromiseBody{
		Value: promise.Value{
			Headers: headers,
			Data:    data,
		},
	}
	res, err := s.service.CompletePromise(req.Id, promise.Canceled, header, body)
	if err != nil {
		return nil, grpcStatus.Error(s.code(err.Code), err.Error())
	}

	return &grpcApi.CancelPromiseResponse{
		Noop:    res.Status == t_api.StatusOK,
		Promise: protoPromise(res.Promise),
	}, nil
}

func (s *server) ResolvePromise(ctx context.Context, req *grpcApi.ResolvePromiseRequest) (*grpcApi.ResolvePromiseResponse, error) {
	var idempotencyKey *idempotency.Key
	if req.IdempotencyKey != "" {
		idempotencyKey = util.ToPointer(idempotency.Key(req.IdempotencyKey))
	}

	var headers map[string]string
	if req.Value != nil {
		headers = req.Value.Headers
	}

	var data []byte
	if req.Value != nil {
		data = req.Value.Data
	}

	header := &service.CompletePromiseHeader{
		Header:         service.Header{RequestId: req.RequestId},
		Strict:         req.Strict,
		IdempotencyKey: idempotencyKey,
	}

	body := &service.CompletePromiseBody{
		Value: promise.Value{
			Headers: headers,
			Data:    data,
		},
	}

	res, err := s.service.CompletePromise(req.Id, promise.Resolved, header, body)
	if err != nil {
		return nil, grpcStatus.Error(s.code(err.Code), err.Error())
	}

	return &grpcApi.ResolvePromiseResponse{
		Noop:    res.Status == t_api.StatusOK,
		Promise: protoPromise(res.Promise),
	}, nil
}

func (s *server) RejectPromise(ctx context.Context, req *grpcApi.RejectPromiseRequest) (*grpcApi.RejectPromiseResponse, error) {
	var idempotencyKey *idempotency.Key
	if req.IdempotencyKey != "" {
		idempotencyKey = util.ToPointer(idempotency.Key(req.IdempotencyKey))
	}

	var headers map[string]string
	if req.Value != nil {
		headers = req.Value.Headers
	}

	var data []byte
	if req.Value != nil {
		data = req.Value.Data
	}

	header := &service.CompletePromiseHeader{
		Header:         service.Header{RequestId: req.RequestId},
		Strict:         req.Strict,
		IdempotencyKey: idempotencyKey,
	}

	body := &service.CompletePromiseBody{
		Value: promise.Value{
			Headers: headers,
			Data:    data,
		},
	}

	res, err := s.service.CompletePromise(req.Id, promise.Rejected, header, body)
	if err != nil {
		return nil, grpcStatus.Error(s.code(err.Code), err.Error())
	}

	return &grpcApi.RejectPromiseResponse{
		Noop:    res.Status == t_api.StatusOK,
		Promise: protoPromise(res.Promise),
	}, nil
}

func protoPromise(promise *promise.Promise) *grpcApi.Promise {
	if promise == nil {
		return nil
	}

	var idempotencyKeyForCreate, idempotencyKeyForComplete string
	if promise.IdempotencyKeyForCreate != nil {
		idempotencyKeyForCreate = string(*promise.IdempotencyKeyForCreate)
	}
	if promise.IdempotencyKeyForComplete != nil {
		idempotencyKeyForComplete = string(*promise.IdempotencyKeyForComplete)
	}

	return &grpcApi.Promise{
		Id:    promise.Id,
		State: protoState(promise.State),
		Param: &grpcApi.Value{
			Headers: promise.Param.Headers,
			Data:    promise.Param.Data,
		},
		Value: &grpcApi.Value{
			Headers: promise.Param.Headers,
			Data:    promise.Param.Data,
		},
		Timeout:                   promise.Timeout,
		IdempotencyKeyForCreate:   idempotencyKeyForCreate,
		IdempotencyKeyForComplete: idempotencyKeyForComplete,
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

func searchState(searchState grpcApi.SearchState) *string {
	switch searchState {
	case grpcApi.SearchState_SEARCH_ALL:
		return nil
	case grpcApi.SearchState_SEARCH_RESOLVED:
		return util.ToPointer("resolved")
	case grpcApi.SearchState_SEARCH_REJECTED:
		return util.ToPointer("rejected")
	case grpcApi.SearchState_SEARCH_PENDING:
		return util.ToPointer("pending")
	default:
		panic("invalid state")
	}
}
