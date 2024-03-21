package grpc

import (
	"context"
	"errors"

	"github.com/resonatehq/resonate/internal/api"
	grpcApi "github.com/resonatehq/resonate/internal/app/subsystems/api/grpc/api"
	"github.com/resonatehq/resonate/internal/app/subsystems/api/service"
	"github.com/resonatehq/resonate/internal/util"
	"github.com/resonatehq/resonate/pkg/promise"
	"google.golang.org/grpc/codes"
	grpcStatus "google.golang.org/grpc/status"
)

func (s *server) ClaimTask(ctx context.Context, req *grpcApi.ClaimTaskRequest) (*grpcApi.ClaimTaskResponse, error) {
	header := &service.Header{
		RequestId: req.RequestId,
	}

	if req.TaskId == "" {
		return nil, grpcStatus.Error(codes.InvalidArgument, "task_id must be provided")
	}

	body := &service.ClaimTaskBody{
		TaskId:          req.TaskId,
		Counter:         int(req.Counter),
		ProcessId:       req.ProcessId,
		ExecutionId:     req.ExecutionId,
		ExpiryInSeconds: req.ExpiryInSeconds,
	}

	resp, err := s.service.ClaimTask(header, body)
	if err != nil {
		var apiErr *api.APIErrorResponse
		util.Assert(errors.As(err, &apiErr), "err must be an api error")
		return nil, grpcStatus.Error(apiErr.APIError.Code.GRPC(), err.Error())
	}

	return &grpcApi.ClaimTaskResponse{
		Promise: protoTaskPromise(resp.Promise),
	}, nil
}

func (s *server) CompleteTask(ctx context.Context, req *grpcApi.CompleteTaskRequest) (*grpcApi.CompleteTaskResponse, error) {
	header := &service.Header{
		RequestId: req.RequestId,
	}

	if req.TaskId == "" {
		return nil, grpcStatus.Error(codes.InvalidArgument, "task_id must be provided")
	}

	var state string
	switch req.State {
	case grpcApi.TaskState_RESOLVED:
		state = promise.Resolved.String()
	case grpcApi.TaskState_REJECTED:
		state = promise.Rejected.String()
	default:
		return nil, grpcStatus.Error(codes.InvalidArgument, "state must be provided and be either RESOLVED or REJECTED")
	}

	body := &service.CompleteTaskBody{
		TaskId:      req.TaskId,
		Counter:     int(req.Counter),
		ExecutionId: req.ExecutionId,
		State:       state,
		Value: promise.Value{
			Headers: req.Value.Headers,
			Data:    req.Value.Data,
		},
	}

	_, err := s.service.CompleteTask(header, body)
	if err != nil {
		var apiErr *api.APIErrorResponse
		util.Assert(errors.As(err, &apiErr), "err must be an api error")
		return nil, grpcStatus.Error(apiErr.APIError.Code.GRPC(), err.Error())
	}

	return &grpcApi.CompleteTaskResponse{}, nil
}

func protoTaskPromise(promise *promise.Promise) *grpcApi.TaskPromise {
	// if promise == nil {
	// 	return nil
	// }

	// var idempotencyKeyForCreate, idempotencyKeyForComplete string
	// if promise.IdempotencyKeyForCreate != nil {
	// 	idempotencyKeyForCreate = string(*promise.IdempotencyKeyForCreate)
	// }
	// if promise.IdempotencyKeyForComplete != nil {
	// 	idempotencyKeyForComplete = string(*promise.IdempotencyKeyForComplete)
	// }

	// todo: comback to deal with this type difference.
	return &grpcApi.TaskPromise{
		// Id:    promise.Id,
		// State: protoState(promise.State),
		// Param: &grpcApi.Value{
		// 	Headers: promise.Param.Headers,
		// 	Data:    promise.Param.Data,
		// },
		// Value: &grpcApi.Value{
		// 	Headers: promise.Param.Headers,
		// 	Data:    promise.Param.Data,
		// },
		// Timeout:                   promise.Timeout,
		// IdempotencyKeyForCreate:   idempotencyKeyForCreate,
		// IdempotencyKeyForComplete: idempotencyKeyForComplete,
		// CreatedOn:                 util.SafeDeref(promise.CreatedOn),
		// CompletedOn:               util.SafeDeref(promise.CompletedOn),
		// Tags:                      promise.Tags,
	}
}
