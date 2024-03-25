package grpc

import (
	"context"
	"errors"

	"github.com/resonatehq/resonate/internal/api"
	grpcApi "github.com/resonatehq/resonate/internal/app/subsystems/api/grpc/api"
	"github.com/resonatehq/resonate/internal/app/subsystems/api/service"
	"github.com/resonatehq/resonate/internal/util"
	"github.com/resonatehq/resonate/pkg/lock"
	"google.golang.org/grpc/codes"
	grpcStatus "google.golang.org/grpc/status"
)

func (s *server) AcquireLock(ctx context.Context, req *grpcApi.AcquireLockRequest) (*grpcApi.AcquireLockResponse, error) {
	header := &service.Header{
		RequestId: req.RequestId,
	}

	if req.Lock == nil {
		return nil, grpcStatus.Error(codes.InvalidArgument, "lock must be provided")
	}
	if req.Lock.ResourceId == "" {
		return nil, grpcStatus.Error(codes.InvalidArgument, "lock.resource_id must be provided")
	}
	if req.Lock.ProcessId == "" {
		return nil, grpcStatus.Error(codes.InvalidArgument, "lock.process_id must be provided")
	}
	if req.Lock.ExecutionId == "" {
		return nil, grpcStatus.Error(codes.InvalidArgument, "lock.execution_id must be provided")
	}
	if req.Lock.ExpiryInMilliseconds == 0 {
		return nil, grpcStatus.Error(codes.InvalidArgument, "lock.expiry_in_milliseconds must be provided")
	}

	body := &service.AcquireLockBody{
		ResourceId:           req.Lock.ResourceId,
		ProcessId:            req.Lock.ProcessId,
		ExecutionId:          req.Lock.ExecutionId,
		ExpiryInMilliseconds: req.Lock.ExpiryInMilliseconds,
	}

	resp, err := s.service.AcquireLock(header, body)
	if err != nil {
		var apiErr *api.APIErrorResponse
		util.Assert(errors.As(err, &apiErr), "err must be an api error")
		return nil, grpcStatus.Error(apiErr.APIError.Code.GRPC(), err.Error())
	}

	return &grpcApi.AcquireLockResponse{
		Lock: protoLock(resp.Lock),
	}, nil
}

func (s *server) HeartbeatLocks(ctx context.Context, req *grpcApi.HeartbeatLocksRequest) (*grpcApi.HeartbeatLocksResponse, error) {
	header := &service.Header{
		RequestId: req.RequestId,
	}

	if req.ProcessId == "" {
		return nil, grpcStatus.Error(codes.InvalidArgument, "process_id must be provided")
	}

	body := &service.HeartbeatBody{
		ProcessId: req.ProcessId,
	}

	res, err := s.service.Heartbeat(header, body)
	if err != nil {
		var apiErr *api.APIErrorResponse
		util.Assert(errors.As(err, &apiErr), "err must be an api error")
		return nil, grpcStatus.Error(apiErr.APIError.Code.GRPC(), err.Error())
	}

	return &grpcApi.HeartbeatLocksResponse{
		LocksAffected: int32(res.LocksAffected),
	}, nil
}

func (s *server) ReleaseLock(ctx context.Context, req *grpcApi.ReleaseLockRequest) (*grpcApi.ReleaseLockResponse, error) {
	header := &service.Header{
		RequestId: req.RequestId,
	}

	if req.ResourceId == "" {
		return nil, grpcStatus.Error(codes.InvalidArgument, "resource_id must be provided")
	}
	if req.ExecutionId == "" {
		return nil, grpcStatus.Error(codes.InvalidArgument, "execution_id must be provided")
	}

	body := &service.ReleaseLockBody{
		ResourceId:  req.ResourceId,
		ExecutionId: req.ExecutionId,
	}

	_, err := s.service.ReleaseLock(header, body)
	if err != nil {
		var apiErr *api.APIErrorResponse
		util.Assert(errors.As(err, &apiErr), "err must be an api error")
		return nil, grpcStatus.Error(apiErr.APIError.Code.GRPC(), err.Error())
	}

	return &grpcApi.ReleaseLockResponse{}, nil
}

func protoLock(lock *lock.Lock) *grpcApi.Lock {
	if lock == nil {
		return nil
	}

	return &grpcApi.Lock{
		ResourceId:           lock.ResourceId,
		ProcessId:            lock.ProcessId,
		ExecutionId:          lock.ExecutionId,
		ExpiryInMilliseconds: lock.ExpiryInMilliseconds,
	}
}
