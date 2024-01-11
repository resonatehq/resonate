package grpc

import (
	"context"
	"errors"

	"github.com/resonatehq/resonate/internal/api"
	grpcApi "github.com/resonatehq/resonate/internal/app/subsystems/api/grpc/api"
	"github.com/resonatehq/resonate/internal/app/subsystems/api/service"
	"github.com/resonatehq/resonate/internal/util"
	"github.com/resonatehq/resonate/pkg/lock"
	grpcStatus "google.golang.org/grpc/status"
)

func (s *server) AcquireLock(ctx context.Context, req *grpcApi.AcquireLockRequest) (*grpcApi.AcquireLockResponse, error) {
	header := &service.Header{}

	body := &service.AcquireLockBody{
		ResourceId:  req.Lock.ResourceId,
		ProcessId:   req.Lock.ProcessId,
		ExecutionId: req.Lock.ExecutionId,
		Timeout:     req.Lock.Timeout,
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

func (s *server) BulkHeartbeatLock(ctx context.Context, req *grpcApi.BulkHeartbeatRequest) (*grpcApi.BulkHeartbeatResponse, error) {
	header := &service.Header{}

	body := &service.BulkHeartbeatBody{
		ProcessId: req.ProcessId,
		Timeout:   req.Timeout,
	}

	_, err := s.service.BulkHeartbeat(header, body)
	if err != nil {
		var apiErr *api.APIErrorResponse
		util.Assert(errors.As(err, &apiErr), "err must be an api error")
		return nil, grpcStatus.Error(apiErr.APIError.Code.GRPC(), err.Error())
	}

	return &grpcApi.BulkHeartbeatResponse{}, nil
}

func (s *server) ReleaseLock(ctx context.Context, req *grpcApi.ReleaseLockRequest) (*grpcApi.ReleaseLockResponse, error) {
	header := &service.Header{}

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
		ResourceId:  lock.ResourceId,
		ProcessId:   lock.ProcessId,
		ExecutionId: lock.ExecutionId,
		Timeout:     lock.Timeout,
	}
}
