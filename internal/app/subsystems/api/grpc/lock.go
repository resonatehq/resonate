package grpc

import (
	"context"

	grpcApi "github.com/resonatehq/resonate/internal/app/subsystems/api/grpc/api"
	"github.com/resonatehq/resonate/internal/app/subsystems/api/service"
	"github.com/resonatehq/resonate/pkg/lock"
	"google.golang.org/grpc/codes"
	grpcStatus "google.golang.org/grpc/status"
)

func (s *server) AcquireLock(ctx context.Context, req *grpcApi.AcquireLockRequest) (*grpcApi.AcquireLockResponse, error) {
	header := &service.Header{
		RequestId: req.RequestId,
	}

	if req.ResourceId == "" {
		return nil, grpcStatus.Error(codes.InvalidArgument, "lock.resource_id must be provided")
	}
	if req.ExecutionId == "" {
		return nil, grpcStatus.Error(codes.InvalidArgument, "lock.execution_id must be provided")
	}
	if req.ProcessId == "" {
		return nil, grpcStatus.Error(codes.InvalidArgument, "lock.process_id must be provided")
	}
	if req.ExpiryInMilliseconds == 0 {
		return nil, grpcStatus.Error(codes.InvalidArgument, "lock.expiry_in_milliseconds must be provided")
	}

	body := &service.AcquireLockBody{
		ResourceId:           req.ResourceId,
		ExecutionId:          req.ExecutionId,
		ProcessId:            req.ProcessId,
		ExpiryInMilliseconds: req.ExpiryInMilliseconds,
	}

	res, err := s.service.AcquireLock(header, body)
	if err != nil {
		return nil, grpcStatus.Error(s.code(err.Code), err.Error())
	}

	return &grpcApi.AcquireLockResponse{
		Lock: protoLock(res.Lock),
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
		return nil, grpcStatus.Error(s.code(err.Code), err.Error())
	}

	return &grpcApi.ReleaseLockResponse{}, nil
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
		return nil, grpcStatus.Error(s.code(err.Code), err.Error())
	}

	return &grpcApi.HeartbeatLocksResponse{
		LocksAffected: int32(res.LocksAffected),
	}, nil
}

func protoLock(lock *lock.Lock) *grpcApi.Lock {
	if lock == nil {
		return nil
	}

	return &grpcApi.Lock{
		ResourceId:           lock.ResourceId,
		ExecutionId:          lock.ExecutionId,
		ProcessId:            lock.ProcessId,
		ExpiryInMilliseconds: lock.ExpiryInMilliseconds,
	}
}
