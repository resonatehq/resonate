package grpc

import (
	"context"

	grpcApi "github.com/resonatehq/resonate/internal/app/subsystems/api/grpc/api"
	"github.com/resonatehq/resonate/internal/kernel/t_api"
	"github.com/resonatehq/resonate/internal/util"
	"github.com/resonatehq/resonate/pkg/lock"
	grpcStatus "google.golang.org/grpc/status"
)

func (s *server) AcquireLock(c context.Context, r *grpcApi.AcquireLockRequest) (*grpcApi.AcquireLockResponse, error) {
	res, err := s.api.Process(r.RequestId, &t_api.Request{
		Kind: t_api.AcquireLock,
		AcquireLock: &t_api.AcquireLockRequest{
			ResourceId:           r.ResourceId,
			ExecutionId:          r.ExecutionId,
			ProcessId:            r.ProcessId,
			ExpiryInMilliseconds: r.ExpiryInMilliseconds,
		},
	})
	if err != nil {
		return nil, grpcStatus.Error(s.code(err.Code), err.Error())
	}

	util.Assert(res.AcquireLock != nil, "result must not be nil")
	return &grpcApi.AcquireLockResponse{
		Lock: protoLock(res.AcquireLock.Lock),
	}, nil
}

func (s *server) ReleaseLock(c context.Context, r *grpcApi.ReleaseLockRequest) (*grpcApi.ReleaseLockResponse, error) {
	res, err := s.api.Process(r.RequestId, &t_api.Request{
		Kind: t_api.ReleaseLock,
		ReleaseLock: &t_api.ReleaseLockRequest{
			ResourceId:  r.ResourceId,
			ExecutionId: r.ExecutionId,
		},
	})
	if err != nil {
		return nil, grpcStatus.Error(s.code(err.Code), err.Error())
	}

	util.Assert(res.ReleaseLock != nil, "result must not be nil")
	return &grpcApi.ReleaseLockResponse{}, nil
}

func (s *server) HeartbeatLocks(c context.Context, r *grpcApi.HeartbeatLocksRequest) (*grpcApi.HeartbeatLocksResponse, error) {
	res, err := s.api.Process(r.RequestId, &t_api.Request{
		Kind: t_api.HeartbeatLocks,
		HeartbeatLocks: &t_api.HeartbeatLocksRequest{
			ProcessId: r.ProcessId,
		},
	})
	if err != nil {
		return nil, grpcStatus.Error(s.code(err.Code), err.Error())
	}

	util.Assert(res.HeartbeatLocks != nil, "result must not be nil")
	return &grpcApi.HeartbeatLocksResponse{
		LocksAffected: int32(res.HeartbeatLocks.LocksAffected),
	}, nil
}

// Helper functions

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
