package grpc

import (
	"context"

	"github.com/resonatehq/resonate/internal/app/subsystems/api/grpc/pb"
	"github.com/resonatehq/resonate/internal/kernel/t_api"
	"google.golang.org/grpc/status"
)

func (s *server) AcquireLock(c context.Context, r *pb.AcquireLockRequest) (*pb.AcquireLockResponse, error) {
	res, err := s.api.Process(r.RequestId, &t_api.Request{
		Payload: &t_api.AcquireLockRequest{
			ResourceId:  r.ResourceId,
			ExecutionId: r.ExecutionId,
			ProcessId:   r.ProcessId,
			Ttl:         r.Ttl,
		},
	})
	if err != nil {
		return nil, status.Error(s.code(err.Code), err.Error())
	}

	_ = res.AsAcquireLockResponse() // Serves as type assertion
	return &pb.AcquireLockResponse{
		Acquired: res.Status == t_api.StatusCreated,
	}, nil
}

func (s *server) ReleaseLock(c context.Context, r *pb.ReleaseLockRequest) (*pb.ReleaseLockResponse, error) {
	res, err := s.api.Process(r.RequestId, &t_api.Request{
		Payload: &t_api.ReleaseLockRequest{
			ResourceId:  r.ResourceId,
			ExecutionId: r.ExecutionId,
		},
	})
	if err != nil {
		return nil, status.Error(s.code(err.Code), err.Error())
	}

	_ = res.AsReleaseLockResponse() // Serves as type assertion
	return &pb.ReleaseLockResponse{
		Released: res.Status == t_api.StatusCreated,
	}, nil
}

func (s *server) HeartbeatLocks(c context.Context, r *pb.HeartbeatLocksRequest) (*pb.HeartbeatLocksResponse, error) {
	res, err := s.api.Process(r.RequestId, &t_api.Request{
		Payload: &t_api.HeartbeatLocksRequest{
			ProcessId: r.ProcessId,
		},
	})
	if err != nil {
		return nil, status.Error(s.code(err.Code), err.Error())
	}

	return &pb.HeartbeatLocksResponse{
		LocksAffected: int32(res.AsHeartbeatLocksResponse().LocksAffected),
	}, nil
}
