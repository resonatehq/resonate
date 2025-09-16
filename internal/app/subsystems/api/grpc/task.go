package grpc

import (
	"context"

	"github.com/resonatehq/resonate/internal/app/subsystems/api/grpc/pb"
	"github.com/resonatehq/resonate/internal/kernel/t_api"
	"github.com/resonatehq/resonate/internal/util"
	"github.com/resonatehq/resonate/pkg/message"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (s *server) ClaimTask(c context.Context, r *pb.ClaimTaskRequest) (*pb.ClaimTaskResponse, error) {
	res, err := s.api.Process(r.RequestId, &t_api.Request{
		Payload: &t_api.ClaimTaskRequest{
			Id:        r.Id,
			ProcessId: r.ProcessId,
			Counter:   int(r.Counter),
			Ttl:       r.Ttl,
		},
	})
	if err != nil {
		return nil, status.Error(s.code(err.Code), err.Error())
	}
	if r.Ttl < 0 {
		return nil, status.Error(codes.InvalidArgument, "The field ttl must be greater than or equal to zero")
	}

	claimTask := res.AsClaimTaskResponse()
	util.Assert(res.Status != t_api.StatusCreated || (claimTask.Task != nil && claimTask.Task.Mesg != nil), "task and mesg must not be nil if created")

	var mesg *pb.Mesg
	if res.Status == t_api.StatusCreated {
		promises := map[string]*pb.MesgPromise{
			"root": {
				Id:   claimTask.Task.Mesg.Root,
				Href: claimTask.RootPromiseHref,
				Data: protoPromise(claimTask.RootPromise),
			},
		}

		if claimTask.Task.Mesg.Type == message.Resume {
			promises["leaf"] = &pb.MesgPromise{
				Id:   claimTask.Task.Mesg.Leaf,
				Href: claimTask.LeafPromiseHref,
				Data: protoPromise(claimTask.LeafPromise),
			}
		}

		mesg = &pb.Mesg{
			Type:     string(claimTask.Task.Mesg.Type),
			Promises: promises,
		}
	}

	return &pb.ClaimTaskResponse{
		Claimed: res.Status == t_api.StatusCreated,
		Mesg:    mesg,
	}, nil
}

func (s *server) CompleteTask(c context.Context, r *pb.CompleteTaskRequest) (*pb.CompleteTaskResponse, error) {
	res, err := s.api.Process(r.RequestId, &t_api.Request{
		Payload: &t_api.CompleteTaskRequest{
			Id:      r.Id,
			Counter: int(r.Counter),
		},
	})
	if err != nil {
		return nil, status.Error(s.code(err.Code), err.Error())
	}

	_ = res.AsCompleteTaskResponse() // Serves as a type assertion
	return &pb.CompleteTaskResponse{
		Completed: res.Status == t_api.StatusCreated,
	}, nil
}

func (s *server) DropTask(c context.Context, r *pb.DropTaskRequest) (*pb.DropTaskResponse, error) {
	res, err := s.api.Process(r.RequestId, &t_api.Request{
		Payload: &t_api.DropTaskRequest{
			Id:      r.Id,
			Counter: int(r.Counter),
		},
	})
	if err != nil {
		return nil, status.Error(s.code(err.Code), err.Error())
	}

	_ = res.AsDropTaskResponse() // Serves as a type assertion
	return &pb.DropTaskResponse{
		Dropped: res.Status == t_api.StatusCreated,
	}, nil
}

func (s *server) HeartbeatTasks(c context.Context, r *pb.HeartbeatTasksRequest) (*pb.HeartbeatTasksResponse, error) {
	res, err := s.api.Process(r.RequestId, &t_api.Request{
		Payload: &t_api.HeartbeatTasksRequest{
			ProcessId: r.ProcessId,
		},
	})
	if err != nil {
		return nil, status.Error(s.code(err.Code), err.Error())
	}

	return &pb.HeartbeatTasksResponse{
		TasksAffected: res.AsHeartbeatTasksResponse().TasksAffected,
	}, nil
}
