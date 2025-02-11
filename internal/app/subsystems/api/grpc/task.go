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
		Kind: t_api.ClaimTask,
		ClaimTask: &t_api.ClaimTaskRequest{
			Id:        r.Id,
			ProcessId: r.ProcessId,
			Counter:   int(r.Counter),
			Ttl:       int(r.Ttl),
		},
	})
	if err != nil {
		return nil, status.Error(s.code(err.Code), err.Error())
	}
	if r.Ttl < 0 {
		return nil, status.Error(codes.InvalidArgument, "The ttl field must be greater or equal than zero")
	}

	util.Assert(res.ClaimTask != nil, "result must not be nil")
	util.Assert(res.ClaimTask.Status != t_api.StatusCreated || (res.ClaimTask.Task != nil && res.ClaimTask.Task.Mesg != nil), "task and mesg must not be nil if created")

	var mesg *pb.Mesg
	if res.ClaimTask.Status == t_api.StatusCreated {
		promises := map[string]*pb.MesgPromise{
			"root": {
				Id:   res.ClaimTask.Task.Mesg.Root,
				Href: res.ClaimTask.RootPromiseHref,
				Data: protoPromise(res.ClaimTask.RootPromise),
			},
		}

		if res.ClaimTask.Task.Mesg.Type == message.Resume {
			promises["leaf"] = &pb.MesgPromise{
				Id:   res.ClaimTask.Task.Mesg.Leaf,
				Href: res.ClaimTask.LeafPromiseHref,
				Data: protoPromise(res.ClaimTask.LeafPromise),
			}
		}

		mesg = &pb.Mesg{
			Type:     string(res.ClaimTask.Task.Mesg.Type),
			Promises: promises,
		}
	}

	return &pb.ClaimTaskResponse{
		Claimed: res.ClaimTask.Status == t_api.StatusCreated,
		Mesg:    mesg,
	}, nil
}

func (s *server) CompleteTask(c context.Context, r *pb.CompleteTaskRequest) (*pb.CompleteTaskResponse, error) {
	res, err := s.api.Process(r.RequestId, &t_api.Request{
		Kind: t_api.CompleteTask,
		CompleteTask: &t_api.CompleteTaskRequest{
			Id:      r.Id,
			Counter: int(r.Counter),
		},
	})
	if err != nil {
		return nil, status.Error(s.code(err.Code), err.Error())
	}

	util.Assert(res.CompleteTask != nil, "result must not be nil")
	return &pb.CompleteTaskResponse{
		Completed: res.CompleteTask.Status == t_api.StatusCreated,
	}, nil
}

func (s *server) HeartbeatTasks(c context.Context, r *pb.HeartbeatTasksRequest) (*pb.HeartbeatTasksResponse, error) {
	res, err := s.api.Process(r.RequestId, &t_api.Request{
		Kind: t_api.HeartbeatTasks,
		HeartbeatTasks: &t_api.HeartbeatTasksRequest{
			ProcessId: r.ProcessId,
		},
	})
	if err != nil {
		return nil, status.Error(s.code(err.Code), err.Error())
	}

	util.Assert(res.HeartbeatTasks != nil, "result must not be nil")
	return &pb.HeartbeatTasksResponse{
		TasksAffected: res.HeartbeatTasks.TasksAffected,
	}, nil
}
