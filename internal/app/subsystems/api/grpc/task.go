package grpc

import (
	"context"

	grpcApi "github.com/resonatehq/resonate/internal/app/subsystems/api/grpc/api"
	"github.com/resonatehq/resonate/internal/kernel/t_api"
	"github.com/resonatehq/resonate/internal/util"
	"github.com/resonatehq/resonate/pkg/message"
	grpcStatus "google.golang.org/grpc/status"
)

func (s *server) ClaimTask(c context.Context, r *grpcApi.ClaimTaskRequest) (*grpcApi.ClaimTaskResponse, error) {
	res, err := s.api.Process(r.RequestId, &t_api.Request{
		Kind: t_api.ClaimTask,
		ClaimTask: &t_api.ClaimTaskRequest{
			Id:        r.Id,
			ProcessId: r.ProcessId,
			Counter:   int(r.Counter),
			Frequency: int(r.Frequency),
		},
	})
	if err != nil {
		return nil, grpcStatus.Error(s.code(err.Code), err.Error())
	}

	util.Assert(res.ClaimTask != nil, "result must not be nil")
	util.Assert(res.ClaimTask.Status != t_api.StatusCreated || (res.ClaimTask.Task != nil && res.ClaimTask.Task.Mesg != nil), "task and mesg must not be nil if created")

	var mesg *grpcApi.Mesg
	if res.ClaimTask.Status == t_api.StatusCreated {
		promises := map[string]*grpcApi.PromiseOrHref{}

		if res.ClaimTask.RootPromise != nil {
			promises["root"] = &grpcApi.PromiseOrHref{
				Data: &grpcApi.PromiseOrHref_Promise{Promise: protoPromise(res.ClaimTask.RootPromise)},
			}
		} else {
			promises["root"] = &grpcApi.PromiseOrHref{
				Data: &grpcApi.PromiseOrHref_Href{Href: res.ClaimTask.RootPromiseHref},
			}
		}

		if res.ClaimTask.Task.Mesg.Type == message.Resume {
			if res.ClaimTask.LeafPromise != nil {
				promises["leaf"] = &grpcApi.PromiseOrHref{
					Data: &grpcApi.PromiseOrHref_Promise{Promise: protoPromise(res.ClaimTask.LeafPromise)},
				}
			} else {
				promises["leaf"] = &grpcApi.PromiseOrHref{
					Data: &grpcApi.PromiseOrHref_Href{Href: res.ClaimTask.LeafPromiseHref},
				}
			}
		}

		mesg = &grpcApi.Mesg{
			Type:     string(res.ClaimTask.Task.Mesg.Type),
			Promises: promises,
		}
	}

	return &grpcApi.ClaimTaskResponse{
		Claimed: res.ClaimTask.Status == t_api.StatusCreated,
		Mesg:    mesg,
	}, nil
}

func (s *server) CompleteTask(c context.Context, r *grpcApi.CompleteTaskRequest) (*grpcApi.CompleteTaskResponse, error) {
	res, err := s.api.Process(r.RequestId, &t_api.Request{
		Kind: t_api.CompleteTask,
		CompleteTask: &t_api.CompleteTaskRequest{
			Id:      r.Id,
			Counter: int(r.Counter),
		},
	})
	if err != nil {
		return nil, grpcStatus.Error(s.code(err.Code), err.Error())
	}

	util.Assert(res.CompleteTask != nil, "result must not be nil")
	return &grpcApi.CompleteTaskResponse{
		Completed: res.CompleteTask.Status == t_api.StatusCreated,
	}, nil
}

func (s *server) HeartbeatTasks(c context.Context, r *grpcApi.HeartbeatTasksRequest) (*grpcApi.HeartbeatTasksResponse, error) {
	res, err := s.api.Process(r.RequestId, &t_api.Request{
		Kind: t_api.HeartbeatTasks,
		HeartbeatTasks: &t_api.HeartbeatTasksRequest{
			ProcessId: r.ProcessId,
		},
	})
	if err != nil {
		return nil, grpcStatus.Error(s.code(err.Code), err.Error())
	}

	util.Assert(res.HeartbeatTasks != nil, "result must not be nil")
	return &grpcApi.HeartbeatTasksResponse{
		TasksAffected: res.HeartbeatTasks.TasksAffected,
	}, nil
}
