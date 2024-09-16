package grpc

import (
	"context"

	grpcApi "github.com/resonatehq/resonate/internal/app/subsystems/api/grpc/api"
	"github.com/resonatehq/resonate/internal/app/subsystems/api/service"
	"github.com/resonatehq/resonate/internal/kernel/t_api"
	"github.com/resonatehq/resonate/internal/util"
	"google.golang.org/grpc/codes"
	grpcStatus "google.golang.org/grpc/status"
)

func (s *server) ClaimTask(ctx context.Context, req *grpcApi.ClaimTaskRequest) (*grpcApi.ClaimTaskResponse, error) {
	header := &service.Header{
		RequestId: req.RequestId,
	}

	if req.Id == "" {
		return nil, grpcStatus.Error(codes.InvalidArgument, "task.id must be provided")
	}
	if req.ProcessId == "" {
		return nil, grpcStatus.Error(codes.InvalidArgument, "task.processId must be provided")
	}
	if req.Frequency == 0 {
		return nil, grpcStatus.Error(codes.InvalidArgument, "task.frequency must be provided")
	}

	body := &service.ClaimTaskBody{
		Id:        req.Id,
		ProcessId: req.ProcessId,
		Counter:   int(req.Counter),
		Frequency: int(req.Frequency),
	}

	res, err := s.service.ClaimTask(header, body)
	if err != nil {
		return nil, grpcStatus.Error(s.code(err.Code), err.Error())
	}

	util.Assert(res.Status != t_api.StatusCreated || res.Mesg != nil, "message must not be nil if created")

	promises := map[string]*grpcApi.Promise{}
	for k, promise := range res.Mesg.Promises {
		promises[k] = protoPromise(promise)
	}

	return &grpcApi.ClaimTaskResponse{
		Type:     string(res.Mesg.Type),
		Promises: promises,
	}, nil
}

func (s *server) CompleteTask(ctx context.Context, req *grpcApi.CompleteTaskRequest) (*grpcApi.CompleteTaskResponse, error) {
	header := &service.Header{
		RequestId: req.RequestId,
	}

	if req.Id == "" {
		return nil, grpcStatus.Error(codes.InvalidArgument, "task.id must be provided")
	}

	body := &service.CompleteTaskBody{
		Id:      req.Id,
		Counter: int(req.Counter),
	}

	_, err := s.service.CompleteTask(header, body)
	if err != nil {
		return nil, grpcStatus.Error(s.code(err.Code), err.Error())
	}

	return &grpcApi.CompleteTaskResponse{}, nil
}

func (s *server) HeartbeatTasks(ctx context.Context, req *grpcApi.HeartbeatTasksRequest) (*grpcApi.HeartbeatTasksResponse, error) {
	header := &service.Header{
		RequestId: req.RequestId,
	}

	if req.ProcessId == "" {
		return nil, grpcStatus.Error(codes.InvalidArgument, "task.process_id must be provided")
	}

	body := &service.HeartbeatTaskBody{
		ProcessId: req.ProcessId,
	}

	res, err := s.service.HeartbeatTasks(header, body)
	if err != nil {
		return nil, grpcStatus.Error(s.code(err.Code), err.Error())
	}

	return &grpcApi.HeartbeatTasksResponse{
		TasksAffected: res.TasksAffected,
	}, nil
}
