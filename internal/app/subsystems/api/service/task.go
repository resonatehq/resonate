package service

import (
	"github.com/resonatehq/resonate/internal/kernel/bus"
	"github.com/resonatehq/resonate/internal/kernel/t_api"
	"github.com/resonatehq/resonate/internal/util"
)

// CLAIM

func (s *Service) ClaimTask(header *Header, body *ClaimTaskBody) (*t_api.ClaimTaskResponse, *Error) {
	cq := make(chan *bus.CQE[t_api.Request, t_api.Response], 1)

	s.api.Enqueue(&bus.SQE[t_api.Request, t_api.Response]{
		Callback: s.sendOrPanic(cq),
		Submission: &t_api.Request{
			Kind: t_api.ClaimTask,
			Tags: s.tags(header.RequestId, "ClaimTask"),
			ClaimTask: &t_api.ClaimTaskRequest{
				Id:        body.Id,
				ProcessId: body.ProcessId,
				Counter:   body.Counter,
				Frequency: body.Frequency,
			},
		},
	})

	cqe := <-cq
	if cqe.Error != nil {
		return nil, ServerError(cqe.Error)
	}

	util.Assert(cqe.Completion.ClaimTask != nil, "response must not be nil")
	return cqe.Completion.ClaimTask, RequestError(cqe.Completion.Status())
}

// COMPLETE

func (s *Service) CompleteTask(header *Header, body *CompleteTaskBody) (*t_api.CompleteTaskResponse, *Error) {
	cq := make(chan *bus.CQE[t_api.Request, t_api.Response], 1)

	s.api.Enqueue(&bus.SQE[t_api.Request, t_api.Response]{
		Callback: s.sendOrPanic(cq),
		Submission: &t_api.Request{
			Kind: t_api.CompleteTask,
			Tags: s.tags(header.RequestId, "CompleteTask"),
			CompleteTask: &t_api.CompleteTaskRequest{
				Id:      body.Id,
				Counter: body.Counter,
			},
		},
	})

	cqe := <-cq
	if cqe.Error != nil {
		return nil, ServerError(cqe.Error)
	}

	util.Assert(cqe.Completion.CompleteTask != nil, "response must not be nil")
	return cqe.Completion.CompleteTask, RequestError(cqe.Completion.Status())
}

// HEARTBEAT

func (s *Service) HeartbeatTasks(header *Header, body *HeartbeatTaskBody) (*t_api.HeartbeatTasksResponse, *Error) {
	cq := make(chan *bus.CQE[t_api.Request, t_api.Response], 1)

	s.api.Enqueue(&bus.SQE[t_api.Request, t_api.Response]{
		Callback: s.sendOrPanic(cq),
		Submission: &t_api.Request{
			Kind: t_api.HeartbeatTasks,
			Tags: s.tags(header.RequestId, "HeartbeatTasks"),
			HeartbeatTasks: &t_api.HeartbeatTasksRequest{
				ProcessId: body.ProcessId,
			},
		},
	})

	cqe := <-cq
	if cqe.Error != nil {
		return nil, ServerError(cqe.Error)
	}

	util.Assert(cqe.Completion.HeartbeatTasks != nil, "response must not be nil")
	return cqe.Completion.HeartbeatTasks, RequestError(cqe.Completion.Status())
}
