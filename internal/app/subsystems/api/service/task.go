package service

import (
	"errors"

	"github.com/resonatehq/resonate/internal/api"
	"github.com/resonatehq/resonate/internal/kernel/bus"
	"github.com/resonatehq/resonate/internal/kernel/t_api"
	"github.com/resonatehq/resonate/internal/util"
)

// CLAIM

func (s *Service) ClaimTask(header *Header, body *ClaimTaskBody) (*t_api.ClaimTaskResponse, error) {
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
		var resErr *t_api.ResonateError
		util.Assert(errors.As(cqe.Error, &resErr), "err must be a ResonateError")
		return nil, resErr
	}

	util.Assert(cqe.Completion.ClaimTask != nil, "response must not be nil")

	if api.IsRequestError(cqe.Completion.ClaimTask.Status) {
		return nil, api.HandleRequestError(cqe.Completion.ClaimTask.Status)
	}

	return cqe.Completion.ClaimTask, nil
}

// COMPLETE

func (s *Service) CompleteTask(header *Header, body *CompleteTaskBody) (*t_api.CompleteTaskResponse, error) {
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
		var resErr *t_api.ResonateError
		util.Assert(errors.As(cqe.Error, &resErr), "err must be a ResonateError")
		return nil, resErr
	}

	util.Assert(cqe.Completion.CompleteTask != nil, "response must not be nil")

	if api.IsRequestError(cqe.Completion.CompleteTask.Status) {
		return nil, api.HandleRequestError(cqe.Completion.CompleteTask.Status)
	}

	return cqe.Completion.CompleteTask, nil
}

// HEARTBEAT

func (s *Service) HeartbeatTasks(header *Header, body *HeartbeatTaskBody) (*t_api.HeartbeatTasksResponse, error) {
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
		var resErr *t_api.ResonateError
		util.Assert(errors.As(cqe.Error, &resErr), "err must be a ResonateError")
		return nil, resErr
	}

	util.Assert(cqe.Completion.HeartbeatTasks != nil, "response must not be nil")

	if api.IsRequestError(cqe.Completion.HeartbeatTasks.Status) {
		return nil, api.HandleRequestError(cqe.Completion.HeartbeatTasks.Status)
	}

	return cqe.Completion.HeartbeatTasks, nil
}
