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
	util.Assert(body.Frequency != 0, "task.frequency must be greater than zero")

	claimTask := &t_api.ClaimTaskRequest{
		Id:        body.Id,
		Counter:   body.Counter,
		Frequency: body.Frequency,
	}

	cq := make(chan *bus.CQE[t_api.Request, t_api.Response], 1)

	req := &t_api.Request{
		Kind:      t_api.ClaimTask,
		Tags:      s.tags(header.RequestId, "ClaimTask"),
		ClaimTask: claimTask,
	}

	s.api.Enqueue(req, s.sendOrPanic(cq))

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
	completeTask := &t_api.CompleteTaskRequest{
		Id:      body.Id,
		Counter: body.Counter,
	}

	cq := make(chan *bus.CQE[t_api.Request, t_api.Response], 1)

	req := &t_api.Request{
		Kind:         t_api.CompleteTask,
		Tags:         s.tags(header.RequestId, "CompleteTask"),
		CompleteTask: completeTask,
	}

	s.api.Enqueue(req, s.sendOrPanic(cq))

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

func (s *Service) HeartbeatTask(header *Header, body *HeartbeatTaskBody) (*t_api.HeartbeatTaskResponse, error) {
	heartbeatTask := &t_api.HeartbeatTaskRequest{
		Id:      body.Id,
		Counter: body.Counter,
	}

	cq := make(chan *bus.CQE[t_api.Request, t_api.Response], 1)

	req := &t_api.Request{
		Kind:          t_api.HeartbeatTask,
		Tags:          s.tags(header.RequestId, "HeartbeatTask"),
		HeartbeatTask: heartbeatTask,
	}

	s.api.Enqueue(req, s.sendOrPanic(cq))

	cqe := <-cq
	if cqe.Error != nil {
		var resErr *t_api.ResonateError
		util.Assert(errors.As(cqe.Error, &resErr), "err must be a ResonateError")
		return nil, resErr
	}

	util.Assert(cqe.Completion.HeartbeatTask != nil, "response must not be nil")

	if api.IsRequestError(cqe.Completion.HeartbeatTask.Status) {
		return nil, api.HandleRequestError(cqe.Completion.HeartbeatTask.Status)
	}

	return cqe.Completion.HeartbeatTask, nil
}
