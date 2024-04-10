package service

import (
	"errors"
	"strings"

	"github.com/resonatehq/resonate/internal/api"
	"github.com/resonatehq/resonate/internal/kernel/bus"
	"github.com/resonatehq/resonate/internal/kernel/t_api"
	"github.com/resonatehq/resonate/internal/util"
	"github.com/resonatehq/resonate/pkg/promise"
)

// CLAIM

func (s *Service) ClaimTask(header *Header, body *ClaimTaskBody) (*t_api.ClaimTaskResponse, error) {
	util.Assert(body != nil, "body must be provided")
	util.Assert(body.TaskId != "", "task_id must be provided")
	util.Assert(body.Counter >= 0, "counter must be greater than or equal to 0")
	util.Assert(body.ProcessId != "", "process_id must be provided")
	util.Assert(body.ExecutionId != "", "execution_id must be provided")
	util.Assert(body.ExpiryInMilliseconds > 0, "expiry_in_milliseconds must be greater than 0")

	claimTask := &t_api.ClaimTaskRequest{
		TaskId:               body.TaskId,
		Counter:              body.Counter,
		ProcessId:            body.ProcessId,
		ExecutionId:          body.ExecutionId,
		ExpiryInMilliseconds: body.ExpiryInMilliseconds,
	}

	cq := make(chan *bus.CQE[t_api.Request, t_api.Response], 1)

	s.api.Enqueue(&bus.SQE[t_api.Request, t_api.Response]{
		Metadata: s.metadata(header.RequestId, "claim-task"),
		Submission: &t_api.Request{
			Kind:      t_api.ClaimTask,
			ClaimTask: claimTask,
		},
		Callback: s.sendOrPanic(cq),
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
	util.Assert(body != nil, "body must be provided")
	util.Assert(body.TaskId != "", "task_id must be provided")
	util.Assert(body.Counter >= 0, "counter must be greater than or equal to 0")
	util.Assert(body.ExecutionId != "", "execution_id must be provided")

	var state promise.State
	switch strings.ToUpper(body.State) {
	case promise.Resolved.String():
		state = promise.Resolved
	case promise.Rejected.String():
		state = promise.Rejected
	default:
		panic("invalid state provided to CompleteTask") // todo: return api error not this.
	}

	completeTask := &t_api.CompleteTaskRequest{
		TaskId:      body.TaskId,
		Counter:     body.Counter,
		ExecutionId: body.ExecutionId,
		State:       state,
		Value:       body.Value,
	}

	cq := make(chan *bus.CQE[t_api.Request, t_api.Response], 1)

	s.api.Enqueue(&bus.SQE[t_api.Request, t_api.Response]{
		Metadata: s.metadata(header.RequestId, "complete-task"),
		Submission: &t_api.Request{
			Kind:         t_api.CompleteTask,
			CompleteTask: completeTask,
		},
		Callback: s.sendOrPanic(cq),
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
