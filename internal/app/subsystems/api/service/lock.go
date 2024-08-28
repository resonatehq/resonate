package service

import (
	"github.com/resonatehq/resonate/internal/kernel/bus"
	"github.com/resonatehq/resonate/internal/kernel/t_api"
	"github.com/resonatehq/resonate/internal/util"
)

// ACQUIRE

func (s *Service) AcquireLock(header *Header, body *AcquireLockBody) (*t_api.AcquireLockResponse, *Error) {
	util.Assert(body.ResourceId != "", "lock.resource_id must be provided")
	util.Assert(body.ExecutionId != "", "lock.execution_id must be provided")
	util.Assert(body.ProcessId != "", "lock.process_id must be provided")
	util.Assert(body.ExpiryInMilliseconds != 0, "lock.expiry_in_milliseconds must be provided")

	acquireLock := &t_api.AcquireLockRequest{
		ResourceId:           body.ResourceId,
		ExecutionId:          body.ExecutionId,
		ProcessId:            body.ProcessId,
		ExpiryInMilliseconds: body.ExpiryInMilliseconds,
	}

	cq := make(chan *bus.CQE[t_api.Request, t_api.Response], 1)

	s.api.Enqueue(&bus.SQE[t_api.Request, t_api.Response]{
		Callback: s.sendOrPanic(cq),
		Submission: &t_api.Request{
			Kind:        t_api.AcquireLock,
			Tags:        s.tags(header.RequestId, "AcquireLock"),
			AcquireLock: acquireLock,
		},
	})

	cqe := <-cq
	if cqe.Error != nil {
		return nil, ServerError(cqe.Error)
	}

	util.Assert(cqe.Completion.AcquireLock != nil, "response must not be nil")
	return cqe.Completion.AcquireLock, RequestError(cqe.Completion.Status())
}

// HEARTBEAT

func (s *Service) Heartbeat(header *Header, body *HeartbeatBody) (*t_api.HeartbeatLocksResponse, *Error) {
	util.Assert(body.ProcessId != "", "process_id must be provided")

	HeartbeatLocks := &t_api.HeartbeatLocksRequest{
		ProcessId: body.ProcessId,
	}

	cq := make(chan *bus.CQE[t_api.Request, t_api.Response], 1)

	s.api.Enqueue(&bus.SQE[t_api.Request, t_api.Response]{
		Callback: s.sendOrPanic(cq),
		Submission: &t_api.Request{
			Kind:           t_api.HeartbeatLocks,
			Tags:           s.tags(header.RequestId, "HeartbeatLocks"),
			HeartbeatLocks: HeartbeatLocks,
		},
	})

	cqe := <-cq
	if cqe.Error != nil {
		return nil, ServerError(cqe.Error)
	}

	util.Assert(cqe.Completion.HeartbeatLocks != nil, "response must not be nil")
	return cqe.Completion.HeartbeatLocks, RequestError(cqe.Completion.Status())
}

// RELEASE

func (s *Service) ReleaseLock(header *Header, body *ReleaseLockBody) (*t_api.ReleaseLockResponse, *Error) {
	util.Assert(body.ResourceId != "", "resource_id must be provided")
	util.Assert(body.ExecutionId != "", "execution_id must be provided")

	releaseLock := &t_api.ReleaseLockRequest{
		ResourceId:  body.ResourceId,
		ExecutionId: body.ExecutionId,
	}

	cq := make(chan *bus.CQE[t_api.Request, t_api.Response], 1)

	s.api.Enqueue(&bus.SQE[t_api.Request, t_api.Response]{
		Callback: s.sendOrPanic(cq),
		Submission: &t_api.Request{
			Kind:        t_api.ReleaseLock,
			Tags:        s.tags(header.RequestId, "ReleaseLock"),
			ReleaseLock: releaseLock,
		},
	})

	cqe := <-cq
	if cqe.Error != nil {
		return nil, ServerError(cqe.Error)
	}

	util.Assert(cqe.Completion.ReleaseLock != nil, "response must not be nil")
	return cqe.Completion.ReleaseLock, RequestError(cqe.Completion.Status())
}
