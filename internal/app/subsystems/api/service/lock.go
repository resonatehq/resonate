package service

import (
	"errors"

	"github.com/resonatehq/resonate/internal/api"
	"github.com/resonatehq/resonate/internal/kernel/bus"
	"github.com/resonatehq/resonate/internal/kernel/t_api"
	"github.com/resonatehq/resonate/internal/util"
)

// ACQUIRE

func (s *Service) AcquireLock(header *Header, body *AcquireLockBody) (*t_api.AcquireLockResponse, error) {
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

	req := &t_api.Request{
		Kind:        t_api.AcquireLock,
		Tags:        s.tags(header.RequestId, "AcquireLock"),
		AcquireLock: acquireLock,
	}

	s.api.Enqueue(req, s.sendOrPanic(cq))

	cqe := <-cq
	if cqe.Error != nil {
		var resErr *t_api.ResonateError
		util.Assert(errors.As(cqe.Error, &resErr), "err must be a ResonateError")
		return nil, resErr
	}

	util.Assert(cqe.Completion.AcquireLock != nil, "response must not be nil")

	if api.IsRequestError(cqe.Completion.AcquireLock.Status) {
		return nil, api.HandleRequestError(cqe.Completion.AcquireLock.Status)
	}

	return cqe.Completion.AcquireLock, nil
}

// HEARTBEAT

func (s *Service) Heartbeat(header *Header, body *HeartbeatBody) (*t_api.HeartbeatLocksResponse, error) {
	util.Assert(body.ProcessId != "", "process_id must be provided")

	HeartbeatLocks := &t_api.HeartbeatLocksRequest{
		ProcessId: body.ProcessId,
	}

	cq := make(chan *bus.CQE[t_api.Request, t_api.Response], 1)

	req := &t_api.Request{
		Kind:           t_api.HeartbeatLocks,
		Tags:           s.tags(header.RequestId, "HeartbeatLocks"),
		HeartbeatLocks: HeartbeatLocks,
	}

	s.api.Enqueue(req, s.sendOrPanic(cq))

	cqe := <-cq
	if cqe.Error != nil {
		var resErr *t_api.ResonateError
		util.Assert(errors.As(cqe.Error, &resErr), "err must be a ResonateError")
		return nil, resErr
	}

	util.Assert(cqe.Completion.HeartbeatLocks != nil, "response must not be nil")

	if api.IsRequestError(cqe.Completion.HeartbeatLocks.Status) {
		return nil, api.HandleRequestError(cqe.Completion.HeartbeatLocks.Status)
	}

	return cqe.Completion.HeartbeatLocks, nil
}

// RELEASE

func (s *Service) ReleaseLock(header *Header, body *ReleaseLockBody) (*t_api.ReleaseLockResponse, error) {
	util.Assert(body.ResourceId != "", "resource_id must be provided")
	util.Assert(body.ExecutionId != "", "execution_id must be provided")

	releaseLock := &t_api.ReleaseLockRequest{
		ResourceId:  body.ResourceId,
		ExecutionId: body.ExecutionId,
	}

	cq := make(chan *bus.CQE[t_api.Request, t_api.Response], 1)

	req := &t_api.Request{
		Kind:        t_api.ReleaseLock,
		Tags:        s.tags(header.RequestId, "ReleaseLock"),
		ReleaseLock: releaseLock,
	}

	s.api.Enqueue(req, s.sendOrPanic(cq))

	cqe := <-cq
	if cqe.Error != nil {
		var resErr *t_api.ResonateError
		util.Assert(errors.As(cqe.Error, &resErr), "err must be a ResonateError")
		return nil, resErr
	}

	util.Assert(cqe.Completion.ReleaseLock != nil, "response must not be nil")

	if api.IsRequestError(cqe.Completion.ReleaseLock.Status) {
		return nil, api.HandleRequestError(cqe.Completion.ReleaseLock.Status)
	}

	return cqe.Completion.ReleaseLock, nil
}
