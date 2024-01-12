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
	acquireLock := &t_api.AcquireLockRequest{
		ResourceId:  body.ResourceId,
		ProcessId:   body.ProcessId,
		ExecutionId: body.ExecutionId,
		Timeout:     body.Timeout,
	}

	cq := make(chan *bus.CQE[t_api.Request, t_api.Response], 1)

	s.api.Enqueue(&bus.SQE[t_api.Request, t_api.Response]{
		Metadata: s.metadata(header.RequestId, "acquire-lock"),
		Submission: &t_api.Request{
			Kind:        t_api.AcquireLock,
			AcquireLock: acquireLock,
		},
		Callback: s.sendOrPanic(cq),
	})

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
	HeartbeatLocks := &t_api.HeartbeatLocksRequest{
		ProcessId: body.ProcessId,
		Timeout:   body.Timeout,
	}

	cq := make(chan *bus.CQE[t_api.Request, t_api.Response], 1)

	s.api.Enqueue(&bus.SQE[t_api.Request, t_api.Response]{
		Metadata: s.metadata(header.RequestId, "heartbeat-locks"),
		Submission: &t_api.Request{
			Kind:           t_api.HeartbeatLocks,
			HeartbeatLocks: HeartbeatLocks,
		},
		Callback: s.sendOrPanic(cq),
	})

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
	releaseLock := &t_api.ReleaseLockRequest{
		ResourceId:  body.ResourceId,
		ExecutionId: body.ExecutionId,
	}

	cq := make(chan *bus.CQE[t_api.Request, t_api.Response], 1)

	s.api.Enqueue(&bus.SQE[t_api.Request, t_api.Response]{
		Metadata: s.metadata(header.RequestId, "release-lock"),
		Submission: &t_api.Request{
			Kind:        t_api.ReleaseLock,
			ReleaseLock: releaseLock,
		},
		Callback: s.sendOrPanic(cq),
	})

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
