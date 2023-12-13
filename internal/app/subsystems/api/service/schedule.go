package service

import (
	"errors"

	"github.com/resonatehq/resonate/internal/api"
	"github.com/resonatehq/resonate/internal/kernel/bus"
	"github.com/resonatehq/resonate/internal/kernel/t_api"
	"github.com/resonatehq/resonate/internal/util"
)

// CREATE

func (s *Service) CreateSchedule(body *CreateScheduleBody) (*t_api.CreateScheduleResponse, error) {
	cq := make(chan *bus.CQE[t_api.Request, t_api.Response], 1)

	s.api.Enqueue(&bus.SQE[t_api.Request, t_api.Response]{
		Metadata: s.metadata("header.RequestId", "create-schedule"),
		Submission: &t_api.Request{
			Kind: t_api.CreateSchedule,
			CreateSchedule: &t_api.CreateScheduleRequest{
				Id:   body.Id,
				Desc: &body.Desc,
				Cron: body.Cron,
			},
		},
		Callback: s.sendOrPanic(cq),
	})

	cqe := <-cq
	if cqe.Error != nil {
		var resErr *t_api.ResonateError
		util.Assert(errors.As(cqe.Error, &resErr), "err must be a ResonateError")
		return nil, resErr
	}

	util.Assert(cqe.Completion.CreateSchedule != nil, "response must not be nil")

	if api.IsRequestError(cqe.Completion.CreateSchedule.Status) {
		return nil, api.HandleRequestError(cqe.Completion.CreateSchedule.Status)
	}

	return cqe.Completion.CreateSchedule, nil
}

// READ

func (s *Service) ReadSchedule(id string) (*t_api.ReadScheduleResponse, error) {
	cq := make(chan *bus.CQE[t_api.Request, t_api.Response], 1)

	s.api.Enqueue(&bus.SQE[t_api.Request, t_api.Response]{
		Metadata: s.metadata("header.RequestId", "read-schedule"),
		Submission: &t_api.Request{
			Kind: t_api.ReadSchedule,
			ReadSchedule: &t_api.ReadScheduleRequest{
				Id: id,
			},
		},
		Callback: s.sendOrPanic(cq),
	})

	cqe := <-cq
	if cqe.Error != nil {
		var resErr *t_api.ResonateError
		util.Assert(errors.As(cqe.Error, &resErr), "err must be a ResonateError")
		return nil, resErr
	}

	util.Assert(cqe.Completion.ReadSchedule != nil, "response must not be nil")

	if api.IsRequestError(cqe.Completion.ReadSchedule.Status) {
		return nil, api.HandleRequestError(cqe.Completion.ReadSchedule.Status)
	}

	return cqe.Completion.ReadSchedule, nil
}

// DELETE

func (s *Service) DeleteSchedule(id string) (*t_api.DeleteScheduleResponse, error) {
	cq := make(chan *bus.CQE[t_api.Request, t_api.Response], 1)

	s.api.Enqueue(&bus.SQE[t_api.Request, t_api.Response]{
		Metadata: s.metadata("header.RequestId", "delete-schedule"),
		Submission: &t_api.Request{
			Kind: t_api.DeleteSchedule,
			DeleteSchedule: &t_api.DeleteScheduleRequest{
				Id: id,
			},
		},
		Callback: s.sendOrPanic(cq),
	})

	cqe := <-cq
	if cqe.Error != nil {
		var resErr *t_api.ResonateError
		util.Assert(errors.As(cqe.Error, &resErr), "err must be a ResonateError")
		return nil, resErr
	}

	util.Assert(cqe.Completion.DeleteSchedule != nil, "response must not be nil")

	if api.IsRequestError(cqe.Completion.DeleteSchedule.Status) {
		return nil, api.HandleRequestError(cqe.Completion.DeleteSchedule.Status)
	}

	return cqe.Completion.DeleteSchedule, nil
}
