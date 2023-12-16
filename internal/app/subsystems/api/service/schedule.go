package service

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/resonatehq/resonate/internal/api"
	"github.com/resonatehq/resonate/internal/kernel/bus"
	"github.com/resonatehq/resonate/internal/kernel/t_api"
	"github.com/resonatehq/resonate/internal/util"
)

// CREATE

func (s *Service) CreateSchedule(header CreateScheduleHeader, body *CreateScheduleBody) (*t_api.CreateScheduleResponse, error) {
	cq := make(chan *bus.CQE[t_api.Request, t_api.Response], 1)

	// validation
	if err := validateSchedule(body.Cron); err != nil {
		return nil, api.HandleValidationError(err)
	}

	if err := validatePromiseId(body.PromiseId); err != nil {
		return nil, api.HandleValidationError(err)
	}

	s.api.Enqueue(&bus.SQE[t_api.Request, t_api.Response]{
		Metadata: s.metadata(header.RequestId, "create-schedule"),
		Submission: &t_api.Request{
			Kind: t_api.CreateSchedule,
			CreateSchedule: &t_api.CreateScheduleRequest{
				Id:             body.Id,
				Desc:           &body.Desc,
				Cron:           body.Cron,
				PromiseId:      body.PromiseId,
				PromiseParam:   util.SafeDeref(body.PromiseParam),
				PromiseTimeout: body.PromiseTimeout,
				IdempotencyKey: header.IdempotencyKey,
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

// validations

func validateSchedule(schedule string) error {
	// Validate schedule is valid cron expression.
	scheduler, err := util.ParseCron(schedule)
	if err != nil {
		return fmt.Errorf("invalid cron schedule: %v", err)
	}

	// Validate time isn't in the past.
	now := time.Now()
	next := scheduler.Next(now)
	if next.Before(now) {
		return fmt.Errorf("schedule is in the past")
	}

	return nil
}

func validatePromiseId(promiseId string) error {
	if promiseId == "" {
		return fmt.Errorf("promiseId is required")
	}

	// Check required syntax
	// todo: if and only if
	if !strings.Contains(promiseId, "{{") || !strings.Contains(promiseId, "}}") {
		return fmt.Errorf("promiseId must contain a templated variable")
	}
	if !strings.Contains(promiseId, "{{.timestamp}}") {
		return fmt.Errorf("promiseId must contain the {{.timeout}} template variable")
	}

	return nil
}
