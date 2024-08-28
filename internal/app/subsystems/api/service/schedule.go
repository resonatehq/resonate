package service

import (
	"fmt"

	"github.com/resonatehq/resonate/internal/kernel/bus"
	"github.com/resonatehq/resonate/internal/kernel/t_api"
	"github.com/resonatehq/resonate/internal/util"
)

// READ

func (s *Service) ReadSchedule(id string, header *Header) (*t_api.ReadScheduleResponse, *Error) {
	cq := make(chan *bus.CQE[t_api.Request, t_api.Response], 1)

	s.api.Enqueue(&bus.SQE[t_api.Request, t_api.Response]{
		Callback: s.sendOrPanic(cq),
		Submission: &t_api.Request{
			Kind: t_api.ReadSchedule,
			Tags: s.tags(header.RequestId, "ReadSchedule"),
			ReadSchedule: &t_api.ReadScheduleRequest{
				Id: id,
			},
		},
	})

	cqe := <-cq
	if cqe.Error != nil {
		return nil, ServerError(cqe.Error)
	}

	util.Assert(cqe.Completion.ReadSchedule != nil, "response must not be nil")
	return cqe.Completion.ReadSchedule, RequestError(cqe.Completion.Status())
}

// SEARCH

func (s *Service) SearchSchedules(header *Header, params *SearchSchedulesParams) (*t_api.SearchSchedulesResponse, *Error) {
	if params == nil {
		params = &SearchSchedulesParams{}
	}
	var searchSchedules *t_api.SearchSchedulesRequest
	if params.Cursor != nil && *params.Cursor != "" {
		cursor, err := t_api.NewCursor[t_api.SearchSchedulesRequest](*params.Cursor)
		if err != nil {
			return nil, RequestValidationError(err)
		}
		searchSchedules = cursor.Next
	} else {
		// set default query
		if params.Id == nil {
			params.Id = util.ToPointer("*")
		}

		// set default limit
		if params.Limit == nil || *params.Limit == 0 {
			params.Limit = util.ToPointer(100)
		}

		searchSchedules = &t_api.SearchSchedulesRequest{
			Id:    *params.Id,
			Tags:  params.Tags,
			Limit: *params.Limit,
		}
	}

	cq := make(chan *bus.CQE[t_api.Request, t_api.Response], 1)

	s.api.Enqueue(&bus.SQE[t_api.Request, t_api.Response]{
		Callback: s.sendOrPanic(cq),
		Submission: &t_api.Request{
			Kind:            t_api.SearchSchedules,
			Tags:            s.tags(header.RequestId, "SearchSchedules"),
			SearchSchedules: searchSchedules,
		},
	})

	cqe := <-cq
	if cqe.Error != nil {
		return nil, ServerError(cqe.Error)
	}

	util.Assert(cqe.Completion.SearchSchedules != nil, "response must not be nil")
	return cqe.Completion.SearchSchedules, RequestError(cqe.Completion.Status())
}

// CREATE

func (s *Service) CreateSchedule(header CreateScheduleHeader, body *CreateScheduleBody) (*t_api.CreateScheduleResponse, *Error) {
	cq := make(chan *bus.CQE[t_api.Request, t_api.Response], 1)

	// validation
	if err := validateSchedule(body.Cron); err != nil {
		return nil, RequestValidationError(err)
	}

	if err := validatePromiseId(body.PromiseId); err != nil {
		return nil, RequestValidationError(err)
	}

	s.api.Enqueue(&bus.SQE[t_api.Request, t_api.Response]{
		Callback: s.sendOrPanic(cq),
		Submission: &t_api.Request{
			Kind: t_api.CreateSchedule,
			Tags: s.tags(header.RequestId, "CreateSchedule"),
			CreateSchedule: &t_api.CreateScheduleRequest{
				Id:             body.Id,
				Description:    body.Description,
				Cron:           body.Cron,
				Tags:           body.Tags,
				PromiseId:      body.PromiseId,
				PromiseTimeout: body.PromiseTimeout,
				PromiseParam:   body.PromiseParam,
				PromiseTags:    body.PromiseTags,
				IdempotencyKey: header.IdempotencyKey,
			},
		},
	})

	cqe := <-cq
	if cqe.Error != nil {
		return nil, ServerError(cqe.Error)
	}

	util.Assert(cqe.Completion.CreateSchedule != nil, "response must not be nil")
	return cqe.Completion.CreateSchedule, RequestError(cqe.Completion.Status())
}

// DELETE

func (s *Service) DeleteSchedule(id string, header *Header) (*t_api.DeleteScheduleResponse, *Error) {
	cq := make(chan *bus.CQE[t_api.Request, t_api.Response], 1)

	s.api.Enqueue(&bus.SQE[t_api.Request, t_api.Response]{
		Callback: s.sendOrPanic(cq),
		Submission: &t_api.Request{
			Kind: t_api.DeleteSchedule,
			Tags: s.tags(header.RequestId, "DeleteSchedule"),
			DeleteSchedule: &t_api.DeleteScheduleRequest{
				Id: id,
			},
		},
	})

	cqe := <-cq
	if cqe.Error != nil {
		return nil, ServerError(cqe.Error)
	}

	util.Assert(cqe.Completion.DeleteSchedule != nil, "response must not be nil")
	return cqe.Completion.DeleteSchedule, RequestError(cqe.Completion.Status())
}

// validations

func validateSchedule(schedule string) error {
	// Validate schedule is valid cron expression
	if _, err := util.ParseCron(schedule); err != nil {
		return fmt.Errorf("invalid cron schedule: %v", err)
	}

	return nil
}

func validatePromiseId(promiseId string) error {
	if promiseId == "" {
		return fmt.Errorf("promiseId is required")
	}

	return nil
}
