package api

import (
	"errors"
	"fmt"
	"strings"

	"github.com/google/uuid"
	i_api "github.com/resonatehq/resonate/internal/api"
	"github.com/resonatehq/resonate/internal/kernel/bus"
	"github.com/resonatehq/resonate/internal/kernel/t_api"
	"github.com/resonatehq/resonate/internal/util"
	"github.com/resonatehq/resonate/pkg/promise"
)

type API struct {
	i_api.API
	protocol string
}

func New(api i_api.API, protocol string) *API {
	return &API{
		API:      api,
		protocol: protocol,
	}
}

func (a *API) Process(id string, submission *t_api.Request) (*t_api.Response, *Error) {
	if id == "" {
		id = uuid.New().String()
	}
	if submission.Tags == nil {
		submission.Tags = map[string]string{}
	}

	// inject tags
	submission.Tags["id"] = id
	submission.Tags["name"] = submission.Kind.String()
	submission.Tags["protocol"] = a.protocol

	// completion queue
	cq := make(chan *bus.CQE[t_api.Request, t_api.Response], 1)

	a.EnqueueSQE(&bus.SQE[t_api.Request, t_api.Response]{
		Id:         id,
		Submission: submission,
		Callback: func(res *t_api.Response, err error) {
			defer close(cq)

			select {
			case cq <- &bus.CQE[t_api.Request, t_api.Response]{
				Id:         id,
				Completion: res,
				Error:      err,
			}:
			default:
				panic("response channel must not block")
			}
		},
	})

	// cqe := <-cq
	cqe := a.DequeueCQE(cq)
	if cqe.Error != nil {
		return nil, ServerError(cqe.Error)
	}

	if status := cqe.Completion.Status(); !status.IsSuccessful() {
		return nil, RequestError(status)
	}

	return cqe.Completion, nil
}

// Helper functions

func (a *API) SearchPromises(id string, state string, tags map[string]string, limit int, cursor string) (*t_api.SearchPromisesRequest, *Error) {
	if cursor != "" {
		cursor, err := t_api.NewCursor[t_api.SearchPromisesRequest](cursor)
		if err != nil {
			return nil, RequestValidationError(err)
		}

		return cursor.Next, nil
	}

	// validate id
	if id == "" {
		return nil, RequestValidationError(errors.New("The field id is required."))
	}

	// set states
	var states []promise.State
	switch strings.ToLower(state) {
	case "":
		states = []promise.State{
			promise.Pending,
			promise.Resolved,
			promise.Rejected,
			promise.Timedout,
			promise.Canceled,
		}
	case "pending":
		states = []promise.State{
			promise.Pending,
		}
	case "resolved":
		states = []promise.State{
			promise.Resolved,
		}
	case "rejected":
		states = []promise.State{
			promise.Rejected,
			promise.Timedout,
			promise.Canceled,
		}
	default:
		return nil, RequestValidationError(errors.New("The field state must be one of pending, resolved, rejected."))
	}

	// set default tags
	if tags == nil {
		tags = map[string]string{}
	}

	// set default limit
	if limit == 0 {
		limit = 100
	}

	// validate limit
	if limit < 1 || limit > 100 {
		return nil, RequestValidationError(errors.New("The field limit must be between 1 and 100."))
	}

	return &t_api.SearchPromisesRequest{
		Id:     id,
		States: states,
		Tags:   tags,
		Limit:  limit,
	}, nil
}

func (a *API) SearchSchedules(id string, tags map[string]string, limit int, cursor string) (*t_api.SearchSchedulesRequest, *Error) {
	if cursor != "" {
		cursor, err := t_api.NewCursor[t_api.SearchSchedulesRequest](cursor)
		if err != nil {
			return nil, RequestValidationError(err)
		}

		return cursor.Next, nil
	}

	// validate id
	if id == "" {
		return nil, RequestValidationError(errors.New("The field id is required."))
	}

	// set default tags
	if tags == nil {
		tags = map[string]string{}
	}

	// validate limit
	if limit < 1 || limit > 100 {
		return nil, RequestValidationError(errors.New("The field limit must be between 1 and 100."))
	}

	return &t_api.SearchSchedulesRequest{
		Id:    id,
		Tags:  tags,
		Limit: limit,
	}, nil
}

func (a *API) ValidateCron(cron string) *Error {
	if _, err := util.ParseCron(cron); err != nil {
		return RequestValidationError(errors.New("The field cron must be a valid cron expression."))
	}

	return nil
}

func (a *API) TaskProcessId(id string, counter int) string {
	return fmt.Sprintf("%s/%d", id, counter)
}
