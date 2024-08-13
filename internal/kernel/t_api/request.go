package t_api

import (
	"fmt"
	"strconv"

	"github.com/resonatehq/resonate/pkg/idempotency"
	"github.com/resonatehq/resonate/pkg/message"
	"github.com/resonatehq/resonate/pkg/promise"
)

type Request struct {
	Kind Kind
	Tags map[string]string

	// PROMISES
	ReadPromise     *ReadPromiseRequest
	SearchPromises  *SearchPromisesRequest
	CreatePromise   *CreatePromiseRequest
	CompletePromise *CompletePromiseRequest

	// CALLBACKS
	CreateCallback *CreateCallbackRequest

	// SCHEDULES
	ReadSchedule    *ReadScheduleRequest
	SearchSchedules *SearchSchedulesRequest
	CreateSchedule  *CreateScheduleRequest
	DeleteSchedule  *DeleteScheduleRequest

	// LOCKS
	AcquireLock    *AcquireLockRequest
	ReleaseLock    *ReleaseLockRequest
	HeartbeatLocks *HeartbeatLocksRequest

	// TASKS
	ClaimTask      *ClaimTaskRequest
	CompleteTask   *CompleteTaskRequest
	HeartbeatTasks *HeartbeatTasksRequest

	// ECHO
	Echo *EchoRequest
}

func (r *Request) Id() string {
	return r.Tags["request_id"]
}

// Promises

type ReadPromiseRequest struct {
	Id string `json:"id"`
}

type SearchPromisesRequest struct {
	Id     string            `json:"id"`
	States []promise.State   `json:"states"`
	Tags   map[string]string `json:"tags"`
	Limit  int               `json:"limit"`
	SortId *int64            `json:"sortId"`
}

type CreatePromiseRequest struct {
	Id             string            `json:"id"`
	IdempotencyKey *idempotency.Key  `json:"idemptencyKey,omitempty"`
	Strict         bool              `json:"strict"`
	Param          promise.Value     `json:"param,omitempty"`
	Timeout        int64             `json:"timeout"`
	Tags           map[string]string `json:"tags,omitempty"`
}

type CompletePromiseRequest struct {
	Id             string           `json:"id"`
	IdempotencyKey *idempotency.Key `json:"idemptencyKey,omitempty"`
	Strict         bool             `json:"strict"`
	State          promise.State    `json:"state"`
	Value          promise.Value    `json:"value,omitempty"`
}

type CancelPromiseRequest struct {
	Id             string           `json:"id"`
	IdempotencyKey *idempotency.Key `json:"idemptencyKey,omitempty"`
	Strict         bool             `json:"strict"`
	Value          promise.Value    `json:"value,omitempty"`
}

type ResolvePromiseRequest struct {
	Id             string           `json:"id"`
	IdempotencyKey *idempotency.Key `json:"idemptencyKey,omitempty"`
	Strict         bool             `json:"strict"`
	Value          promise.Value    `json:"value,omitempty"`
}

type RejectPromiseRequest struct {
	Id             string           `json:"id"`
	IdempotencyKey *idempotency.Key `json:"idemptencyKey,omitempty"`
	Strict         bool             `json:"strict"`
	Value          promise.Value    `json:"value,omitempty"`
}

// Callbacks

type CreateCallbackRequest struct {
	PromiseId string           `json:"promiseId"`
	Timeout   int64            `json:"timeout"`
	Message   *message.Message `json:"message"`
}

// Schedules

type ReadScheduleRequest struct {
	Id string `json:"id"`
}

type SearchSchedulesRequest struct {
	Id     string            `json:"id"`
	Tags   map[string]string `json:"tags"`
	Limit  int               `json:"limit"`
	SortId *int64            `json:"sortId"`
}

type CreateScheduleRequest struct {
	Id             string            `json:"id"`
	Description    string            `json:"desc,omitempty"`
	Cron           string            `json:"cron"`
	Tags           map[string]string `json:"tags,omitempty"`
	PromiseId      string            `json:"promiseId"`
	PromiseTimeout int64             `json:"promiseTimeout"`
	PromiseParam   promise.Value     `json:"promiseParam,omitempty"`
	PromiseTags    map[string]string `json:"promiseTags,omitempty"`
	IdempotencyKey *idempotency.Key  `json:"idemptencyKey,omitempty"`
}

type DeleteScheduleRequest struct {
	Id string `json:"id"`
}

// Locks

type AcquireLockRequest struct {
	ResourceId           string `json:"resourceId"`
	ExecutionId          string `json:"executionId"`
	ProcessId            string `json:"processId"`
	ExpiryInMilliseconds int64  `json:"expiryInMilliseconds"`
}

type ReleaseLockRequest struct {
	ResourceId  string `json:"resourceId"`
	ExecutionId string `json:"executionId"`
}

type HeartbeatLocksRequest struct {
	ProcessId string `json:"processId"`
}

// Tasks

type ClaimTaskRequest struct {
	Id        string `json:"id"`
	ProcessId string `json:"processId"`
	Counter   int    `json:"counter"`
	Frequency int    `json:"frequency"`
}

type CompleteTaskRequest struct {
	Id      string `json:"id"`
	Counter int    `json:"counter"`
}

type HeartbeatTasksRequest struct {
	ProcessId string `json:"processId"`
}

// Echo

type EchoRequest struct {
	Data string `json:"data"`
}

func (r *Request) String() string {
	switch r.Kind {
	// PROMISES
	case ReadPromise:
		return fmt.Sprintf(
			"ReadPromise(id=%s)",
			r.ReadPromise.Id,
		)
	case SearchPromises:
		sortId := "<nil>"
		if r.SearchPromises.SortId != nil {
			sortId = strconv.FormatInt(*r.SearchPromises.SortId, 10)
		}

		return fmt.Sprintf(
			"SearchPromises(id=%s, states=%s, tags=%s, limit=%d, sortId=%s)",
			r.SearchPromises.Id,
			r.SearchPromises.States,
			r.SearchPromises.Tags,
			r.SearchPromises.Limit,
			sortId,
		)
	case CreatePromise:
		return fmt.Sprintf(
			"CreatePromise(id=%s, idempotencyKey=%s, strict=%t, timeout=%d)",
			r.CreatePromise.Id,
			r.CreatePromise.IdempotencyKey,
			r.CreatePromise.Strict,
			r.CreatePromise.Timeout,
		)
	case CompletePromise:
		return fmt.Sprintf(
			"CompletePromise(id=%s, idempotencyKey=%s, strict=%t, state=%s)",
			r.CompletePromise.Id,
			r.CompletePromise.IdempotencyKey,
			r.CompletePromise.Strict,
			r.CompletePromise.State,
		)

	// CALLBACKS
	case CreateCallback:
		return fmt.Sprintf(
			"CreateCallback(promiseId=%s, message=%s)",
			r.CreateCallback.PromiseId,
			r.CreateCallback.Message,
		)

	// SCHEDULES
	case ReadSchedule:
		return fmt.Sprintf(
			"ReadSchedule(id=%s)",
			r.ReadSchedule.Id,
		)
	case SearchSchedules:
		sortId := "<nil>"
		if r.SearchSchedules.SortId != nil {
			sortId = strconv.FormatInt(*r.SearchSchedules.SortId, 10)
		}

		return fmt.Sprintf(
			"SearchSchedules(id=%s, tags=%s, limit=%d, sortId=%s)",
			r.SearchSchedules.Id,
			r.SearchSchedules.Tags,
			r.SearchSchedules.Limit,
			sortId,
		)
	case CreateSchedule:
		return fmt.Sprintf(
			"CreateSchedule(id=%s, idempotencyKey=%s, description=%s, cron=%s)",
			r.CreateSchedule.Id,
			r.CreateSchedule.IdempotencyKey,
			r.CreateSchedule.Description,
			r.CreateSchedule.Cron,
		)
	case DeleteSchedule:
		return fmt.Sprintf(
			"DeleteSchedule(id=%s)",
			r.DeleteSchedule.Id,
		)

	// LOCKS
	case AcquireLock:
		return fmt.Sprintf(
			"AcquireLock(resourceId=%s, executionId=%s, processId=%s, expiryInMilliseconds=%d)",
			r.AcquireLock.ResourceId,
			r.AcquireLock.ExecutionId,
			r.AcquireLock.ProcessId,
			r.AcquireLock.ExpiryInMilliseconds,
		)
	case ReleaseLock:
		return fmt.Sprintf(
			"ReleaseLock(resourceId=%s, executionId=%s)",
			r.ReleaseLock.ResourceId,
			r.ReleaseLock.ExecutionId,
		)
	case HeartbeatLocks:
		return fmt.Sprintf(
			"HeartbeatLocks(processId=%s)",
			r.HeartbeatLocks.ProcessId,
		)

	// TASKS
	case ClaimTask:
		return fmt.Sprintf(
			"ClaimTask(id=%s, processId=%s, counter=%d, frequency=%d)",
			r.ClaimTask.Id,
			r.ClaimTask.ProcessId,
			r.ClaimTask.Counter,
			r.ClaimTask.Frequency,
		)
	case CompleteTask:
		return fmt.Sprintf(
			"CompleteTask(id=%s, counter=%d)",
			r.CompleteTask.Id,
			r.CompleteTask.Counter,
		)
	case HeartbeatTasks:
		return fmt.Sprintf(
			"HeartbeatTasks(processId=%s)",
			r.HeartbeatTasks.ProcessId,
		)

	// ECHO
	case Echo:
		return fmt.Sprintf(
			"Echo(data=%s)",
			r.Echo.Data,
		)
	default:
		return "Request"
	}
}
