package t_api

import (
	"fmt"
	"strconv"

	"github.com/resonatehq/resonate/pkg/idempotency"
	"github.com/resonatehq/resonate/pkg/promise"

	"github.com/resonatehq/resonate/pkg/subscription"
)

type Request struct {
	Kind Kind
	Tags map[string]string

	// PROMISES
	ReadPromise     *ReadPromiseRequest
	SearchPromises  *SearchPromisesRequest
	CreatePromise   *CreatePromiseRequest
	CompletePromise *CompletePromiseRequest

	// SCHEDULES
	ReadSchedule    *ReadScheduleRequest
	SearchSchedules *SearchSchedulesRequest
	CreateSchedule  *CreateScheduleRequest
	DeleteSchedule  *DeleteScheduleRequest

	// SUBSCRIPTIONS
	ReadSubscriptions  *ReadSubscriptionsRequest
	CreateSubscription *CreateSubscriptionRequest
	DeleteSubscription *DeleteSubscriptionRequest

	// LOCKS
	AcquireLock    *AcquireLockRequest
	HeartbeatLocks *HeartbeatLocksRequest
	ReleaseLock    *ReleaseLockRequest

	// TASKS
	ClaimTask    *ClaimTaskRequest
	CompleteTask *CompleteTaskRequest

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

// Subscriptions

type ReadSubscriptionsRequest struct {
	PromiseId string `json:"promiseId"`
	Limit     int    `json:"limit"`
	SortId    *int64 `json:"sortId"`
}

type CreateSubscriptionRequest struct {
	Id          string                    `json:"id"`
	PromiseId   string                    `json:"promiseId"`
	Url         string                    `json:"url"`
	RetryPolicy *subscription.RetryPolicy `json:"retryPolicy"`
}

type DeleteSubscriptionRequest struct {
	Id        string `json:"id"`
	PromiseId string `json:"promiseId"`
}

type EchoRequest struct {
	Data string `json:"data"`
}

// Locks

type AcquireLockRequest struct {
	ResourceId           string `json:"resourceId"`
	ProcessId            string `json:"processId"`
	ExecutionId          string `json:"executionId"`
	ExpiryInMilliseconds int64  `json:"expiryInMilliseconds"`
}

type HeartbeatLocksRequest struct {
	ProcessId string `json:"processId"`
}

type ReleaseLockRequest struct {
	ResourceId  string `json:"resourceId"`
	ExecutionId string `json:"executionId"`
}

// Tasks

// ClaimTaskRequest allows a process that received a pending task
// to claim ownership of the task for processing. It identifies the
// specific task to claim using the unique taskId and counter
// originally provided with the pending task. Claiming the task locks
// it from being claimed by other workers for a set period of time
// so that duplicate work is avoided. Once claimed, the process should
// carry out the task work then release the claim upon completion or
// failure to allow the task to be retried.
type ClaimTaskRequest struct {
	TaskId               string `json:"taskId"`
	Counter              int    `json:"counter"`
	ProcessId            string `json:"processId"`
	ExecutionId          string `json:"executionId"`
	ExpiryInMilliseconds int64  `json:"expiryInMilliseconds"`
}

// CompleteTaskRequest allows a process that has claimed a task to mark
// the task as complete (resolved or rejected). It identifies the
// specific task to complete using the unique taskId and counter originally
// provided with the pending task. Completing the task releases the claim
// on the task.
type CompleteTaskRequest struct {
	TaskId      string        `json:"taskId"`
	Counter     int           `json:"counter"`
	ExecutionId string        `json:"executionId"`
	State       promise.State `json:"state"`
	Value       promise.Value `json:"value"`
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
			"CreatePromise(id=%s, idempotencyKey=%s, timeout=%d, strict=%t)",
			r.CreatePromise.Id,
			r.CreatePromise.IdempotencyKey,
			r.CreatePromise.Timeout,
			r.CreatePromise.Strict,
		)
	case CompletePromise:
		return fmt.Sprintf(
			"CompletePromise(id=%s, state=%s, idempotencyKey=%s, strict=%t)",
			r.CompletePromise.Id,
			r.CompletePromise.State,
			r.CompletePromise.IdempotencyKey,
			r.CompletePromise.Strict,
		)
	// SCHEDULES
	case ReadSchedule:
		return fmt.Sprintf(
			"ReadSchedule(id=%s)",
			r.ReadSchedule.Id,
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

	// SUBSCRIPTIONS
	case ReadSubscriptions:
		sortId := "<nil>"
		if r.ReadSubscriptions.SortId != nil {
			sortId = strconv.FormatInt(*r.ReadSubscriptions.SortId, 10)
		}

		return fmt.Sprintf(
			"ReadSubscriptions(promiseId=%s, limit=%d, sortId=%s)",
			r.ReadSubscriptions.PromiseId,
			r.ReadSubscriptions.Limit,
			sortId,
		)
	case CreateSubscription:
		return fmt.Sprintf(
			"CreateSubscription(id=%s, promiseId=%s, url=%s)",
			r.CreateSubscription.Id,
			r.CreateSubscription.PromiseId,
			r.CreateSubscription.Url,
		)
	case DeleteSubscription:
		return fmt.Sprintf(
			"DeleteSubscription(id=%s, promiseId=%s)",
			r.DeleteSubscription.Id,
			r.DeleteSubscription.PromiseId,
		)

	// LOCKS
	case AcquireLock:
		return fmt.Sprintf(
			"AcquireLock(resourceId=%s, processId=%s, executionId=%s, expiryInMilliseconds=%d)",
			r.AcquireLock.ResourceId,
			r.AcquireLock.ProcessId,
			r.AcquireLock.ExecutionId,
			r.AcquireLock.ExpiryInMilliseconds,
		)
	case HeartbeatLocks:
		return fmt.Sprintf(
			"HeartbeatLocks(processId=%s)",
			r.HeartbeatLocks.ProcessId,
		)

	case ReleaseLock:
		return fmt.Sprintf(
			"ReleaseLock(resourceId=%s, executionId=%s)",
			r.ReleaseLock.ResourceId,
			r.ReleaseLock.ExecutionId,
		)

	// TASKS
	case ClaimTask:
		return fmt.Sprintf(
			"ClaimTask(taskId=%s, counter=%d, processId=%s, executionId=%s, expiryInMilliseconds=%d)",
			r.ClaimTask.TaskId,
			r.ClaimTask.Counter,
			r.ClaimTask.ProcessId,
			r.ClaimTask.ExecutionId,
			r.ClaimTask.ExpiryInMilliseconds,
		)
	case CompleteTask:
		return fmt.Sprintf(
			"CompleteTask(taskId=%s, counter=%d, executionId=%s, state=%s, value=%s)",
			r.CompleteTask.TaskId,
			r.CompleteTask.Counter,
			r.CompleteTask.ExecutionId,
			r.CompleteTask.State,
			r.CompleteTask.Value,
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
