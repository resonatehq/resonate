package t_api

import (
	"encoding/json"
	"fmt"

	"github.com/resonatehq/resonate/internal/util"
	"github.com/resonatehq/resonate/pkg/idempotency"
	"github.com/resonatehq/resonate/pkg/promise"
)

type Request struct {
	Kind Kind
	Tags map[string]string

	// PROMISES
	ReadPromise          *ReadPromiseRequest
	SearchPromises       *SearchPromisesRequest
	CreatePromise        *CreatePromiseRequest
	CreatePromiseAndTask *CreatePromiseAndTaskRequest
	CompletePromise      *CompletePromiseRequest

	// CALLBACKS
	CreateCallback *CreateCallbackRequest

	// SUBSCRIPTIONS
	CreateSubscription *CreateSubscriptionRequest

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

// Promises

type ReadPromiseRequest struct {
	Id string `json:"id"`
}

func (r *ReadPromiseRequest) String() string {
	return fmt.Sprintf("ReadPromise(id=%s)", r.Id)
}

type SearchPromisesRequest struct {
	Id     string            `json:"id"`
	States []promise.State   `json:"states"`
	Tags   map[string]string `json:"tags"`
	Limit  int               `json:"limit"`
	SortId *int64            `json:"sortId"`
}

func (r *SearchPromisesRequest) String() string {
	return fmt.Sprintf("SearchPromises(id=%s, states=%v, tags=%v, limit=%d, sortId=%d)", r.Id, r.States, r.Tags, r.Limit, util.SafeDeref(r.SortId))
}

type CreatePromiseRequest struct {
	Id             string            `json:"id"`
	IdempotencyKey *idempotency.Key  `json:"idemptencyKey,omitempty"`
	Strict         bool              `json:"strict"`
	Param          promise.Value     `json:"param,omitempty"`
	Timeout        int64             `json:"timeout"`
	Tags           map[string]string `json:"tags,omitempty"`
}

func (r *CreatePromiseRequest) String() string {
	return fmt.Sprintf("CreatePromise(id=%s, idempotencyKey=%v, strict=%v, param=%v, timeout=%d, tags=%v)", r.Id, r.IdempotencyKey, r.Strict, r.Param, r.Timeout, r.Tags)
}

type CreatePromiseAndTaskRequest struct {
	Promise *CreatePromiseRequest
	Task    *CreateTaskRequest
}

func (r *CreatePromiseAndTaskRequest) String() string {
	return fmt.Sprintf("CreatePromiseAndTask(promise=%v, task=%v)", r.Promise, r.Task)
}

type CompletePromiseRequest struct {
	Id             string           `json:"id"`
	IdempotencyKey *idempotency.Key `json:"idemptencyKey,omitempty"`
	Strict         bool             `json:"strict"`
	State          promise.State    `json:"state"`
	Value          promise.Value    `json:"value,omitempty"`
}

func (r *CompletePromiseRequest) String() string {
	return fmt.Sprintf("CompletePromise(id=%s, idempotencyKey=%v, strict=%v, state=%v, value=%v)", r.Id, r.IdempotencyKey, r.Strict, r.State, r.Value)
}

// Callbacks

type CreateCallbackRequest struct {
	Id            string          `json:"id"`
	PromiseId     string          `json:"promiseId"`
	RootPromiseId string          `json:"rootPromiseId"` // TODO: we should be able to know this from the promise itself
	Timeout       int64           `json:"timeout"`
	Recv          json.RawMessage `json:"recv"`
}

func (r *CreateCallbackRequest) String() string {
	return fmt.Sprintf("CreateCallback(id=%s, promiseId=%s, rootPromiseId=%s, timeout=%d, recv=%s)", r.Id, r.PromiseId, r.RootPromiseId, r.Timeout, r.Recv)
}

// Subscription

type CreateSubscriptionRequest struct {
	Id        string          `json:"id"`
	PromiseId string          `json:"promiseId"`
	Timeout   int64           `json:"timeout"`
	Recv      json.RawMessage `json:"recv"`
}

func (r *CreateSubscriptionRequest) String() string {
	return fmt.Sprintf("CreateSubscription(id=%s, promiseId=%s, recv=%s)", r.Id, r.PromiseId, r.Recv)
}

// Schedules

type ReadScheduleRequest struct {
	Id string `json:"id"`
}

func (r *ReadScheduleRequest) String() string {
	return fmt.Sprintf("ReadSchedule(id=%s)", r.Id)
}

type SearchSchedulesRequest struct {
	Id     string            `json:"id"`
	Tags   map[string]string `json:"tags"`
	Limit  int               `json:"limit"`
	SortId *int64            `json:"sortId"`
}

func (r *SearchSchedulesRequest) String() string {
	return fmt.Sprintf("SearchSchedules(id=%s, tags=%v, limit=%d, sortId=%d)", r.Id, r.Tags, r.Limit, util.SafeDeref(r.SortId))
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

func (r *CreateScheduleRequest) String() string {
	return fmt.Sprintf(
		"CreateSchedule(id=%s, desc=%s, cron=%s, tags=%v, promiseId=%s, promiseTimeout=%d, promiseParam=%v, promiseTags=%v, idempotencyKey=%v)",
		r.Id,
		r.Description,
		r.Cron,
		r.Tags,
		r.PromiseId,
		r.PromiseTimeout,
		r.PromiseParam,
		r.PromiseTags,
		r.IdempotencyKey,
	)
}

type DeleteScheduleRequest struct {
	Id string `json:"id"`
}

func (r *DeleteScheduleRequest) String() string {
	return fmt.Sprintf("DeleteSchedule(id=%s)", r.Id)
}

// Locks

type AcquireLockRequest struct {
	ResourceId  string `json:"resourceId"`
	ExecutionId string `json:"executionId"`
	ProcessId   string `json:"processId"`
	Ttl         int64  `json:"ttl"`
}

func (r *AcquireLockRequest) String() string {
	return fmt.Sprintf(
		"AcquireLock(resourceId=%s, executionId=%s, processId=%s, ttl=%d)",
		r.ResourceId,
		r.ExecutionId,
		r.ProcessId,
		r.Ttl,
	)
}

type ReleaseLockRequest struct {
	ResourceId  string `json:"resourceId"`
	ExecutionId string `json:"executionId"`
}

func (r *ReleaseLockRequest) String() string {
	return fmt.Sprintf("ReleaseLock(resourceId=%s, executionId=%s)", r.ResourceId, r.ExecutionId)
}

type HeartbeatLocksRequest struct {
	ProcessId string `json:"processId"`
}

func (r *HeartbeatLocksRequest) String() string {
	return fmt.Sprintf("HeartbeatLocks(processId=%s)", r.ProcessId)
}

// Tasks

type CreateTaskRequest struct {
	PromiseId string `json:"promiseId"`
	ProcessId string `json:"processId"`
	Ttl       int    `json:"ttl"`
	Timeout   int64  `json:"timeout"`
}

func (r *CreateTaskRequest) String() string {
	return fmt.Sprintf(
		"CreateTask(promiseId=%s, processId=%s, ttl=%d, timeout=%d)",
		r.PromiseId,
		r.ProcessId,
		r.Ttl,
		r.Timeout,
	)
}

type ClaimTaskRequest struct {
	Id        string `json:"id"`
	Counter   int    `json:"counter"`
	ProcessId string `json:"processId"`
	Ttl       int    `json:"ttl"`
}

func (r *ClaimTaskRequest) String() string {
	return fmt.Sprintf("ClaimTask(id=%s, counter=%d, processId=%s, ttl=%d)", r.Id, r.Counter, r.ProcessId, r.Ttl)
}

type CompleteTaskRequest struct {
	Id      string `json:"id"`
	Counter int    `json:"counter"`
}

func (r *CompleteTaskRequest) String() string {
	return fmt.Sprintf("CompleteTask(id=%s, counter=%d)", r.Id, r.Counter)
}

type HeartbeatTasksRequest struct {
	ProcessId string `json:"processId"`
}

func (r *HeartbeatTasksRequest) String() string {
	return fmt.Sprintf("HeartbeatTasks(processId=%s)", r.ProcessId)
}

// Echo

type EchoRequest struct {
	Data string `json:"data"`
}

func (r *EchoRequest) String() string {
	return fmt.Sprintf("Echo(data=%s)", r.Data)
}

func (r *Request) String() string {
	switch r.Kind {
	// PROMISES
	case ReadPromise:
		return r.ReadPromise.String()
	case SearchPromises:
		return r.SearchPromises.String()
	case CreatePromise:
		return r.CreatePromise.String()
	case CreatePromiseAndTask:
		return r.CreatePromiseAndTask.String()
	case CompletePromise:
		return r.CompletePromise.String()

	// CALLBACKS
	case CreateCallback:
		return r.CreateCallback.String()

	// SUBSCRIPTION
	case CreateSubscription:
		return r.CreateSubscription.String()

	// SCHEDULES
	case ReadSchedule:
		return r.ReadSchedule.String()
	case SearchSchedules:
		return r.SearchSchedules.String()
	case CreateSchedule:
		return r.CreateSchedule.String()
	case DeleteSchedule:
		return r.DeleteSchedule.String()

	// LOCKS
	case AcquireLock:
		return r.AcquireLock.String()
	case ReleaseLock:
		return r.ReleaseLock.String()
	case HeartbeatLocks:
		return r.HeartbeatLocks.String()

	// TASKS
	case ClaimTask:
		return r.ClaimTask.String()
	case CompleteTask:
		return r.CompleteTask.String()
	case HeartbeatTasks:
		return r.HeartbeatTasks.String()

	// ECHO
	case Echo:
		return r.Echo.String()

	default:
		return "Request"
	}
}
