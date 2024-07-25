package t_api

import (
	"fmt"

	"github.com/resonatehq/resonate/pkg/lock"
	"github.com/resonatehq/resonate/pkg/promise"
	"github.com/resonatehq/resonate/pkg/schedule"
	"github.com/resonatehq/resonate/pkg/subscription"
)

type Response struct {
	Kind Kind
	Tags map[string]string

	// Promises
	CreatePromise   *CreatePromiseResponse
	ReadPromise     *ReadPromiseResponse
	SearchPromises  *SearchPromisesResponse
	CompletePromise *CompletePromiseResponse

	// SCHEDULES
	CreateSchedule  *CreateScheduleResponse
	SearchSchedules *SearchSchedulesResponse
	ReadSchedule    *ReadScheduleResponse
	DeleteSchedule  *DeleteScheduleResponse

	// SUBSCRIPTIONS
	ReadSubscriptions  *ReadSubscriptionsResponse
	CreateSubscription *CreateSubscriptionResponse
	DeleteSubscription *DeleteSubscriptionResponse

	// LOCKS
	AcquireLock    *AcquireLockResponse
	ReleaseLock    *ReleaseLockResponse
	HeartbeatLocks *HeartbeatLocksResponse

	// TASKS
	ClaimTask    *ClaimTaskResponse
	CompleteTask *CompleteTaskResponse

	// ECHO
	Echo *EchoResponse
}

// Promises

type CreatePromiseResponse struct {
	Status  ResponseStatus   `json:"status"`
	Promise *promise.Promise `json:"promise,omitempty"`
}

type ReadPromiseResponse struct {
	Status  ResponseStatus   `json:"status"`
	Promise *promise.Promise `json:"promise,omitempty"`
}

type SearchPromisesResponse struct {
	Status   ResponseStatus                 `json:"status"`
	Cursor   *Cursor[SearchPromisesRequest] `json:"cursor,omitempty"`
	Promises []*promise.Promise             `json:"promises,omitempty"`
}

type CompletePromiseResponse struct {
	Status  ResponseStatus   `json:"status"`
	Promise *promise.Promise `json:"promise,omitempty"`
}

// Schedules

type CreateScheduleResponse struct {
	Status   ResponseStatus     `json:"status"`
	Schedule *schedule.Schedule `json:"schedule,omitempty"`
}

type SearchSchedulesResponse struct {
	Status    ResponseStatus                  `json:"status"`
	Cursor    *Cursor[SearchSchedulesRequest] `json:"cursor,omitempty"`
	Schedules []*schedule.Schedule            `json:"schedules,omitempty"`
}

type ReadScheduleResponse struct {
	Status   ResponseStatus     `json:"status"`
	Schedule *schedule.Schedule `json:"schedule,omitempty"`
}

type DeleteScheduleResponse struct {
	Status ResponseStatus `json:"status"`
}

// Subscriptions

type ReadSubscriptionsResponse struct {
	Status        ResponseStatus                    `json:"status"`
	Cursor        *Cursor[ReadSubscriptionsRequest] `json:"cursor,omitempty"`
	Subscriptions []*subscription.Subscription      `json:"subscriptions,omitempty"`
}

type CreateSubscriptionResponse struct {
	Status       ResponseStatus             `json:"status"`
	Subscription *subscription.Subscription `json:"subscription,omitempty"`
}

type DeleteSubscriptionResponse struct {
	Status ResponseStatus `json:"status"`
}

type EchoResponse struct {
	Data string `json:"data"`
}

// Locks

type AcquireLockResponse struct {
	Status ResponseStatus `json:"status"`
	Lock   *lock.Lock     `json:"lock,omitempty"`
}

type ReleaseLockResponse struct {
	Status ResponseStatus `json:"status"`
}

type HeartbeatLocksResponse struct {
	Status        ResponseStatus `json:"status"`
	LocksAffected int64          `json:"locksAffected"`
}

// Tasks

// ClaimTaskResponse is a response to a ClaimTaskRequest.
// It contains the status of the claim operation and the promise that was claimed.
// The promise param is the input to the task. Think RPC call.
// Promise value is the response. -- dst with claim and complete is like threst,, it;s the enuqueing that needs work..
type ClaimTaskResponse struct {
	Status  ResponseStatus   `json:"status"`
	Promise *promise.Promise `json:"promise,omitempty"`
}

// CompleteTaskResponse is just an acknowledgement that the task was completed.
type CompleteTaskResponse struct {
	Status ResponseStatus `json:"status"`
}

func (r *Response) Id() string {
	return r.Tags["request_id"]
}

func (r *Response) Status() ResponseStatus {
	switch r.Kind {
	// PROMISES
	case ReadPromise:
		return r.ReadPromise.Status
	case SearchPromises:
		return r.SearchPromises.Status
	case CreatePromise:
		return r.CreatePromise.Status
	case CompletePromise:
		return r.CompletePromise.Status
	// SCHEDULES
	case ReadSchedule:
		return r.ReadSchedule.Status
	case SearchSchedules:
		return r.SearchSchedules.Status
	case CreateSchedule:
		return r.CreateSchedule.Status
	case DeleteSchedule:
		return r.DeleteSchedule.Status
	// SUBSCRIPTIONS
	case ReadSubscriptions:
		return r.ReadSubscriptions.Status
	case CreateSubscription:
		return r.CreateSubscription.Status
	case DeleteSubscription:
		return r.DeleteSubscription.Status
	// LOCKS
	case AcquireLock:
		return r.AcquireLock.Status
	case ReleaseLock:
		return r.ReleaseLock.Status
	case HeartbeatLocks:
		return r.HeartbeatLocks.Status
	default:
		return 0
	}
}

func (r *Response) String() string {
	switch r.Kind {
	// PROMISES
	case ReadPromise:
		return fmt.Sprintf(
			"ReadPromise(status=%d, promise=%s)",
			r.ReadPromise.Status,
			r.ReadPromise.Promise,
		)
	case SearchPromises:
		return fmt.Sprintf(
			"SearchPromises(status=%d, cursor=%s, promises=%s)",
			r.SearchPromises.Status,
			r.SearchPromises.Cursor,
			r.SearchPromises.Promises,
		)
	case CreatePromise:
		return fmt.Sprintf(
			"CreatePromise(status=%d, promise=%s)",
			r.CreatePromise.Status,
			r.CreatePromise.Promise,
		)
	case CompletePromise:
		return fmt.Sprintf(
			"CompletePromise(status=%d, promise=%s)",
			r.CompletePromise.Status,
			r.CompletePromise.Promise,
		)

	// SCHEDULES
	case ReadSchedule:
		return fmt.Sprintf(
			"ReadSchedule(status=%d, schedule=%s)",
			r.ReadSchedule.Status,
			r.ReadSchedule.Schedule,
		)
	case SearchSchedules:
		return fmt.Sprintf(
			"SearchSchedules(status=%d, cursor=%s, schedules=%s)",
			r.SearchSchedules.Status,
			r.SearchSchedules.Cursor,
			r.SearchSchedules.Schedules,
		)
	case CreateSchedule:
		return fmt.Sprintf(
			"CreateSchedule(status=%d, schedule=%s)",
			r.CreateSchedule.Status,
			r.CreateSchedule.Schedule,
		)
	case DeleteSchedule:
		return fmt.Sprintf(
			"DeleteSchedule(status=%d)",
			r.DeleteSchedule.Status,
		)

	// SUBSCRIPTIONS
	case ReadSubscriptions:
		return fmt.Sprintf(
			"ReadSubscriptions(status=%d, subscriptions=%s)",
			r.ReadSubscriptions.Status,
			r.ReadSubscriptions.Subscriptions,
		)
	case CreateSubscription:
		return fmt.Sprintf(
			"CreateSubscription(status=%d, subscriptions=%s)",
			r.CreateSubscription.Status,
			r.CreateSubscription.Subscription,
		)
	case DeleteSubscription:
		return fmt.Sprintf(
			"DeleteSubscription(status=%d)",
			r.DeleteSubscription.Status,
		)

	// LOCKS
	case AcquireLock:
		return fmt.Sprintf(
			"AcquireLock(status=%d, lock=%s)",
			r.AcquireLock.Status,
			r.AcquireLock.Lock,
		)
	case ReleaseLock:
		return fmt.Sprintf(
			"ReleaseLock(status=%d)",
			r.ReleaseLock.Status,
		)
	case HeartbeatLocks:
		return fmt.Sprintf(
			"HeartbeatLocks(status=%d, locksAffected=%d)",
			r.HeartbeatLocks.Status,
			r.HeartbeatLocks.LocksAffected,
		)

	// TASKS
	case ClaimTask:
		return fmt.Sprintf(
			"ClaimTask(status=%d, promise=%s)",
			r.ClaimTask.Status,
			r.ClaimTask.Promise,
		)
	case CompleteTask:
		return fmt.Sprintf(
			"CompleteTask(status=%d)",
			r.CompleteTask.Status,
		)

	// ECHO
	case Echo:
		return fmt.Sprintf(
			"Echo(data=%s)",
			r.Echo.Data,
		)
	default:
		return "Response"
	}
}
