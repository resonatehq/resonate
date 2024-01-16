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

	// PROMISES
	CreatePromise  *CreatePromiseResponse
	ReadPromise    *ReadPromiseResponse
	SearchPromises *SearchPromisesResponse
	CancelPromise  *CompletePromiseResponse
	ResolvePromise *CompletePromiseResponse
	RejectPromise  *CompletePromiseResponse

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
	HeartbeatLocks *HeartbeatLocksResponse
	ReleaseLock    *ReleaseLockResponse

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

type HeartbeatLocksResponse struct {
	Status ResponseStatus `json:"status"`
}

type ReleaseLockResponse struct {
	Status ResponseStatus `json:"status"`
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
	case CancelPromise:
		return fmt.Sprintf(
			"CancelPromise(status=%d, promise=%s)",
			r.CancelPromise.Status,
			r.CancelPromise.Promise,
		)
	case ResolvePromise:
		return fmt.Sprintf(
			"ResolvePromise(status=%d, promise=%s)",
			r.ResolvePromise.Status,
			r.ResolvePromise.Promise,
		)
	case RejectPromise:
		return fmt.Sprintf(
			"RejectPromise(status=%d, promise=%s)",
			r.RejectPromise.Status,
			r.RejectPromise.Promise,
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
	case HeartbeatLocks:
		return fmt.Sprintf(
			"HeartbeatLocks(status=%d)",
			r.HeartbeatLocks.Status,
		)
	case ReleaseLock:
		return fmt.Sprintf(
			"ReleaseLock(status=%d)",
			r.ReleaseLock.Status,
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
