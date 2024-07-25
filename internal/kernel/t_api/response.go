package t_api

import (
	"fmt"

	"github.com/resonatehq/resonate/pkg/lock"
	"github.com/resonatehq/resonate/pkg/promise"
	"github.com/resonatehq/resonate/pkg/schedule"
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

	// LOCKS
	AcquireLock    *AcquireLockResponse
	ReleaseLock    *ReleaseLockResponse
	HeartbeatLocks *HeartbeatLocksResponse

	// ECHO
	Echo *EchoResponse
}

func (r *Response) Id() string {
	return r.Tags["request_id"]
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

// Echo

type EchoResponse struct {
	Data string `json:"data"`
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
