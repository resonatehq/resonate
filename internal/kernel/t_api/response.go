package t_api

import (
	"fmt"

	"github.com/resonatehq/resonate/pkg/callback"
	"github.com/resonatehq/resonate/pkg/lock"
	"github.com/resonatehq/resonate/pkg/promise"
	"github.com/resonatehq/resonate/pkg/schedule"
	"github.com/resonatehq/resonate/pkg/task"
)

type Response struct {
	Kind Kind
	Tags map[string]string

	// Promises
	CreatePromise   *CreatePromiseResponse
	ReadPromise     *ReadPromiseResponse
	SearchPromises  *SearchPromisesResponse
	CompletePromise *CompletePromiseResponse

	// CALLBACKS
	CreateCallback *CreateCallbackResponse

	// SCHEDULES
	CreateSchedule  *CreateScheduleResponse
	SearchSchedules *SearchSchedulesResponse
	ReadSchedule    *ReadScheduleResponse
	DeleteSchedule  *DeleteScheduleResponse

	// LOCKS
	AcquireLock    *AcquireLockResponse
	ReleaseLock    *ReleaseLockResponse
	HeartbeatLocks *HeartbeatLocksResponse

	// TASKS
	ClaimTask      *ClaimTaskResponse
	CompleteTask   *CompleteTaskResponse
	HeartbeatTasks *HeartbeatTasksResponse

	// ECHO
	Echo *EchoResponse
}

// Promises

type CreatePromiseResponse struct {
	Status   StatusCode         `json:"status"`
	Promise  *promise.Promise   `json:"promise,omitempty"`
	Task     *task.Task         `json:"task,omitempty"`
	Callback *callback.Callback `json:"callback,omitempty"`
}

type ReadPromiseResponse struct {
	Status  StatusCode       `json:"status"`
	Promise *promise.Promise `json:"promise,omitempty"`
}

type SearchPromisesResponse struct {
	Status   StatusCode                     `json:"status"`
	Cursor   *Cursor[SearchPromisesRequest] `json:"cursor,omitempty"`
	Promises []*promise.Promise             `json:"promises,omitempty"`
}

type CompletePromiseResponse struct {
	Status  StatusCode       `json:"status"`
	Promise *promise.Promise `json:"promise,omitempty"`
}

// Callbacks

type CreateCallbackResponse struct {
	Status   StatusCode         `json:"status"`
	Promise  *promise.Promise   `json:"promise,omitempty"`
	Callback *callback.Callback `json:"callback,omitempty"`
}

// Schedules

type CreateScheduleResponse struct {
	Status   StatusCode         `json:"status"`
	Schedule *schedule.Schedule `json:"schedule,omitempty"`
}

type SearchSchedulesResponse struct {
	Status    StatusCode                      `json:"status"`
	Cursor    *Cursor[SearchSchedulesRequest] `json:"cursor,omitempty"`
	Schedules []*schedule.Schedule            `json:"schedules,omitempty"`
}

type ReadScheduleResponse struct {
	Status   StatusCode         `json:"status"`
	Schedule *schedule.Schedule `json:"schedule,omitempty"`
}

type DeleteScheduleResponse struct {
	Status StatusCode `json:"status"`
}

// Locks

type AcquireLockResponse struct {
	Status StatusCode `json:"status"`
	Lock   *lock.Lock `json:"lock,omitempty"`
}

type ReleaseLockResponse struct {
	Status StatusCode `json:"status"`
}

type HeartbeatLocksResponse struct {
	Status        StatusCode `json:"status"`
	LocksAffected int64      `json:"locksAffected"`
}

// Tasks

type ClaimTaskResponse struct {
	Status StatusCode `json:"status"`
	Task   *task.Task `json:"task,omitempty"`
}

type CompleteTaskResponse struct {
	Status StatusCode `json:"status"`
	Task   *task.Task `json:"task,omitempty"`
}

type HeartbeatTasksResponse struct {
	Status        StatusCode `json:"status"`
	TasksAffected int64      `json:"tasksAffected"`
}

// Echo

type EchoResponse struct {
	Data string `json:"data"`
}

func (r *Response) Status() StatusCode {
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
	// CALLBACKS
	case CreateCallback:
		return r.CreateCallback.Status
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
	// TASKS
	case ClaimTask:
		return r.ClaimTask.Status
	case CompleteTask:
		return r.CompleteTask.Status
	case HeartbeatTasks:
		return r.HeartbeatTasks.Status
	// ECHO
	case Echo:
		return 200
	default:
		panic(fmt.Sprintf("invalid response kind: %s", r.Kind))
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
			"CreatePromise(status=%d, promise=%s, task=%s, callback=%s)",
			r.CreatePromise.Status,
			r.CreatePromise.Promise,
			r.CreatePromise.Task,
			r.CreatePromise.Callback,
		)
	case CompletePromise:
		return fmt.Sprintf(
			"CompletePromise(status=%d, promise=%s)",
			r.CompletePromise.Status,
			r.CompletePromise.Promise,
		)

	// CALLBACKS
	case CreateCallback:
		return fmt.Sprintf(
			"CreateCallback(status=%d, promise=%s, callback=%s)",
			r.CreateCallback.Status,
			r.CreateCallback.Promise,
			r.CreateCallback.Callback,
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

	// TASKS
	case ClaimTask:
		return fmt.Sprintf(
			"ClaimTask(status=%d, task=%s)",
			r.ClaimTask.Status,
			r.ClaimTask.Task,
		)
	case CompleteTask:
		return fmt.Sprintf(
			"CompleteTask(status=%d, task=%s)",
			r.CompleteTask.Status,
			r.CompleteTask.Task,
		)
	case HeartbeatTasks:
		return fmt.Sprintf(
			"HeartbeatTasks(status=%d, tasksAffected=%d)",
			r.HeartbeatTasks.Status,
			r.HeartbeatTasks.TasksAffected,
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
