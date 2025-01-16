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
	ReadPromise          *ReadPromiseResponse
	SearchPromises       *SearchPromisesResponse
	CreatePromise        *CreatePromiseResponse
	CreatePromiseAndTask *CreatePromiseAndTaskResponse
	CompletePromise      *CompletePromiseResponse

	// CALLBACKS
	CreateCallback *CreateCallbackResponse

	// NOTIFY
	CreateNotify *CreateNotifyResponse

	// SCHEDULES
	ReadSchedule    *ReadScheduleResponse
	SearchSchedules *SearchSchedulesResponse
	CreateSchedule  *CreateScheduleResponse
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

type ReadPromiseResponse struct {
	Status  StatusCode       `json:"status"`
	Promise *promise.Promise `json:"promise,omitempty"`
}

func (r *ReadPromiseResponse) String() string {
	return fmt.Sprintf("ReadPromise(status=%d, promise=%v)", r.Status, r.Promise)
}

type SearchPromisesResponse struct {
	Status   StatusCode                     `json:"status"`
	Promises []*promise.Promise             `json:"promises,omitempty"`
	Cursor   *Cursor[SearchPromisesRequest] `json:"cursor,omitempty"`
}

func (r *SearchPromisesResponse) String() string {
	return fmt.Sprintf("SearchPromises(status=%d, promises=%v, cursor=%v)", r.Status, r.Promises, r.Cursor)
}

type CreatePromiseResponse struct {
	Status  StatusCode       `json:"status"`
	Promise *promise.Promise `json:"promise,omitempty"`
}

func (r *CreatePromiseResponse) String() string {
	return fmt.Sprintf("CreatePromise(status=%d, promise=%v)", r.Status, r.Promise)
}

type CreatePromiseAndTaskResponse struct {
	Status  StatusCode       `json:"status"`
	Promise *promise.Promise `json:"promise,omitempty"`
	Task    *task.Task       `json:"task,omitempty"`
}

func (r *CreatePromiseAndTaskResponse) String() string {
	return fmt.Sprintf("CreatePromiseAndTask(status=%d, promise=%v, task=%v)", r.Status, r.Promise, r.Task)
}

type CompletePromiseResponse struct {
	Status  StatusCode       `json:"status"`
	Promise *promise.Promise `json:"promise,omitempty"`
}

func (r *CompletePromiseResponse) String() string {
	return fmt.Sprintf("CompletePromise(status=%d, promise=%v)", r.Status, r.Promise)
}

// Callbacks

type CreateCallbackResponse struct {
	Status   StatusCode         `json:"status"`
	Promise  *promise.Promise   `json:"promise,omitempty"`
	Callback *callback.Callback `json:"callback,omitempty"`
}

func (r *CreateCallbackResponse) String() string {
	return fmt.Sprintf("CreateCallback(status=%d, promise=%v, callback=%v)", r.Status, r.Promise, r.Callback)
}

// Notify

type CreateNotifyResponse struct {
	Status   StatusCode         `json:"status"`
	Promise  *promise.Promise   `json:"promise,omitempty"`
	Callback *callback.Callback `json:"callback,omitempty"`
}

func (r *CreateNotifyResponse) String() string {
	return fmt.Sprintf("CreateNotify(status=%d, promise=%v, callback=%v)", r.Status, r.Promise, r.Callback)
}

// Schedules

type ReadScheduleResponse struct {
	Status   StatusCode         `json:"status"`
	Schedule *schedule.Schedule `json:"schedule,omitempty"`
}

func (r *ReadScheduleResponse) String() string {
	return fmt.Sprintf("ReadSchedule(status=%d, schedule=%v)", r.Status, r.Schedule)
}

type SearchSchedulesResponse struct {
	Status    StatusCode                      `json:"status"`
	Schedules []*schedule.Schedule            `json:"schedules,omitempty"`
	Cursor    *Cursor[SearchSchedulesRequest] `json:"cursor,omitempty"`
}

func (r *SearchSchedulesResponse) String() string {
	return fmt.Sprintf("SearchSchedules(status=%d, schedules=%v, cursor=%v)", r.Status, r.Schedules, r.Cursor)
}

type CreateScheduleResponse struct {
	Status   StatusCode         `json:"status"`
	Schedule *schedule.Schedule `json:"schedule,omitempty"`
}

func (r *CreateScheduleResponse) String() string {
	return fmt.Sprintf("CreateSchedule(status=%d, schedule=%v)", r.Status, r.Schedule)
}

type DeleteScheduleResponse struct {
	Status StatusCode `json:"status"`
}

func (r *DeleteScheduleResponse) String() string {
	return fmt.Sprintf("DeleteSchedule(status=%d)", r.Status)
}

// Locks

type AcquireLockResponse struct {
	Status StatusCode `json:"status"`
	Lock   *lock.Lock `json:"lock,omitempty"`
}

func (r *AcquireLockResponse) String() string {
	return fmt.Sprintf("AcquireLock(status=%d, lock=%v)", r.Status, r.Lock)
}

type ReleaseLockResponse struct {
	Status StatusCode `json:"status"`
}

func (r *ReleaseLockResponse) String() string {
	return fmt.Sprintf("ReleaseLock(status=%d)", r.Status)
}

type HeartbeatLocksResponse struct {
	Status        StatusCode `json:"status"`
	LocksAffected int64      `json:"locksAffected"`
}

func (r *HeartbeatLocksResponse) String() string {
	return fmt.Sprintf("HeartbeatLocks(status=%d, locksAffected=%d)", r.Status, r.LocksAffected)
}

// Tasks

type ClaimTaskResponse struct {
	Status          StatusCode       `json:"status"`
	Task            *task.Task       `json:"task,omitempty"`
	RootPromise     *promise.Promise `json:"rootPromise,omitempty"`
	LeafPromise     *promise.Promise `json:"leafPromise,omitempty"`
	RootPromiseHref string           `json:"rootPromiseHref,omitempty"`
	LeafPromiseHref string           `json:"leafPromiseHref,omitempty"`
}

func (r *ClaimTaskResponse) String() string {
	return fmt.Sprintf("ClaimTask(status=%d, task=%v)", r.Status, r.Task)
}

type CompleteTaskResponse struct {
	Status StatusCode `json:"status"`
	Task   *task.Task `json:"task,omitempty"`
}

func (r *CompleteTaskResponse) String() string {
	return fmt.Sprintf("CompleteTask(status=%d, task=%v)", r.Status, r.Task)
}

type HeartbeatTasksResponse struct {
	Status        StatusCode `json:"status"`
	TasksAffected int64      `json:"tasksAffected"`
}

func (r *HeartbeatTasksResponse) String() string {
	return fmt.Sprintf("HeartbeatTasks(status=%d, tasksAffected=%d)", r.Status, r.TasksAffected)
}

// Echo

type EchoResponse struct {
	Data string `json:"data"`
}

func (r *EchoResponse) String() string {
	return fmt.Sprintf("Echo(data=%s)", r.Data)
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
	case CreatePromiseAndTask:
		return r.CreatePromiseAndTask.Status
	case CompletePromise:
		return r.CompletePromise.Status

	// CALLBACKS
	case CreateCallback:
		return r.CreateCallback.Status

	// NOTIFY
	case CreateNotify:
		return r.CreateNotify.Status

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
		return "Response"
	}
}
