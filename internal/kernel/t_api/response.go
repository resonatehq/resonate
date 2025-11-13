package t_api

import (
	"fmt"

	"github.com/resonatehq/resonate/internal/util"
	"github.com/resonatehq/resonate/pkg/callback"
	"github.com/resonatehq/resonate/pkg/lock"
	"github.com/resonatehq/resonate/pkg/promise"
	"github.com/resonatehq/resonate/pkg/schedule"
	"github.com/resonatehq/resonate/pkg/task"
)

type Response struct {
	Status   StatusCode
	Metadata map[string]string
	Payload  ResponsePayload
}

type ResponsePayload interface {
	Kind() Kind
	String() string
	isResponsePayload()
}

// Promises

type ReadPromiseResponse struct {
	Promise *promise.Promise `json:"promise,omitempty"`
}

func (r *ReadPromiseResponse) String() string {
	return fmt.Sprintf("ReadPromise(promise=%v)", r.Promise)
}

func (r *ReadPromiseResponse) Kind() Kind { return ReadPromise }

type SearchPromisesResponse struct {
	Promises []*promise.Promise             `json:"promises,omitempty"`
	Cursor   *Cursor[SearchPromisesRequest] `json:"cursor,omitempty"`
}

func (r *SearchPromisesResponse) String() string {
	return fmt.Sprintf("SearchPromises(promises=%v, cursor=%v)", r.Promises, r.Cursor)
}

func (r *SearchPromisesResponse) Kind() Kind { return SearchPromises }

type CreatePromiseResponse struct {
	Promise *promise.Promise `json:"promise,omitempty"`
}

func (r *CreatePromiseResponse) String() string {
	return fmt.Sprintf("CreatePromise(promise=%v)", r.Promise)
}

func (r *CreatePromiseResponse) Kind() Kind { return CreatePromise }

type CreatePromiseAndTaskResponse struct {
	Promise *promise.Promise `json:"promise,omitempty"`
	Task    *task.Task       `json:"task,omitempty"`
}

func (r *CreatePromiseAndTaskResponse) String() string {
	return fmt.Sprintf("CreatePromiseAndTask(promise=%v, task=%v)", r.Promise, r.Task)
}

func (r *CreatePromiseAndTaskResponse) Kind() Kind { return CreatePromiseAndTask }

type CompletePromiseResponse struct {
	Promise *promise.Promise `json:"promise,omitempty"`
}

func (r *CompletePromiseResponse) String() string {
	return fmt.Sprintf("CompletePromise(promise=%v)", r.Promise)
}

func (r *CompletePromiseResponse) Kind() Kind { return CompletePromise }

// Callbacks

type CreateCallbackResponse struct {
	Promise  *promise.Promise   `json:"promise,omitempty"`
	Callback *callback.Callback `json:"callback,omitempty"`
}

func (r *CreateCallbackResponse) String() string {
	return fmt.Sprintf("CreateCallback(promise=%v, callback=%v)", r.Promise, r.Callback)
}

func (r *CreateCallbackResponse) Kind() Kind { return CreateCallback }

// Schedules

type ReadScheduleResponse struct {
	Schedule *schedule.Schedule `json:"schedule,omitempty"`
}

func (r *ReadScheduleResponse) String() string {
	return fmt.Sprintf("ReadSchedule(schedule=%v)", r.Schedule)
}

func (r *ReadScheduleResponse) Kind() Kind { return ReadSchedule }

type SearchSchedulesResponse struct {
	Schedules []*schedule.Schedule            `json:"schedules,omitempty"`
	Cursor    *Cursor[SearchSchedulesRequest] `json:"cursor,omitempty"`
}

func (r *SearchSchedulesResponse) String() string {
	return fmt.Sprintf("SearchSchedules(schedules=%v, cursor=%v)", r.Schedules, r.Cursor)
}

func (r *SearchSchedulesResponse) Kind() Kind { return SearchSchedules }

type CreateScheduleResponse struct {
	Schedule *schedule.Schedule `json:"schedule,omitempty"`
}

func (r *CreateScheduleResponse) String() string {
	return fmt.Sprintf("CreateSchedule(schedule=%v)", r.Schedule)
}

func (r *CreateScheduleResponse) Kind() Kind { return CreateSchedule }

type DeleteScheduleResponse struct{}

func (r *DeleteScheduleResponse) String() string {
	return "DeleteSchedule()"
}

func (r *DeleteScheduleResponse) Kind() Kind { return DeleteSchedule }

// Locks

type AcquireLockResponse struct {
	Lock *lock.Lock `json:"lock,omitempty"`
}

func (r *AcquireLockResponse) String() string {
	return fmt.Sprintf("AcquireLock(lock=%v)", r.Lock)
}

func (r *AcquireLockResponse) Kind() Kind { return AcquireLock }

type ReleaseLockResponse struct {
}

func (r *ReleaseLockResponse) String() string {
	return "ReleaseLock()"
}

func (r *ReleaseLockResponse) Kind() Kind { return ReleaseLock }

type HeartbeatLocksResponse struct {
	LocksAffected int64 `json:"locksAffected"`
}

func (r *HeartbeatLocksResponse) String() string {
	return fmt.Sprintf("HeartbeatLocks(locksAffected=%d)", r.LocksAffected)
}

func (r *HeartbeatLocksResponse) Kind() Kind { return HeartbeatLocks }

// Tasks

type ClaimTaskResponse struct {
	Task            *task.Task       `json:"task,omitempty"`
	RootPromise     *promise.Promise `json:"rootPromise,omitempty"`
	LeafPromise     *promise.Promise `json:"leafPromise,omitempty"`
	RootPromiseHref string           `json:"rootPromiseHref,omitempty"`
	LeafPromiseHref string           `json:"leafPromiseHref,omitempty"`
}

func (r *ClaimTaskResponse) String() string {
	return fmt.Sprintf("ClaimTask(task=%v)", r.Task)
}

func (r *ClaimTaskResponse) Kind() Kind { return ClaimTask }

type CompleteTaskResponse struct {
	Task *task.Task `json:"task,omitempty"`
}

func (r *CompleteTaskResponse) String() string {
	return fmt.Sprintf("CompleteTask(task=%v)", r.Task)
}

func (r *CompleteTaskResponse) Kind() Kind { return CompleteTask }

type DropTaskResponse struct {
	Task *task.Task `json:"task,omitempty"`
}

func (r *DropTaskResponse) String() string {
	return fmt.Sprintf("DropTask(task=%v)", r.Task)
}

func (r *DropTaskResponse) Kind() Kind { return DropTask }

type HeartbeatTasksResponse struct {
	TasksAffected int64 `json:"tasksAffected"`
}

func (r *HeartbeatTasksResponse) String() string {
	return fmt.Sprintf("HeartbeatTasks(tasksAffected=%d)", r.TasksAffected)
}

func (r *HeartbeatTasksResponse) Kind() Kind { return HeartbeatTasks }

// Echo

type EchoResponse struct {
	Data string `json:"data"`
}

func (r *EchoResponse) String() string {
	return fmt.Sprintf("Echo(data=%s)", r.Data)
}

func (r *EchoResponse) Kind() Kind { return Echo }

// Marker methods that make each of the request types be a
// ResponsePayload type.
func (r *ReadPromiseResponse) isResponsePayload()          {}
func (r *SearchPromisesResponse) isResponsePayload()       {}
func (r *CreatePromiseResponse) isResponsePayload()        {}
func (r *CreatePromiseAndTaskResponse) isResponsePayload() {}
func (r *CompletePromiseResponse) isResponsePayload()      {}
func (r *CreateCallbackResponse) isResponsePayload()       {}
func (r *ReadScheduleResponse) isResponsePayload()         {}
func (r *SearchSchedulesResponse) isResponsePayload()      {}
func (r *CreateScheduleResponse) isResponsePayload()       {}
func (r *DeleteScheduleResponse) isResponsePayload()       {}
func (r *AcquireLockResponse) isResponsePayload()          {}
func (r *ReleaseLockResponse) isResponsePayload()          {}
func (r *HeartbeatLocksResponse) isResponsePayload()       {}
func (r *ClaimTaskResponse) isResponsePayload()            {}
func (r *CompleteTaskResponse) isResponsePayload()         {}
func (r *DropTaskResponse) isResponsePayload()             {}
func (r *HeartbeatTasksResponse) isResponsePayload()       {}
func (r *EchoResponse) isResponsePayload()                 {}

func (r *Response) String() string {
	util.Assert(r.Payload != nil, "Payload must not be nil")
	return fmt.Sprintf("Response(status=%d, %s)", r.Status, r.Payload.String())
}

func (r *Response) Kind() Kind {
	util.Assert(r.Payload != nil, "Payload must not be nil")
	return r.Payload.Kind()
}

// Methods to cast Response.Payload to specific response payload types (direct assertion)
func (r *Response) AsReadPromiseResponse() *ReadPromiseResponse {
	return r.Payload.(*ReadPromiseResponse)
}

func (r *Response) AsSearchPromisesResponse() *SearchPromisesResponse {
	return r.Payload.(*SearchPromisesResponse)
}

func (r *Response) AsCreatePromiseResponse() *CreatePromiseResponse {
	return r.Payload.(*CreatePromiseResponse)
}

func (r *Response) AsCreatePromiseAndTaskResponse() *CreatePromiseAndTaskResponse {
	return r.Payload.(*CreatePromiseAndTaskResponse)
}

func (r *Response) AsCompletePromiseResponse() *CompletePromiseResponse {
	return r.Payload.(*CompletePromiseResponse)
}

func (r *Response) AsCreateCallbackResponse() *CreateCallbackResponse {
	return r.Payload.(*CreateCallbackResponse)
}

func (r *Response) AsReadScheduleResponse() *ReadScheduleResponse {
	return r.Payload.(*ReadScheduleResponse)
}

func (r *Response) AsSearchSchedulesResponse() *SearchSchedulesResponse {
	return r.Payload.(*SearchSchedulesResponse)
}

func (r *Response) AsCreateScheduleResponse() *CreateScheduleResponse {
	return r.Payload.(*CreateScheduleResponse)
}

func (r *Response) AsDeleteScheduleResponse() *DeleteScheduleResponse {
	return r.Payload.(*DeleteScheduleResponse)
}

func (r *Response) AsAcquireLockResponse() *AcquireLockResponse {
	return r.Payload.(*AcquireLockResponse)
}

func (r *Response) AsReleaseLockResponse() *ReleaseLockResponse {
	return r.Payload.(*ReleaseLockResponse)
}

func (r *Response) AsHeartbeatLocksResponse() *HeartbeatLocksResponse {
	return r.Payload.(*HeartbeatLocksResponse)
}

func (r *Response) AsClaimTaskResponse() *ClaimTaskResponse {
	return r.Payload.(*ClaimTaskResponse)
}

func (r *Response) AsCompleteTaskResponse() *CompleteTaskResponse {
	return r.Payload.(*CompleteTaskResponse)
}

func (r *Response) AsDropTaskResponse() *DropTaskResponse {
	return r.Payload.(*DropTaskResponse)
}

func (r *Response) AsHeartbeatTasksResponse() *HeartbeatTasksResponse {
	return r.Payload.(*HeartbeatTasksResponse)
}

func (r *Response) AsEchoResponse() *EchoResponse {
	return r.Payload.(*EchoResponse)
}
