package t_api

import (
	"fmt"

	"github.com/resonatehq/resonate/internal/util"
	"github.com/resonatehq/resonate/pkg/callback"
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

type PromiseGetResponse struct {
	Promise *promise.Promise `json:"promise,omitempty"`
}

func (r *PromiseGetResponse) String() string {
	return fmt.Sprintf("PromiseGet(promise=%v)", r.Promise)
}

func (r *PromiseGetResponse) Kind() Kind { return PromiseGet }

type PromiseSearchResponse struct {
	Promises []*promise.Promise            `json:"promises,omitempty"`
	Cursor   *Cursor[PromiseSearchRequest] `json:"cursor,omitempty"`
}

func (r *PromiseSearchResponse) String() string {
	return fmt.Sprintf("PromiseSearch(promises=%v, cursor=%v)", r.Promises, r.Cursor)
}

func (r *PromiseSearchResponse) Kind() Kind { return PromiseSearch }

type PromiseCreateResponse struct {
	Promise *promise.Promise `json:"promise,omitempty"`
}

func (r *PromiseCreateResponse) String() string {
	return fmt.Sprintf("PromiseCreate(promise=%v)", r.Promise)
}

func (r *PromiseCreateResponse) Kind() Kind { return PromiseCreate }

type TaskCreateResponse struct {
	Promise *promise.Promise `json:"promise,omitempty"`
	Task    *task.Task       `json:"task,omitempty"`
}

func (r *TaskCreateResponse) String() string {
	return fmt.Sprintf("TaskCreate(promise=%v, task=%v)", r.Promise, r.Task)
}

func (r *TaskCreateResponse) Kind() Kind { return TaskCreate }

type PromiseCompleteResponse struct {
	Promise *promise.Promise `json:"promise,omitempty"`
}

func (r *PromiseCompleteResponse) String() string {
	return fmt.Sprintf("PromiseComplete(promise=%v)", r.Promise)
}

func (r *PromiseCompleteResponse) Kind() Kind { return PromiseComplete }

// Callbacks

type PromiseRegisterResponse struct {
	Promise  *promise.Promise   `json:"promise,omitempty"`
	Callback *callback.Callback `json:"callback,omitempty"`
}

func (r *PromiseRegisterResponse) String() string {
	return fmt.Sprintf("PromiseRegister(promise=%v, callback=%v)", r.Promise, r.Callback)
}

func (r *PromiseRegisterResponse) Kind() Kind { return PromiseRegister }

// Schedules

type ScheduleGetResponse struct {
	Schedule *schedule.Schedule `json:"schedule,omitempty"`
}

func (r *ScheduleGetResponse) String() string {
	return fmt.Sprintf("ScheduleGet(schedule=%v)", r.Schedule)
}

func (r *ScheduleGetResponse) Kind() Kind { return ScheduleRead }

type ScheduleSearchResponse struct {
	Schedules []*schedule.Schedule             `json:"schedules,omitempty"`
	Cursor    *Cursor[ScheduleSearchRequest] `json:"cursor,omitempty"`
}

func (r *ScheduleSearchResponse) String() string {
	return fmt.Sprintf("ScheduleSearch(schedules=%v, cursor=%v)", r.Schedules, r.Cursor)
}

func (r *ScheduleSearchResponse) Kind() Kind { return ScheduleSearch }

type ScheduleCreateResponse struct {
	Schedule *schedule.Schedule `json:"schedule,omitempty"`
}

func (r *ScheduleCreateResponse) String() string {
	return fmt.Sprintf("ScheduleCreate(schedule=%v)", r.Schedule)
}

func (r *ScheduleCreateResponse) Kind() Kind { return ScheduleCreate }

type ScheduleDeleteResponse struct{}

func (r *ScheduleDeleteResponse) String() string {
	return "ScheduleDelete()"
}

func (r *ScheduleDeleteResponse) Kind() Kind { return ScheduleDelete }

// Tasks

type TaskAcquireResponse struct {
	Task            *task.Task       `json:"task,omitempty"`
	RootPromise     *promise.Promise `json:"rootPromise,omitempty"`
	LeafPromise     *promise.Promise `json:"leafPromise,omitempty"`
	RootPromiseHref string           `json:"rootPromiseHref,omitempty"`
	LeafPromiseHref string           `json:"leafPromiseHref,omitempty"`
}

func (r *TaskAcquireResponse) String() string {
	return fmt.Sprintf("TaskAcquire(task=%v)", r.Task)
}

func (r *TaskAcquireResponse) Kind() Kind { return TaskAcquire }

type TaskCompleteResponse struct {
	Task *task.Task `json:"task,omitempty"`
}

func (r *TaskCompleteResponse) String() string {
	return fmt.Sprintf("TaskComplete(task=%v)", r.Task)
}

func (r *TaskCompleteResponse) Kind() Kind { return TaskComplete }

type TaskReleaseResponse struct {
	Task *task.Task `json:"task,omitempty"`
}

func (r *TaskReleaseResponse) String() string {
	return fmt.Sprintf("TaskRelease(task=%v)", r.Task)
}

func (r *TaskReleaseResponse) Kind() Kind { return TaskRelease }

type TaskHeartbeatResponse struct {
	TasksAffected int64 `json:"tasksAffected"`
}

func (r *TaskHeartbeatResponse) String() string {
	return fmt.Sprintf("TaskHeartbeat(tasksAffected=%d)", r.TasksAffected)
}

func (r *TaskHeartbeatResponse) Kind() Kind { return TaskHeartbeat }

// Echo

type EchoResponse struct {
	Data string `json:"data"`
}

func (r *EchoResponse) String() string {
	return fmt.Sprintf("Echo(data=%s)", r.Data)
}

func (r *EchoResponse) Kind() Kind { return Echo }

// Noop

type NoopResponse struct{}

func (r *NoopResponse) String() string {
	return "Noop()"
}

func (r *NoopResponse) Kind() Kind { return Noop }

// Marker methods that make each of the request types be a
// ResponsePayload type.
func (r *PromiseGetResponse) isResponsePayload()      {}
func (r *PromiseSearchResponse) isResponsePayload()   {}
func (r *PromiseCreateResponse) isResponsePayload()   {}
func (r *TaskCreateResponse) isResponsePayload()      {}
func (r *PromiseCompleteResponse) isResponsePayload() {}
func (r *PromiseRegisterResponse) isResponsePayload() {}
func (r *ScheduleGetResponse) isResponsePayload()     {}
func (r *ScheduleSearchResponse) isResponsePayload()  {}
func (r *ScheduleCreateResponse) isResponsePayload()  {}
func (r *ScheduleDeleteResponse) isResponsePayload()  {}
func (r *TaskAcquireResponse) isResponsePayload()     {}
func (r *TaskCompleteResponse) isResponsePayload()    {}
func (r *TaskReleaseResponse) isResponsePayload()     {}
func (r *TaskHeartbeatResponse) isResponsePayload()   {}
func (r *EchoResponse) isResponsePayload()            {}
func (r *NoopResponse) isResponsePayload()            {}

func (r *Response) String() string {
	util.Assert(r.Payload != nil, "Payload must not be nil")
	return fmt.Sprintf("Response(status=%d, %s)", r.Status, r.Payload.String())
}

func (r *Response) Kind() Kind {
	util.Assert(r.Payload != nil, "Payload must not be nil")
	return r.Payload.Kind()
}

// Methods to cast Response.Payload to specific response payload types (direct assertion)
func (r *Response) AsPromiseGetResponse() *PromiseGetResponse {
	return r.Payload.(*PromiseGetResponse)
}

func (r *Response) AsPromiseSearchResponse() *PromiseSearchResponse {
	return r.Payload.(*PromiseSearchResponse)
}

func (r *Response) AsPromiseCreateResponse() *PromiseCreateResponse {
	return r.Payload.(*PromiseCreateResponse)
}

func (r *Response) AsTaskCreateResponse() *TaskCreateResponse {
	return r.Payload.(*TaskCreateResponse)
}

func (r *Response) AsPromiseCompleteResponse() *PromiseCompleteResponse {
	return r.Payload.(*PromiseCompleteResponse)
}

func (r *Response) AsPromiseRegisterResponse() *PromiseRegisterResponse {
	return r.Payload.(*PromiseRegisterResponse)
}

func (r *Response) AsScheduleGetResponse() *ScheduleGetResponse {
	return r.Payload.(*ScheduleGetResponse)
}

func (r *Response) AsScheduleSearchResponse() *ScheduleSearchResponse {
	return r.Payload.(*ScheduleSearchResponse)
}

func (r *Response) AsScheduleCreateResponse() *ScheduleCreateResponse {
	return r.Payload.(*ScheduleCreateResponse)
}

func (r *Response) AsScheduleDeleteResponse() *ScheduleDeleteResponse {
	return r.Payload.(*ScheduleDeleteResponse)
}

func (r *Response) AsTaskAcquireResponse() *TaskAcquireResponse {
	return r.Payload.(*TaskAcquireResponse)
}

func (r *Response) AsTaskCompleteResponse() *TaskCompleteResponse {
	return r.Payload.(*TaskCompleteResponse)
}

func (r *Response) AsTaskReleaseResponse() *TaskReleaseResponse {
	return r.Payload.(*TaskReleaseResponse)
}

func (r *Response) AsTaskHeartbeatResponse() *TaskHeartbeatResponse {
	return r.Payload.(*TaskHeartbeatResponse)
}

func (r *Response) AsEchoResponse() *EchoResponse {
	return r.Payload.(*EchoResponse)
}
