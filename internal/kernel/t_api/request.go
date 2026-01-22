package t_api

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/resonatehq/resonate/internal/util"
	"github.com/resonatehq/resonate/pkg/message"
	"github.com/resonatehq/resonate/pkg/promise"
)

type Request struct {
	Head map[string]string
	Data RequestPayload
}

type RequestPayload interface {
	Kind() Kind
	String() string
	Validate() error
	isRequestPayload()
}

// Promises

type PromiseGetRequest struct {
	Id string `json:"id"`
}

func (r *PromiseGetRequest) String() string {
	return fmt.Sprintf("PromiseGet(id=%s)", r.Id)
}

func (r *PromiseGetRequest) Validate() error {
	return nil
}

func (r *PromiseGetRequest) Kind() Kind {
	return PromiseGet
}

type PromiseSearchRequest struct {
	Id     string            `json:"id"`
	States []promise.State   `json:"states"`
	Tags   map[string]string `json:"tags"`
	Limit  int               `json:"limit"`
	SortId *int64            `json:"sortId"`
}

func (r *PromiseSearchRequest) String() string {
	return fmt.Sprintf("PromiseSearch(id=%s, states=%v, tags=%v, limit=%d, sortId=%d)", r.Id, r.States, r.Tags, r.Limit, util.SafeDeref(r.SortId))
}

func (r *PromiseSearchRequest) Validate() error {
	return nil
}

func (r *PromiseSearchRequest) Kind() Kind {
	return PromiseSearch
}

type PromiseCreateRequest struct {
	Id      string            `json:"id"`
	Param   promise.Value     `json:"param,omitempty"`
	Timeout int64             `json:"timeout"`
	Tags    map[string]string `json:"tags,omitempty"`
}

func (r *PromiseCreateRequest) String() string {
	return fmt.Sprintf("PromiseCreate(id=%s, param=%v, timeout=%d, tags=%v)", r.Id, r.Param, r.Timeout, r.Tags)
}

func (r *PromiseCreateRequest) Validate() error {
	return nil
}

func (r *PromiseCreateRequest) Kind() Kind {
	return PromiseCreate
}

type TaskCreateRequest struct {
	Promise *PromiseCreateRequest
	Task    *CreateTaskRequest
}

func (r *TaskCreateRequest) String() string {
	return fmt.Sprintf("TaskCreate(promise=%v, task=%v)", r.Promise, r.Task)
}

func (r *TaskCreateRequest) Validate() error {
	return nil
}

func (r *TaskCreateRequest) Kind() Kind {
	return TaskCreate
}

type PromiseCompleteRequest struct {
	Id    string        `json:"id"`
	State promise.State `json:"state"`
	Value promise.Value `json:"value,omitempty"`
}

func (r *PromiseCompleteRequest) String() string {
	return fmt.Sprintf("PromiseComplete(id=%s, state=%v, value=%v)", r.Id, r.State, r.Value)
}

func (r *PromiseCompleteRequest) Validate() error {
	return nil
}

func (r *PromiseCompleteRequest) Kind() Kind {
	return PromiseComplete
}

type PromiseRegisterRequest struct {
	Awaiter string `json:"awaiter"`
	Awaited string `json:"awaited"`
}

func (r *PromiseRegisterRequest) String() string {
	return fmt.Sprintf("PromiseRegister(awaiter=%s, awaited=%s)", r.Awaiter, r.Awaited)
}

func (r *PromiseRegisterRequest) Validate() error {
	if r.Awaiter == r.Awaited {
		return errors.New("awaiter and awaited promises must be different")
	}
	return nil
}

func (r *PromiseRegisterRequest) Kind() Kind {
	return PromiseRegister
}

type PromiseSubscribeRequest struct {
	Awaited string `json:"id"`
	Address string `json:"address"`
}

func (r *PromiseSubscribeRequest) String() string {
	return fmt.Sprintf("PromiseSubscribe(awaited=%s, address=%s)", r.Awaited, r.Address)
}

func (r *PromiseSubscribeRequest) Validate() error {
	return nil
}

func (r *PromiseSubscribeRequest) Kind() Kind {
	return PromiseSubscribe
}

// TODO(avillega): Currently used on the old api.
// Once the old api is fully deleted and the task framework separates the
// concept of Message and Task we can refactor this request to be closer
// to the protocol and api.
type CallbackCreateRequest struct {
	Id        string          `json:"id"`
	PromiseId string          `json:"promiseId"`
	Recv      json.RawMessage `json:"recv"`
	Mesg      *message.Mesg   `json:"mesg"`
	Timeout   int64           `json:"timeout"`
}

func (r *CallbackCreateRequest) String() string {
	return fmt.Sprintf("CallbackCreate(id=%s, promiseId=%s, recv=%s, mesg=%s, timeout=%d)", r.Id, r.PromiseId, r.Recv, r.Mesg, r.Timeout)
}

func (r *CallbackCreateRequest) Validate() error {
	if r.Mesg.Type == "resume" && r.PromiseId == r.Mesg.Root {
		return errors.New("promise and root promise must be different")
	}
	return nil
}

func (r *CallbackCreateRequest) Kind() Kind {
	return CallbackCreate
}

// Schedules

type ScheduleGetRequest struct {
	Id string `json:"id"`
}

func (r *ScheduleGetRequest) String() string {
	return fmt.Sprintf("ScheduleGet(id=%s)", r.Id)
}

func (r *ScheduleGetRequest) Validate() error {
	return nil
}

func (r *ScheduleGetRequest) Kind() Kind {
	return ScheduleRead
}

type ScheduleSearchRequest struct {
	Id     string            `json:"id"`
	Tags   map[string]string `json:"tags"`
	Limit  int               `json:"limit"`
	SortId *int64            `json:"sortId"`
}

func (r *ScheduleSearchRequest) String() string {
	return fmt.Sprintf("ScheduleSearch(id=%s, tags=%v, limit=%d, sortId=%d)", r.Id, r.Tags, r.Limit, util.SafeDeref(r.SortId))
}

func (r *ScheduleSearchRequest) Validate() error {
	return nil
}

func (r *ScheduleSearchRequest) Kind() Kind {
	return ScheduleSearch
}

type ScheduleCreateRequest struct {
	Id             string            `json:"id"`
	Description    string            `json:"desc,omitempty"`
	Cron           string            `json:"cron"`
	Tags           map[string]string `json:"tags,omitempty"`
	PromiseId      string            `json:"promiseId"`
	PromiseTimeout int64             `json:"promiseTimeout"`
	PromiseParam   promise.Value     `json:"promiseParam,omitempty"`
	PromiseTags    map[string]string `json:"promiseTags,omitempty"`
}

func (r *ScheduleCreateRequest) String() string {
	return fmt.Sprintf(
		"ScheduleCreate(id=%s, desc=%s, cron=%s, tags=%v, promiseId=%s, promiseTimeout=%d, promiseParam=%v, promiseTags=%v)",
		r.Id,
		r.Description,
		r.Cron,
		r.Tags,
		r.PromiseId,
		r.PromiseTimeout,
		r.PromiseParam,
		r.PromiseTags,
	)
}

func (r *ScheduleCreateRequest) Validate() error {
	return nil
}

func (r *ScheduleCreateRequest) Kind() Kind {
	return ScheduleCreate
}

type ScheduleDeleteRequest struct {
	Id string `json:"id"`
}

func (r *ScheduleDeleteRequest) String() string {
	return fmt.Sprintf("ScheduleDelete(id=%s)", r.Id)
}

func (r *ScheduleDeleteRequest) Validate() error {
	return nil
}

func (r *ScheduleDeleteRequest) Kind() Kind {
	return ScheduleDelete
}

// Tasks

// CreateTaskRequest is not a Request on its own it needs to be part of a CreatePromiseAndTask Request
// that is why it does not implement the request interface
type CreateTaskRequest struct {
	PromiseId string `json:"promiseId"`
	ProcessId string `json:"processId"`
	Ttl       int64  `json:"ttl" binding:"min=0"`
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

type TaskAcquireRequest struct {
	Id        string `json:"id"`
	Counter   int    `json:"counter"`
	ProcessId string `json:"processId"`
	Ttl       int64  `json:"ttl" binding:"min=0"`
}

func (r *TaskAcquireRequest) String() string {
	return fmt.Sprintf("TaskAcquire(id=%s, counter=%d, processId=%s, ttl=%d)", r.Id, r.Counter, r.ProcessId, r.Ttl)
}

func (r *TaskAcquireRequest) Validate() error {
	return nil
}

func (r *TaskAcquireRequest) Kind() Kind {
	return TaskAcquire
}

type TaskCompleteRequest struct {
	Id      string `json:"id"`
	Counter int    `json:"counter"`
}

func (r *TaskCompleteRequest) String() string {
	return fmt.Sprintf("TaskComplete(id=%s, counter=%d)", r.Id, r.Counter)
}

func (r *TaskCompleteRequest) Validate() error {
	return nil
}

func (r *TaskCompleteRequest) Kind() Kind {
	return TaskComplete
}

type TaskReleaseRequest struct {
	Id      string `json:"id"`
	Counter int    `json:"counter"`
}

func (r *TaskReleaseRequest) String() string {
	return fmt.Sprintf("TaskRelease(id=%s, counter=%d)", r.Id, r.Counter)
}

func (r *TaskReleaseRequest) Validate() error {
	return nil
}

func (r *TaskReleaseRequest) Kind() Kind {
	return TaskRelease
}

type TaskHeartbeatRequest struct {
	ProcessId string `json:"processId"`
}

func (r *TaskHeartbeatRequest) String() string {
	return fmt.Sprintf("TaskHeartbeat(processId=%s)", r.ProcessId)
}

func (r *TaskHeartbeatRequest) Validate() error {
	return nil
}

func (r *TaskHeartbeatRequest) Kind() Kind {
	return TaskHeartbeat
}

// Echo

type EchoRequest struct {
	Data string `json:"data"`
}

func (r *EchoRequest) String() string {
	return fmt.Sprintf("Echo(data=%s)", r.Data)
}

func (r *EchoRequest) Validate() error {
	return nil
}

func (r *EchoRequest) Kind() Kind {
	return Echo
}

// Noop

type NoopRequest struct{}

func (r *NoopRequest) String() string {
	return "Noop()"
}

func (r *NoopRequest) Validate() error {
	return nil
}

func (r *NoopRequest) Kind() Kind {
	return Noop
}

// Marker methods that make each of the request types be a
// RequestPayload type.
func (r *PromiseGetRequest) isRequestPayload()       {}
func (r *PromiseSearchRequest) isRequestPayload()    {}
func (r *PromiseCreateRequest) isRequestPayload()    {}
func (r *TaskCreateRequest) isRequestPayload()       {}
func (r *PromiseCompleteRequest) isRequestPayload()  {}
func (r *PromiseRegisterRequest) isRequestPayload()  {}
func (r *PromiseSubscribeRequest) isRequestPayload() {}
func (r *CallbackCreateRequest) isRequestPayload()   {}
func (r *ScheduleGetRequest) isRequestPayload()      {}
func (r *ScheduleSearchRequest) isRequestPayload()   {}
func (r *ScheduleCreateRequest) isRequestPayload()   {}
func (r *ScheduleDeleteRequest) isRequestPayload()   {}
func (r *TaskAcquireRequest) isRequestPayload()      {}
func (r *TaskCompleteRequest) isRequestPayload()     {}
func (r *TaskReleaseRequest) isRequestPayload()      {}
func (r *TaskHeartbeatRequest) isRequestPayload()    {}
func (r *EchoRequest) isRequestPayload()             {}
func (r *NoopRequest) isRequestPayload()             {}

// Request Methods

func (r *Request) String() string {
	util.Assert(r.Data != nil, "Payload cannot be nil")
	return r.Data.String()
}

func (r *Request) Kind() Kind {
	util.Assert(r.Data != nil, "Payload cannot be nil")
	return r.Data.Kind()
}

func (r *Request) Validate() error {
	util.Assert(r.Data != nil, "Payload cannot be nil")
	return r.Data.Validate()
}
