package t_api

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/resonatehq/resonate/internal/util"
	"github.com/resonatehq/resonate/pkg/idempotency"
	"github.com/resonatehq/resonate/pkg/message"
	"github.com/resonatehq/resonate/pkg/promise"
)

type RequestPayload interface {
	Kind() Kind
	String() string
	Validate() error
	isRequestPayload()
}

type Request struct {
	Metadata map[string]string
	Payload  RequestPayload
}

// Promises

type ReadPromiseRequest struct {
	Id string `json:"id"`
}

func (r *ReadPromiseRequest) String() string {
	return fmt.Sprintf("ReadPromise(id=%s)", r.Id)
}

func (r *ReadPromiseRequest) Validate() error {
	return nil
}

func (r *ReadPromiseRequest) Kind() Kind {
	return ReadPromise
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

func (r *SearchPromisesRequest) Validate() error {
	return nil
}

func (r *SearchPromisesRequest) Kind() Kind {
	return SearchPromises
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

func (r *CreatePromiseRequest) Validate() error {
	return nil
}

func (r *CreatePromiseRequest) Kind() Kind {
	return CreatePromise
}

type CreatePromiseAndTaskRequest struct {
	Promise *CreatePromiseRequest
	Task    *CreateTaskRequest
}

func (r *CreatePromiseAndTaskRequest) String() string {
	return fmt.Sprintf("CreatePromiseAndTask(promise=%v, task=%v)", r.Promise, r.Task)
}

func (r *CreatePromiseAndTaskRequest) Validate() error {
	return nil
}

func (r *CreatePromiseAndTaskRequest) Kind() Kind {
	return CreatePromiseAndTask
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

func (r *CompletePromiseRequest) Validate() error {
	return nil
}

func (r *CompletePromiseRequest) Kind() Kind {
	return CompletePromise
}

// Callbacks

type CreateCallbackRequest struct {
	Id        string          `json:"id"`
	PromiseId string          `json:"promiseId"`
	Recv      json.RawMessage `json:"recv"`
	Mesg      *message.Mesg   `json:"mesg"`
	Timeout   int64           `json:"timeout"`
}

func (r *CreateCallbackRequest) String() string {
	return fmt.Sprintf("CreateCallback(id=%s, promiseId=%s, recv=%s, mesg=%s, timeout=%d)", r.Id, r.PromiseId, r.Recv, r.Mesg, r.Timeout)
}

func (r *CreateCallbackRequest) Validate() error {
	if r.Mesg.Type == "resume" && r.PromiseId == r.Mesg.Root {
		return errors.New("promise and root promise must be different")
	}
	return nil
}

func (r *CreateCallbackRequest) Kind() Kind {
	return CreateCallback
}

// Schedules

type ReadScheduleRequest struct {
	Id string `json:"id"`
}

func (r *ReadScheduleRequest) String() string {
	return fmt.Sprintf("ReadSchedule(id=%s)", r.Id)
}

func (r *ReadScheduleRequest) Validate() error {
	return nil
}

func (r *ReadScheduleRequest) Kind() Kind {
	return ReadSchedule
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

func (r *SearchSchedulesRequest) Validate() error {
	return nil
}

func (r *SearchSchedulesRequest) Kind() Kind {
	return SearchSchedules
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

func (r *CreateScheduleRequest) Validate() error {
	return nil
}

func (r *CreateScheduleRequest) Kind() Kind {
	return CreateSchedule
}

type DeleteScheduleRequest struct {
	Id string `json:"id"`
}

func (r *DeleteScheduleRequest) String() string {
	return fmt.Sprintf("DeleteSchedule(id=%s)", r.Id)
}

func (r *DeleteScheduleRequest) Validate() error {
	return nil
}

func (r *DeleteScheduleRequest) Kind() Kind {
	return DeleteSchedule
}

// Locks

type AcquireLockRequest struct {
	ResourceId  string `json:"resourceId"`
	ExecutionId string `json:"executionId"`
	ProcessId   string `json:"processId"`
	Ttl         int64  `json:"ttl" binding:"min=0"`
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

func (r *AcquireLockRequest) Validate() error {
	return nil
}

func (r *AcquireLockRequest) Kind() Kind {
	return AcquireLock
}

type ReleaseLockRequest struct {
	ResourceId  string `json:"resourceId"`
	ExecutionId string `json:"executionId"`
}

func (r *ReleaseLockRequest) String() string {
	return fmt.Sprintf("ReleaseLock(resourceId=%s, executionId=%s)", r.ResourceId, r.ExecutionId)
}

func (r *ReleaseLockRequest) Validate() error {
	return nil
}

func (r *ReleaseLockRequest) Kind() Kind {
	return ReleaseLock
}

type HeartbeatLocksRequest struct {
	ProcessId string `json:"processId"`
}

func (r *HeartbeatLocksRequest) String() string {
	return fmt.Sprintf("HeartbeatLocks(processId=%s)", r.ProcessId)
}

func (r *HeartbeatLocksRequest) Validate() error {
	return nil
}

func (r *HeartbeatLocksRequest) Kind() Kind {
	return HeartbeatLocks
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

type ClaimTaskRequest struct {
	Id        string `json:"id"`
	Counter   int    `json:"counter"`
	ProcessId string `json:"processId"`
	Ttl       int64  `json:"ttl" binding:"min=0"`
}

func (r *ClaimTaskRequest) String() string {
	return fmt.Sprintf("ClaimTask(id=%s, counter=%d, processId=%s, ttl=%d)", r.Id, r.Counter, r.ProcessId, r.Ttl)
}

func (r *ClaimTaskRequest) Validate() error {
	return nil
}

func (r *ClaimTaskRequest) Kind() Kind {
	return ClaimTask
}

type CompleteTaskRequest struct {
	Id      string `json:"id"`
	Counter int    `json:"counter"`
}

func (r *CompleteTaskRequest) String() string {
	return fmt.Sprintf("CompleteTask(id=%s, counter=%d)", r.Id, r.Counter)
}

func (r *CompleteTaskRequest) Validate() error {
	return nil
}

func (r *CompleteTaskRequest) Kind() Kind {
	return CompleteTask
}

type DropTaskRequest struct {
	Id      string `json:"id"`
	Counter int    `json:"counter"`
}

func (r *DropTaskRequest) String() string {
	return fmt.Sprintf("DropTask(id=%s, counter=%d)", r.Id, r.Counter)
}

func (r *DropTaskRequest) Validate() error {
	return nil
}

func (r *DropTaskRequest) Kind() Kind {
	return DropTask
}

type HeartbeatTasksRequest struct {
	ProcessId string `json:"processId"`
}

func (r *HeartbeatTasksRequest) String() string {
	return fmt.Sprintf("HeartbeatTasks(processId=%s)", r.ProcessId)
}

func (r *HeartbeatTasksRequest) Validate() error {
	return nil
}

func (r *HeartbeatTasksRequest) Kind() Kind {
	return HeartbeatTasks
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

// Marker methods that make each of the request types be a
// RequestPayload type.
func (r *ReadPromiseRequest) isRequestPayload()          {}
func (r *SearchPromisesRequest) isRequestPayload()       {}
func (r *CreatePromiseRequest) isRequestPayload()        {}
func (r *CreatePromiseAndTaskRequest) isRequestPayload() {}
func (r *CompletePromiseRequest) isRequestPayload()      {}
func (r *CreateCallbackRequest) isRequestPayload()       {}
func (r *ReadScheduleRequest) isRequestPayload()         {}
func (r *SearchSchedulesRequest) isRequestPayload()      {}
func (r *CreateScheduleRequest) isRequestPayload()       {}
func (r *DeleteScheduleRequest) isRequestPayload()       {}
func (r *AcquireLockRequest) isRequestPayload()          {}
func (r *ReleaseLockRequest) isRequestPayload()          {}
func (r *HeartbeatLocksRequest) isRequestPayload()       {}
func (r *ClaimTaskRequest) isRequestPayload()            {}
func (r *CompleteTaskRequest) isRequestPayload()         {}
func (r *DropTaskRequest) isRequestPayload()             {}
func (r *HeartbeatTasksRequest) isRequestPayload()       {}
func (r *EchoRequest) isRequestPayload()                 {}

// Request Methods

func (r *Request) String() string {
	util.Assert(r.Payload != nil, "Payload cannot be nil")
	return r.Payload.String()
}

func (r *Request) Kind() Kind {
	util.Assert(r.Payload != nil, "Payload cannot be nil")
	return r.Payload.Kind()
}

func (r *Request) Validate() error {
	util.Assert(r.Payload != nil, "Payload cannot be nil")
	return r.Payload.Validate()
}
