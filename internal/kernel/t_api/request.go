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

// RequestPayload is an interface that all specific request types must implement.
// The `isRequestPayload()` method is a private "marker" method that ensures
// only types defined within this package can implement this interface.
type RequestPayload interface {
	Kind() Kind
	String() string  // Retain the Stringer interface for consistent string representation
	Validate() error // Method for validation
	isRequestPayload()
}

// Request is the top-level request type, holding the RequestPayload interface
// and a top-level Metadata field.
type Request struct {
	Metadata map[string]string `json:"metadata,omitempty"` // Top-level metadata
	Payload  RequestPayload    // The actual request payload
}

// Promises

type ReadPromiseRequest struct {
	Id   string            `json:"id"`
	Tags map[string]string `json:"tags,omitempty"`
}

func (r *ReadPromiseRequest) String() string {
	return fmt.Sprintf("ReadPromise(id=%s, tags=%v)", r.Id, r.Tags)
}

func (r *ReadPromiseRequest) Validate() error {
	return nil
}

func (r *ReadPromiseRequest) Kind() Kind {
	return ReadPromise
}

func (r *ReadPromiseRequest) isRequestPayload() {}

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

func (r *SearchPromisesRequest) isRequestPayload() {}

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

func (r *CreatePromiseRequest) isRequestPayload() {}

type CreatePromiseAndTaskRequest struct {
	Promise *CreatePromiseRequest
	Task    *CreateTaskRequest
	Tags    map[string]string `json:"tags,omitempty"`
}

func (r *CreatePromiseAndTaskRequest) String() string {
	return fmt.Sprintf("CreatePromiseAndTask(promise=%v, task=%v, tags=%v)", r.Promise, r.Task, r.Tags)
}

func (r *CreatePromiseAndTaskRequest) Validate() error {
	return nil
}

func (r *CreatePromiseAndTaskRequest) Kind() Kind {
	return CreatePromiseAndTask
}

func (r *CreatePromiseAndTaskRequest) isRequestPayload() {}

type CompletePromiseRequest struct {
	Id             string            `json:"id"`
	IdempotencyKey *idempotency.Key  `json:"idemptencyKey,omitempty"`
	Strict         bool              `json:"strict"`
	State          promise.State     `json:"state"`
	Value          promise.Value     `json:"value,omitempty"`
	Tags           map[string]string `json:"tags,omitempty"`
}

func (r *CompletePromiseRequest) String() string {
	return fmt.Sprintf("CompletePromise(id=%s, idempotencyKey=%v, strict=%v, state=%v, value=%v, tags=%v)", r.Id, r.IdempotencyKey, r.Strict, r.State, r.Value, r.Tags)
}

func (r *CompletePromiseRequest) Validate() error {
	return nil
}

func (r *CompletePromiseRequest) Kind() Kind {
	return CompletePromise
}

func (r *CompletePromiseRequest) isRequestPayload() {}

// Callbacks

type CreateCallbackRequest struct {
	Id        string            `json:"id"`
	PromiseId string            `json:"promiseId"`
	Recv      json.RawMessage   `json:"recv"`
	Mesg      *message.Mesg     `json:"mesg"`
	Timeout   int64             `json:"timeout"`
	Tags      map[string]string `json:"tags,omitempty"`
}

func (r *CreateCallbackRequest) String() string {
	return fmt.Sprintf("CreateCallback(id=%s, promiseId=%s, recv=%s, mesg=%s, timeout=%d, tags=%v)", r.Id, r.PromiseId, r.Recv, r.Mesg, r.Timeout, r.Tags)
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

func (r *CreateCallbackRequest) isRequestPayload() {}

// Schedules

type ReadScheduleRequest struct {
	Id   string            `json:"id"`
	Tags map[string]string `json:"tags,omitempty"`
}

func (r *ReadScheduleRequest) String() string {
	return fmt.Sprintf("ReadSchedule(id=%s, tags=%v)", r.Id, r.Tags)
}

func (r *ReadScheduleRequest) Validate() error {
	return nil
}

func (r *ReadScheduleRequest) Kind() Kind {
	return ReadSchedule
}

func (r *ReadScheduleRequest) isRequestPayload() {}

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

func (r *SearchSchedulesRequest) isRequestPayload() {}

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

func (r *CreateScheduleRequest) isRequestPayload() {}

type DeleteScheduleRequest struct {
	Id   string            `json:"id"`
	Tags map[string]string `json:"tags,omitempty"`
}

func (r *DeleteScheduleRequest) String() string {
	return fmt.Sprintf("DeleteSchedule(id=%s, tags=%v)", r.Id, r.Tags)
}

func (r *DeleteScheduleRequest) Validate() error {
	return nil
}

func (r *DeleteScheduleRequest) Kind() Kind {
	return DeleteSchedule
}

func (r *DeleteScheduleRequest) isRequestPayload() {}

// Locks

type AcquireLockRequest struct {
	ResourceId  string            `json:"resourceId"`
	ExecutionId string            `json:"executionId"`
	ProcessId   string            `json:"processId"`
	Ttl         int64             `json:"ttl" binding:"min=0"`
	Tags        map[string]string `json:"tags,omitempty"`
}

func (r *AcquireLockRequest) String() string {
	return fmt.Sprintf(
		"AcquireLock(resourceId=%s, executionId=%s, processId=%s, ttl=%d, tags=%v)",
		r.ResourceId,
		r.ExecutionId,
		r.ProcessId,
		r.Ttl,
		r.Tags,
	)
}

func (r *AcquireLockRequest) Validate() error {
	return nil
}

func (r *AcquireLockRequest) Kind() Kind {
	return AcquireLock
}

func (r *AcquireLockRequest) isRequestPayload() {}

type ReleaseLockRequest struct {
	ResourceId  string            `json:"resourceId"`
	ExecutionId string            `json:"executionId"`
	Tags        map[string]string `json:"tags,omitempty"`
}

func (r *ReleaseLockRequest) String() string {
	return fmt.Sprintf("ReleaseLock(resourceId=%s, executionId=%s, tags=%v)", r.ResourceId, r.ExecutionId, r.Tags)
}

func (r *ReleaseLockRequest) Validate() error {
	return nil
}

func (r *ReleaseLockRequest) Kind() Kind {
	return ReleaseLock
}

func (r *ReleaseLockRequest) isRequestPayload() {}

type HeartbeatLocksRequest struct {
	ProcessId string            `json:"processId"`
	Tags      map[string]string `json:"tags,omitempty"`
}

func (r *HeartbeatLocksRequest) String() string {
	return fmt.Sprintf("HeartbeatLocks(processId=%s, tags=%v)", r.ProcessId, r.Tags)
}

func (r *HeartbeatLocksRequest) Validate() error {
	return nil
}

func (r *HeartbeatLocksRequest) Kind() Kind {
	return HeartbeatLocks
}

func (r *HeartbeatLocksRequest) isRequestPayload() {}

// Tasks

// CreateTaskRequest is not a Request on its own it needs to be part of a CreatePromiseAndTask Request
// that is why it does not implement the request interface
type CreateTaskRequest struct {
	PromiseId string            `json:"promiseId"`
	ProcessId string            `json:"processId"`
	Ttl       int               `json:"ttl" binding:"min=0"`
	Timeout   int64             `json:"timeout"`
	Tags      map[string]string `json:"tags,omitempty"`
}

func (r *CreateTaskRequest) String() string {
	return fmt.Sprintf(
		"CreateTask(promiseId=%s, processId=%s, ttl=%d, timeout=%d, tags=%v)",
		r.PromiseId,
		r.ProcessId,
		r.Ttl,
		r.Timeout,
		r.Tags,
	)
}

type ClaimTaskRequest struct {
	Id        string            `json:"id"`
	Counter   int               `json:"counter"`
	ProcessId string            `json:"processId"`
	Ttl       int               `json:"ttl" binding:"min=0"`
	Tags      map[string]string `json:"tags,omitempty"`
}

func (r *ClaimTaskRequest) String() string {
	return fmt.Sprintf("ClaimTask(id=%s, counter=%d, processId=%s, ttl=%d, tags=%v)", r.Id, r.Counter, r.ProcessId, r.Ttl, r.Tags)
}

func (r *ClaimTaskRequest) Validate() error {
	return nil
}

func (r *ClaimTaskRequest) Kind() Kind {
	return ClaimTask
}

func (r *ClaimTaskRequest) isRequestPayload() {}

type CompleteTaskRequest struct {
	Id      string            `json:"id"`
	Counter int               `json:"counter"`
	Tags    map[string]string `json:"tags,omitempty"`
}

func (r *CompleteTaskRequest) String() string {
	return fmt.Sprintf("CompleteTask(id=%s, counter=%d, tags=%v)", r.Id, r.Counter, r.Tags)
}

func (r *CompleteTaskRequest) Validate() error {
	return nil
}

func (r *CompleteTaskRequest) Kind() Kind {
	return CompleteTask
}

func (r *CompleteTaskRequest) isRequestPayload() {}

type DropTaskRequest struct {
	Id      string            `json:"id"`
	Counter int               `json:"counter"`
	Tags    map[string]string `json:"tags,omitempty"`
}

func (r *DropTaskRequest) String() string {
	return fmt.Sprintf("DropTask(id=%s, counter=%d, tags=%v)", r.Id, r.Counter, r.Tags)
}

func (r *DropTaskRequest) Validate() error {
	return nil
}

func (r *DropTaskRequest) Kind() Kind {
	return DropTask
}

func (r *DropTaskRequest) isRequestPayload() {}

type HeartbeatTasksRequest struct {
	ProcessId string            `json:"processId"`
	Tags      map[string]string `json:"tags,omitempty"`
}

func (r *HeartbeatTasksRequest) String() string {
	return fmt.Sprintf("HeartbeatTasks(processId=%s, tags=%v)", r.ProcessId, r.Tags)
}

func (r *HeartbeatTasksRequest) Validate() error {
	return nil
}

func (r *HeartbeatTasksRequest) Kind() Kind {
	return HeartbeatTasks
}

func (r *HeartbeatTasksRequest) isRequestPayload() {}

// Echo

type EchoRequest struct {
	Data string            `json:"data"`
	Tags map[string]string `json:"tags,omitempty"`
}

func (r *EchoRequest) String() string {
	return fmt.Sprintf("Echo(data=%s, tags=%v)", r.Data, r.Tags)
}

func (r *EchoRequest) Validate() error {
	return nil
}

func (r *EchoRequest) Kind() Kind {
	return Echo
}

func (r *EchoRequest) isRequestPayload() {}

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
