package resonate

import (
	"bytes"
	"encoding/json"
	"fmt"
)

// Value is the wire format for data crossing the durability boundary.
//
// On the wire, Data is either omitted, an empty JSON string (meaning nil
// payload), or a base64-encoded string of (optionally encrypted) JSON. After
// the codec decodes it, Data holds the decoded JSON value directly.
type Value struct {
	Headers map[string]string `json:"headers,omitempty"`
	Data    json.RawMessage   `json:"data,omitempty"`
}

// NewValue serializes any value into a Value with Data set to its JSON form.
// This is the "raw" wrapping path (no encryption/base64) used when constructing
// internal payloads; the Codec is used for the durability boundary path.
func NewValue(v any) (Value, error) {
	raw, err := json.Marshal(v)
	if err != nil {
		return Value{}, &EncodingError{Msg: err.Error()}
	}
	return Value{Data: raw}, nil
}

// DataOrNull returns the Data field, or a JSON null literal if absent.
func (v Value) DataOrNull() json.RawMessage {
	if len(v.Data) == 0 {
		return json.RawMessage("null")
	}
	return v.Data
}

// Decode unmarshals the Data field into out. If Data is absent, out is left
// untouched and Decode returns nil.
func (v Value) Decode(out any) error {
	if len(v.Data) == 0 {
		return nil
	}
	return json.Unmarshal(v.Data, out)
}

// UnmarshalJSON accepts both the object form { "headers": ..., "data": ... }
// and a bare non-object JSON value (in which case the raw value is treated as
// the Data field).
func (v *Value) UnmarshalJSON(b []byte) error {
	trimmed := bytes.TrimLeft(b, " \t\n\r")
	if len(trimmed) == 0 || string(trimmed) == "null" {
		*v = Value{}
		return nil
	}
	if trimmed[0] == '{' {
		var aux struct {
			Headers map[string]string `json:"headers"`
			Data    json.RawMessage   `json:"data"`
		}
		if err := json.Unmarshal(b, &aux); err != nil {
			return err
		}
		v.Headers = aux.Headers
		v.Data = aux.Data
		return nil
	}
	v.Headers = nil
	v.Data = bytes.Clone(b)
	return nil
}

// PromiseState is the lifecycle state of a durable promise. Values are
// snake_case on the wire.
type PromiseState string

const (
	// PromiseStatePending means the promise has been created but not yet settled.
	PromiseStatePending PromiseState = "pending"
	// PromiseStateResolved means the promise completed successfully with a value.
	PromiseStateResolved PromiseState = "resolved"
	// PromiseStateRejected means the promise was rejected with an error by the workflow.
	PromiseStateRejected PromiseState = "rejected"
	// PromiseStateRejectedCanceled means the promise was explicitly canceled before it resolved.
	PromiseStateRejectedCanceled PromiseState = "rejected_canceled"
	// PromiseStateRejectedTimedout means the promise expired before it was settled.
	PromiseStateRejectedTimedout PromiseState = "rejected_timedout"
)

// TaskState is the lifecycle state of a task. Values are snake_case on the wire.
type TaskState string

const (
	// TaskStatePending means the task exists on the server but no worker has acquired it yet.
	TaskStatePending TaskState = "pending"
	// TaskStateAcquired means a worker holds the lease and is currently executing the task.
	TaskStateAcquired TaskState = "acquired"
	// TaskStateSuspended means the task is waiting on one or more promise callbacks before resuming.
	TaskStateSuspended TaskState = "suspended"
	// TaskStateHalted means the task has been administratively stopped and will not resume.
	TaskStateHalted TaskState = "halted"
	// TaskStateFulfilled means the task completed and its root promise has been settled.
	TaskStateFulfilled TaskState = "fulfilled"
)

// SettleState is the target state for a promise.settle request.
type SettleState string

const (
	// SettleStateResolved requests that the promise be settled as successfully completed.
	SettleStateResolved SettleState = "resolved"
	// SettleStateRejected requests that the promise be settled as failed with an error.
	SettleStateRejected SettleState = "rejected"
	// SettleStateRejectedCanceled requests that the promise be settled as explicitly canceled.
	SettleStateRejectedCanceled SettleState = "rejected_canceled"
)

// PromiseRecord is a durable promise as stored by the server.
type PromiseRecord struct {
	ID        string            `json:"id"`
	State     PromiseState      `json:"state"`
	Param     Value             `json:"param"`
	Value     Value             `json:"value"`
	Tags      map[string]string `json:"tags"`
	TimeoutAt int64             `json:"timeoutAt"`
	CreatedAt int64             `json:"createdAt,omitempty"`
	SettledAt *int64            `json:"settledAt,omitempty"`
}

// TaskRecord is a task as returned by the server.
type TaskRecord struct {
	ID      string    `json:"id"`
	State   TaskState `json:"state"`
	Version int64     `json:"version"`
	// Resumes is intentionally untyped on the wire: it may be an array of
	// strings, a number, or a boolean depending on the server's choice.
	Resumes json.RawMessage `json:"resumes,omitempty"`
	TTL     *int64          `json:"ttl,omitempty"`
	PID     *string         `json:"pid,omitempty"`
}

// ScheduleRecord is a recurring schedule as returned by the server.
type ScheduleRecord struct {
	ID             string            `json:"id"`
	Cron           string            `json:"cron"`
	PromiseID      string            `json:"promiseId"`
	PromiseTimeout int64             `json:"promiseTimeout"`
	PromiseParam   Value             `json:"promiseParam,omitempty"`
	PromiseTags    map[string]string `json:"promiseTags,omitempty"`
	CreatedAt      int64             `json:"createdAt,omitempty"`
	NextRunAt      int64             `json:"nextRunAt,omitempty"`
	LastRunAt      *int64            `json:"lastRunAt,omitempty"`
}

// PromiseCreateReq is the data payload for a promise.create request.
type PromiseCreateReq struct {
	ID        string            `json:"id"`
	TimeoutAt int64             `json:"timeoutAt"`
	Param     Value             `json:"param"`
	Tags      map[string]string `json:"tags"`
}

// PromiseSettleReq is the data payload for a promise.settle request.
type PromiseSettleReq struct {
	ID    string      `json:"id"`
	State SettleState `json:"state"`
	Value Value       `json:"value"`
}

// PromiseRegisterCallbackData is the data payload nested inside task.suspend
// actions.
type PromiseRegisterCallbackData struct {
	Awaited string `json:"awaited"`
	Awaiter string `json:"awaiter"`
}

// TaskData is the SDK-internal payload encoded into a root promise's param
// when dispatching a function or workflow remotely.
type TaskData struct {
	Func string          `json:"func"`
	Args json.RawMessage `json:"args,omitempty"`
}

// TaskDataValue builds a Value wrapping {"func": funcName, "args": args}.
// The result is suitable for use as PromiseCreateReq.Param when remote-dispatching.
func TaskDataValue(funcName string, args any) (Value, error) {
	argsRaw, err := json.Marshal(args)
	if err != nil {
		return Value{}, &EncodingError{Msg: fmt.Sprintf("encode args: %v", err)}
	}
	payload, err := json.Marshal(TaskData{Func: funcName, Args: argsRaw})
	if err != nil {
		return Value{}, &EncodingError{Msg: fmt.Sprintf("encode task data: %v", err)}
	}
	return Value{Data: payload}, nil
}
