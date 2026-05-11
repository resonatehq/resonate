package network

import (
	"encoding/json"
	"fmt"

	resonate "github.com/resonatehq/resonate-sdk-go"
)

// Message is the sum type of server push-messages.
//
//	ExecuteMessage: { "kind": "execute", "data": { "task": { "id", "version" } } }
//	UnblockMessage: { "kind": "unblock", "data": { "promise": PromiseRecord } }
type Message interface {
	isMessage()
}

// ExecuteMessage tells this worker to run a task. Fields are flattened from
// the nested `data.task.{id,version}` shape for ergonomics.
type ExecuteMessage struct {
	TaskID  string
	Version int64
}

func (ExecuteMessage) isMessage() {}

// UnblockMessage signals that a promise this worker is waiting on has been
// settled. The Promise field is the raw JSON; the codec is responsible for
// decoding it into a PromiseRecord with decoded Param/Value fields.
type UnblockMessage struct {
	Promise json.RawMessage
}

func (UnblockMessage) isMessage() {}

// DecodeMessage parses a push-message frame from the network. Unknown kinds
// return an error rather than a nil Message so callers can log and drop.
func DecodeMessage(raw []byte) (Message, error) {
	var head struct {
		Kind string          `json:"kind"`
		Data json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal(raw, &head); err != nil {
		return nil, &resonate.DecodingError{Msg: fmt.Sprintf("message envelope: %v", err)}
	}
	switch head.Kind {
	case "execute":
		var inner struct {
			Task struct {
				ID      string `json:"id"`
				Version int64  `json:"version"`
			} `json:"task"`
		}
		if err := json.Unmarshal(head.Data, &inner); err != nil {
			return nil, &resonate.DecodingError{Msg: fmt.Sprintf("execute data: %v", err)}
		}
		return ExecuteMessage{TaskID: inner.Task.ID, Version: inner.Task.Version}, nil
	case "unblock":
		var inner struct {
			Promise json.RawMessage `json:"promise"`
		}
		if err := json.Unmarshal(head.Data, &inner); err != nil {
			return nil, &resonate.DecodingError{Msg: fmt.Sprintf("unblock data: %v", err)}
		}
		return UnblockMessage{Promise: inner.Promise}, nil
	default:
		return nil, &resonate.DecodingError{Msg: fmt.Sprintf("unknown message kind %q", head.Kind)}
	}
}
