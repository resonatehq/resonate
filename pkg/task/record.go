package task

import (
	"encoding/json"

	"github.com/resonatehq/resonate/pkg/message"
)

type TaskRecord struct {
	Id          string
	ProcessId   *string
	State       State
	Message     []byte
	Timeout     int64
	Counter     int
	Attempt     int
	Frequency   int
	Expiration  int64
	CreatedOn   *int64
	CompletedOn *int64
}

func (r *TaskRecord) Task() (*Task, error) {
	message, err := bytesToMessage(r.Message)
	if err != nil {
		return nil, err
	}

	return &Task{
		Id:          r.Id,
		ProcessId:   r.ProcessId,
		State:       r.State,
		Message:     message,
		Timeout:     r.Timeout,
		Counter:     r.Counter,
		Attempt:     r.Attempt,
		Frequency:   r.Frequency,
		Expiration:  r.Expiration,
		CreatedOn:   r.CreatedOn,
		CompletedOn: r.CompletedOn,
	}, nil
}

func bytesToMessage(b []byte) (*message.Message, error) {
	var m *message.Message

	if b != nil {
		if err := json.Unmarshal(b, &m); err != nil {
			return nil, err
		}
	}

	return m, nil
}
