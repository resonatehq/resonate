package task

import (
	"encoding/json"

	"github.com/resonatehq/resonate/pkg/message"
)

type TaskRecord struct {
	Id          string
	ProcessId   *string
	State       State
	Recv        []byte
	Mesg        []byte
	Timeout     int64
	Counter     int
	Attempt     int
	Frequency   int
	Expiration  int64
	CreatedOn   *int64
	CompletedOn *int64
}

func (r *TaskRecord) Task() (*Task, error) {
	var mesg *message.Mesg
	if err := json.Unmarshal(r.Mesg, &mesg); err != nil {
		return nil, err
	}

	return &Task{
		Id:          r.Id,
		ProcessId:   r.ProcessId,
		State:       r.State,
		Recv:        r.Recv,
		Mesg:        mesg,
		Timeout:     r.Timeout,
		Counter:     r.Counter,
		Attempt:     r.Attempt,
		Frequency:   r.Frequency,
		Expiration:  r.Expiration,
		CreatedOn:   r.CreatedOn,
		CompletedOn: r.CompletedOn,
	}, nil
}
