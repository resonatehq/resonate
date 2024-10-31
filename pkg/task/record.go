package task

import (
	"encoding/json"

	"github.com/resonatehq/resonate/pkg/message"
)

type TaskRecord struct {
	Id            string
	ProcessId     *string
	State         State
	RootPromiseId string
	Recv          []byte
	Mesg          []byte
	Timeout       int64
	Counter       int
	Attempt       int
	Ttl           int
	ExpiresAt     int64
	CreatedOn     *int64
	CompletedOn   *int64
}

func (r *TaskRecord) Task() (*Task, error) {
	var mesg *message.Mesg
	if err := json.Unmarshal(r.Mesg, &mesg); err != nil {
		return nil, err
	}

	return &Task{
		Id:            r.Id,
		ProcessId:     r.ProcessId,
		State:         r.State,
		RootPromiseId: r.RootPromiseId,
		Recv:          r.Recv,
		Mesg:          mesg,
		Timeout:       r.Timeout,
		Counter:       r.Counter,
		Attempt:       r.Attempt,
		Ttl:           r.Ttl,
		ExpiresAt:     r.ExpiresAt,
		CreatedOn:     r.CreatedOn,
		CompletedOn:   r.CompletedOn,
	}, nil
}
